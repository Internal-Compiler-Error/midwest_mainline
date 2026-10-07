//! BEP 44: arbitrary data in the DHT. An immutable item is stored at the SHA-1 of its
//! (bencoded) value; a mutable one at the SHA-1 of an ed25519 public key and an optional salt,
//! signed by that key over a sequence number that only goes up. Items are stored like peers
//! are, at the nodes closest to the target, and live two hours unless put again.

use std::time::Duration;

use bendy::encoding::Encoder;
use diesel::prelude::*;
pub use ed25519_dalek::SigningKey;
use ed25519_dalek::{Signature, Signer, Verifier, VerifyingKey};
use sha1::{Digest, Sha1};

use crate::dht::state::SharedState;
use crate::message::error::KrpcError;
use crate::message::find_node_get_peers_response::{Item, ItemSignature};
use crate::message::item_queries::{PutQuery, Signed};
use crate::message::parse_value;
use crate::schema::item;
use crate::types::NodeId;
use crate::utils::unix_timestmap_ms;
use tracing::warn;

/// The longest bencoded value BEP 44 stores
pub const MAX_VALUE: usize = 1000;
/// The longest salt
pub const MAX_SALT: usize = 64;
/// BEP 44 lets an item go after two hours without a put; one an hour keeps it alive
pub const ITEM_LIFETIME: Duration = Duration::from_secs(2 * 60 * 60);

/// Where an immutable item with this bencoded `value` is stored
pub fn immutable_target(value: &[u8]) -> NodeId {
    NodeId(Sha1::digest(value).into())
}

/// Where a mutable item of `key` and `salt` is stored
pub fn mutable_target(key: &[u8; 32], salt: &[u8]) -> NodeId {
    let mut hash = Sha1::new();
    hash.update(key);
    hash.update(salt);
    NodeId(hash.finalize().into())
}

/// What a mutable item's signature covers
fn signed_bytes(salt: &[u8], seq: i64, value: &[u8]) -> Vec<u8> {
    let mut buf = vec![];
    if !salt.is_empty() {
        buf.extend_from_slice(format!("4:salt{}:", salt.len()).as_bytes());
        buf.extend_from_slice(salt);
    }
    buf.extend_from_slice(format!("3:seqi{seq}e1:v").as_bytes());
    buf.extend_from_slice(value);
    buf
}

/// The signature for putting `value` under `key`'s `salt` at `seq`
pub fn sign(key: &SigningKey, salt: &[u8], seq: i64, value: &[u8]) -> [u8; 64] {
    key.sign(&signed_bytes(salt, seq, value)).to_bytes()
}

fn verify(key: &[u8; 32], salt: &[u8], seq: i64, value: &[u8], sig: &[u8; 64]) -> bool {
    let Ok(key) = VerifyingKey::from_bytes(key) else {
        return false;
    };
    key.verify(&signed_bytes(salt, seq, value), &Signature::from_bytes(sig))
        .is_ok()
}

/// Whether `value` is one value in canonical bencode, the only kind targets and signatures can
/// be agreed on
pub fn is_canonical_bencode(value: &[u8]) -> bool {
    let Some(parsed) = parse_value(value) else {
        return false;
    };
    let mut enc = Encoder::new();
    enc.emit(&parsed).is_ok() && enc.get_output().is_ok_and(|again| again == value)
}

/// A mutable item as it was found
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutableItem {
    pub key: [u8; 32],
    pub salt: Vec<u8>,
    pub seq: i64,
    /// bencoded
    pub value: Vec<u8>,
    pub sig: [u8; 64],
}

impl MutableItem {
    /// `item`, answered for `key` and `salt`, if it is one of theirs and the signature holds
    pub(crate) fn verified(item: &Item, key: &[u8; 32], salt: &[u8]) -> Option<MutableItem> {
        let signature = item.signature.as_ref()?;
        (signature.key == *key && verify(key, salt, signature.seq, &item.value, &signature.sig)).then(|| MutableItem {
            key: *key,
            salt: salt.to_vec(),
            seq: signature.seq,
            value: item.value.clone(),
            sig: signature.sig,
        })
    }
}

type ItemRow = (Vec<u8>, Option<Vec<u8>>, Option<i64>, Option<Vec<u8>>, i64);

impl SharedState {
    /// The item stored at `target` and put within ITEM_LIFETIME, unless it's mutable and no
    /// newer than `newer_than`
    pub(crate) fn stored_item(&self, target: &NodeId, newer_than: Option<i64>) -> Option<Item> {
        let mut conn = self.conn.get().expect("failed to get one connection from pool");
        let cutoff = unix_timestmap_ms() - ITEM_LIFETIME.as_millis() as i64;
        let (value, key, seq, sig, _): ItemRow = item::table
            .filter(item::target.eq(target.as_bytes()))
            .filter(item::last_put.ge(cutoff))
            .select((item::value, item::key, item::seq, item::sig, item::last_put))
            .first(&mut conn)
            .ok()?;
        let signature = match (key, seq, sig) {
            (Some(key), Some(seq), Some(sig)) => Some(ItemSignature {
                key: key.try_into().ok()?,
                seq,
                sig: sig.try_into().ok()?,
            }),
            _ => None,
        };
        if let (Some(signature), Some(newer_than)) = (&signature, newer_than)
            && signature.seq <= newer_than
        {
            return None;
        }
        Some(Item { value, signature })
    }

    /// Stores what `put` brings, if BEP 44 lets it in; the error is the KRPC error to answer with
    pub(crate) fn store_item(&self, put: &PutQuery) -> Result<(), KrpcError> {
        let value = put.value();
        if value.len() > MAX_VALUE {
            return Err(KrpcError::new(205, "Message (v field) too big".to_string()));
        }
        if !is_canonical_bencode(value) {
            return Err(KrpcError::new_protocol());
        }
        let mut conn = self.conn.get().map_err(|_| KrpcError::new_server())?;
        let now = unix_timestmap_ms();
        let Some(Signed { key, salt, seq, sig }) = put.signed() else {
            diesel::insert_into(item::table)
                .values((
                    item::target.eq(immutable_target(value).as_bytes()),
                    item::value.eq(value),
                    item::last_put.eq(now),
                ))
                .on_conflict(item::target)
                .do_update()
                .set(item::last_put.eq(now))
                .execute(&mut conn)
                .inspect_err(|e| warn!("couldn't store a BEP 44 item: {e}"))?;
            return Ok(());
        };
        if salt.len() > MAX_SALT {
            return Err(KrpcError::new(207, "salt (salt field) too big".to_string()));
        }
        if !verify(key, salt, *seq, value, sig) {
            return Err(KrpcError::new(206, "invalid signature".to_string()));
        }
        let target = mutable_target(key, salt);
        // immediate: a deferred one that reads first fails at once, busy timeout or not, when
        // another write gets in before its own
        conn.immediate_transaction(|conn| {
            let stored: Option<Option<i64>> = item::table
                .filter(item::target.eq(target.as_bytes()))
                .select(item::seq)
                .first(conn)
                .optional()
                .inspect_err(|e| warn!("couldn't store a BEP 44 item: {e}"))?;
            if let Some(cas) = put.cas()
                && stored.flatten() != Some(cas)
            {
                return Err(KrpcError::new(
                    301,
                    "the CAS hash mismatched, re-read value and try again".to_string(),
                ));
            }
            if let Some(Some(stored)) = stored
                && *seq < stored
            {
                return Err(KrpcError::new(302, "sequence number less than current".to_string()));
            }
            let row = (
                item::target.eq(target.as_bytes()),
                item::value.eq(value),
                item::key.eq(key.as_slice()),
                item::salt.eq(salt),
                item::seq.eq(seq),
                item::sig.eq(sig.as_slice()),
                item::last_put.eq(now),
            );
            diesel::replace_into(item::table)
                .values(row)
                .execute(conn)
                .inspect_err(|e| warn!("couldn't store a BEP 44 item: {e}"))?;
            Ok(())
        })
    }

    /// Deletes items not put again within ITEM_LIFETIME
    pub(crate) fn expire_items(&self) -> Result<usize, diesel::result::Error> {
        let mut conn = self.conn.get().expect("failed to get one connection from pool");
        let cutoff = unix_timestmap_ms() - ITEM_LIFETIME.as_millis() as i64;
        diesel::delete(item::table.filter(item::last_put.lt(cutoff))).execute(&mut conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes<const N: usize>(hex: &str) -> [u8; N] {
        hex::decode(hex).unwrap().try_into().unwrap()
    }

    const KEY: &str = "77ff84905a91936367c01360803104f92432fcd904a43511876df5cdf3e7e548";

    #[test]
    fn bep_44s_test_vectors() {
        let value = b"12:Hello World!";
        assert_eq!(signed_bytes(b"", 1, value), b"3:seqi1e1:v12:Hello World!".to_vec());
        assert_eq!(
            signed_bytes(b"foobar", 1, value),
            b"4:salt6:foobar3:seqi1e1:v12:Hello World!".to_vec()
        );

        let key = bytes::<32>(KEY);
        let sig = bytes::<64>(
            "305ac8aeb6c9c151fa120f120ea2cfb923564e11552d06a5d856091e5e853cff1260d3f39e4999684aa92eb73ffd136e6f4f3ecbfda0ce53a1608ecd7ae21f01",
        );
        assert!(verify(&key, b"", 1, value, &sig));
        assert!(!verify(&key, b"", 2, value, &sig), "the signature covers seq");
        assert_eq!(
            mutable_target(&key, b"").0,
            bytes::<20>("4a533d47ec9c7d95b1ad75f576cffc641853b750")
        );

        let salted = bytes::<64>(
            "6834284b6b24c3204eb2fea824d82f88883a3d95e8b4a21b8c0ded553d17d17ddf9a8a7104b1258f30bed3787e6cb896fca78c58f8e03b5f18f14951a87d9a08",
        );
        assert!(verify(&key, b"foobar", 1, value, &salted));
        assert_eq!(
            mutable_target(&key, b"foobar").0,
            bytes::<20>("411eba73b6f087ca51a3795d9c8c938d365e32c1")
        );

        assert_eq!(
            immutable_target(value).0,
            bytes::<20>("e5f96f6f38320f0f33959cb4d3d656452117aadb")
        );
    }

    #[test]
    fn what_we_sign_verifies() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let sig = sign(&key, b"salt", 5, b"i42e");
        assert!(verify(&key.verifying_key().to_bytes(), b"salt", 5, b"i42e", &sig));
        assert!(!verify(&key.verifying_key().to_bytes(), b"salt", 5, b"i43e", &sig));
    }

    #[test]
    fn only_one_canonical_value_is_bencode_enough() {
        assert!(is_canonical_bencode(b"12:Hello World!"));
        assert!(is_canonical_bencode(b"d1:ai1e1:bli2eee"));
        assert!(!is_canonical_bencode(b"d1:bi1e1:ai2ee"), "keys out of order");
        assert!(!is_canonical_bencode(b"i1ei2e"), "two values");
        assert!(!is_canonical_bencode(b"i01e"));
        assert!(!is_canonical_bencode(b""));
    }
}
