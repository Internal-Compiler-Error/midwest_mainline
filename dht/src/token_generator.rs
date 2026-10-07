//! BEP 5's write tokens: what a get_peers (or BEP 44 get) answer hands the querier, to be shown
//! with its announce_peer (or put). A token is a keyed hash of the querier's IP and the current
//! epoch, so nothing is kept per querier; it stays good for the epoch it was issued in and the
//! next, 5 to 10 minutes, as BEP 5 suggests.

use std::net::IpAddr;
use std::time::{Duration, Instant};

use sha3::{Digest, Sha3_256};

use crate::types::Token;

const EPOCH: Duration = Duration::from_secs(5 * 60);
/// Opaque to the querier, so as short as is still unguessable; it rides in every answer
const TOKEN_LEN: usize = 8;

#[derive(Debug, Clone)]
pub(crate) struct TokenGenerator {
    secret: [u8; 32],
    started: Instant,
}

impl TokenGenerator {
    pub(crate) fn new(secret: [u8; 32]) -> Self {
        Self {
            secret,
            started: Instant::now(),
        }
    }

    fn epoch(&self) -> u64 {
        (self.started.elapsed().as_secs() / EPOCH.as_secs()) + 1
    }

    /// The token of `ip` in `epoch`; tokens bind to the querier's IP (BEP 5), so a host may
    /// only announce with a token that was issued to its own address
    fn token_in(&self, epoch: u64, ip: &IpAddr) -> Token {
        let mut hasher = Sha3_256::new();
        hasher.update(self.secret);
        hasher.update(epoch.to_be_bytes());
        match ip {
            IpAddr::V4(ip) => hasher.update(ip.octets()),
            IpAddr::V6(ip) => hasher.update(ip.octets()),
        }
        Token::from_bytes(&hasher.finalize()[..TOKEN_LEN])
    }

    /// The token to hand `ip` now
    pub(crate) fn token_for_ip(&self, ip: &IpAddr) -> Token {
        self.token_in(self.epoch(), ip)
    }

    /// Whether `token` was handed to `ip` in this epoch or the one before
    pub(crate) fn is_valid_token(&self, ip: &IpAddr, token: &Token) -> bool {
        self.is_valid_in(self.epoch(), ip, token)
    }

    fn is_valid_in(&self, epoch: u64, ip: &IpAddr, token: &Token) -> bool {
        *token == self.token_in(epoch, ip) || *token == self.token_in(epoch - 1, ip)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_bind_to_the_requesters_ip() {
        let tokens = TokenGenerator::new([42; 32]);
        let a: IpAddr = "1.2.3.4".parse().unwrap();
        let b: IpAddr = "5.6.7.8".parse().unwrap();

        let token = tokens.token_for_ip(&a);
        assert_eq!(token.as_bytes().len(), TOKEN_LEN);
        assert!(tokens.is_valid_token(&a, &token));
        assert!(
            !tokens.is_valid_token(&b, &token),
            "a token must not validate from another IP"
        );
        assert!(!TokenGenerator::new([43; 32]).is_valid_token(&a, &token));
    }

    #[test]
    fn a_token_is_good_for_its_epoch_and_the_next_whether_or_not_others_were_issued() {
        let tokens = TokenGenerator::new([42; 32]);
        let a: IpAddr = "1.2.3.4".parse().unwrap();
        let issued = tokens.token_in(7, &a);
        assert!(tokens.is_valid_in(7, &a, &issued));
        assert!(tokens.is_valid_in(8, &a, &issued));
        assert!(!tokens.is_valid_in(9, &a, &issued));
        assert!(!tokens.is_valid_in(6, &a, &issued));
    }
}
