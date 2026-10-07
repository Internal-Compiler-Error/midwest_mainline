//! BEP 52's SHA-256 Merkle trees: a file's leaves are the hashes of its 16 KiB blocks, padded
//! with all-zero hashes to a power of two, and every node above is the hash of its two
//! children. A piece's hash is the root of the subtree over its blocks; the file's `pieces
//! root` is the root of the whole tree.

use sha2::{Digest, Sha256};

pub type Hash = [u8; 32];

/// The leaf size: every v2 tree hashes 16 KiB blocks, whatever the piece size.
pub const BLOCK: usize = 16 << 10;

pub fn pair(left: &Hash, right: &Hash) -> Hash {
    let mut hasher = Sha256::new();
    hasher.update(left);
    hasher.update(right);
    hasher.finalize().into()
}

/// The root of a subtree `levels` tall whose leaves are all zero hashes: what stands in for a
/// whole piece past the end of a file in the layers above the piece layer.
pub fn zero_subtree(levels: u32) -> Hash {
    (0..levels).fold([0; 32], |h, _| pair(&h, &h))
}

/// The hashes of `data`'s 16 KiB blocks, the last one possibly short: a tree's leaves.
pub fn leaves(data: &[u8]) -> Vec<Hash> {
    data.chunks(BLOCK).map(|block| Sha256::digest(block).into()).collect()
}

/// The root over `layer`, filled out to `width` (a power of two, at least `layer.len()`) with
/// `pad`, the value of a node of that layer that covers nothing but padding. Works in place.
fn root(mut layer: Vec<Hash>, mut width: usize, mut pad: Hash) -> Hash {
    debug_assert!(width.is_power_of_two() && width >= layer.len());
    while width > 1 {
        if layer.len() % 2 == 1 {
            layer.push(pad);
        }
        let half = layer.len() / 2;
        for i in 0..half {
            layer[i] = pair(&layer[2 * i], &layer[2 * i + 1]);
        }
        layer.truncate(half);
        pad = pair(&pad, &pad);
        width /= 2;
    }
    layer.first().copied().unwrap_or(pad)
}

/// The root of the tree over `data`'s 16 KiB blocks, `leaves` wide (a power of two covering
/// them all); leaves past the data are zero hashes, not hashes of zeros.
pub fn data_root(data: &[u8], leaves: usize) -> Hash {
    root(self::leaves(data), leaves, [0; 32])
}

/// The leaves a file of `len` bytes needs under its root: a power of two, at least one.
pub fn file_leaves(len: u64) -> usize {
    (len.div_ceil(BLOCK as u64) as usize).max(1).next_power_of_two()
}

/// A file's `pieces root` from its piece layer (one hash per piece, `piece_size` bytes each).
pub fn root_from_layer(layer: &[Hash], piece_size: u32) -> Hash {
    let levels = (piece_size as usize / BLOCK).trailing_zeros();
    root(layer.to_vec(), layer.len().next_power_of_two(), zero_subtree(levels))
}

/// Every layer of the tree over `layer`, bottom up and padded out to a power of two: what
/// answering a BEP 52 hash request for it takes.
pub fn layers_above(layer: &[Hash], pad: Hash) -> Vec<Vec<Hash>> {
    let mut out = vec![];
    let mut nodes = layer.to_vec();
    nodes.resize(layer.len().next_power_of_two().max(1), pad);
    while nodes.len() > 1 {
        let next = nodes.as_chunks::<2>().0.iter().map(|[l, r]| pair(l, r)).collect();
        out.push(std::mem::replace(&mut nodes, next));
    }
    out.push(nodes);
    out
}

#[cfg(test)]
mod test {
    use super::*;

    fn hex(h: &Hash) -> String {
        h.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Reference values from Python's hashlib, built straight from BEP 52's definition.
    #[test]
    fn roots_match_a_reference() {
        // one short block: the root is its hash
        assert_eq!(
            hex(&data_root(b"abc", 1)),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        // 40000 bytes of 0x61: three blocks, the last 7232 bytes, a fourth zero leaf
        let data = vec![b'a'; 40000];
        assert_eq!(
            hex(&data_root(&data, file_leaves(40000))),
            "225106564456ed33b02cc22e9d6f5014fd9f4c5383bee6605e07664a44d260ea"
        );
    }

    #[test]
    fn a_layer_rolls_up_to_the_same_root_as_the_blocks() {
        // 5 pieces of 32 KiB (2 blocks each), the last one short
        let data: Vec<u8> = (0..(4 * 32768 + 20000)).map(|i| (i * 7 % 251) as u8).collect();
        let layer: Vec<Hash> = data.chunks(32768).map(|piece| data_root(piece, 2)).collect();
        assert_eq!(
            root_from_layer(&layer, 32768),
            data_root(&data, file_leaves(data.len() as u64))
        );
        let tree = layers_above(&layer, zero_subtree(1));
        assert_eq!(tree.len(), 4, "8 → 4 → 2 → 1");
        assert_eq!(tree[3][0], root_from_layer(&layer, 32768));
    }
}
