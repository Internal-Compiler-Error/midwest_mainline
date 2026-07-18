use std::sync::atomic::{AtomicU32, Ordering};

/// Ensures we never use the same ID for two different requests
// TODO: while tempting, maybe do the type masturbation of generic over all atomic integers part
// some other day
#[derive(Debug)]
pub struct TxnIdGenerator {
    next_id: AtomicU32,
}

impl TxnIdGenerator {
    pub fn new() -> Self {
        TxnIdGenerator {
            // random start so in-flight transaction ids aren't trivially predictable
            // from outside; uniqueness within the process comes from the increment
            next_id: AtomicU32::new(rand::random()),
        }
    }

    pub fn next(&self) -> u32 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generators_do_not_share_a_predictable_sequence() {
        let a = TxnIdGenerator::new();
        let b = TxnIdGenerator::new();
        assert_ne!(a.next(), b.next(), "same-seed generators would make ids predictable");
    }
}
