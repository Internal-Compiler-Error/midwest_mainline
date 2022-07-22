use std::sync::atomic::{AtomicU16, Ordering};

/// Ensures we never use the same ID for two different requests
#[derive(Debug)]
pub struct TransactionIdPool {
    next_id: AtomicU16,
}

impl TransactionIdPool {
    pub fn new() -> Self {
        TransactionIdPool {
            next_id: AtomicU16::new(0),
        }
    }

    pub fn next(&self) -> u16 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }
}
