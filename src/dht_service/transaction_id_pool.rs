use std::sync::atomic::{AtomicU32, Ordering};

/// Ensures we never use the same ID for two different requests
#[derive(Debug)]
pub struct TransactionIdPool {
    next_id: AtomicU32,
}

impl TransactionIdPool {
    pub fn new() -> Self {
        TransactionIdPool {
            next_id: AtomicU32::new(0),
        }
    }

    pub fn next(&self) -> u32 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }
}
