use rand::{prelude::SmallRng, Rng, SeedableRng};
use std::{
    collections::HashSet,
    ops::Deref,
    sync::atomic::{AtomicU16, Ordering},
};
use tokio::{runtime::Handle, sync::Mutex};

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
