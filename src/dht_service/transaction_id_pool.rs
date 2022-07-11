use std::collections::HashSet;
use std::ops::Deref;
use std::sync::atomic::{AtomicU16, Ordering};
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use tokio::runtime::Handle;
use tokio::sync::Mutex;

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
        self
            .next_id
            .fetch_add(1, Ordering::SeqCst)
    }
}
