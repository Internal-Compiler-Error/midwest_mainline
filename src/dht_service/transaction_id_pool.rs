use std::collections::HashSet;
use std::ops::Deref;
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use tokio::runtime::Handle;
use tokio::sync::Mutex;

/// Ensures we never use the same ID for two different requests
#[derive(Debug)]
pub struct TransactionIdPool {
    used_pool: Mutex<HashSet<u16>>,
}

pub struct Id<'a> {
    pool: &'a TransactionIdPool,
    id: u16,
}

impl<'a> Id<'a> {
    fn new(pool: &'a TransactionIdPool, id: u16) -> Self {
        Id { pool, id }
    }
}

impl Deref for Id<'_> {
    type Target = u16;

    fn deref(&self) -> &Self::Target {
        &self.id
    }
}

impl Drop for Id<'_> {
    fn drop(&mut self) {
        let remove_id = async {
            self
                .pool
                .used_pool
                .lock()
                .await
                .remove(&self.id);
        };
        let rt_handle = Handle::current();
        rt_handle.block_on(remove_id);
    }
}

impl TransactionIdPool {
    pub fn new() -> Self {
        TransactionIdPool {
            used_pool:
            Mutex::new(HashSet::new())
        }
    }

    pub async fn get_id(&self) -> Id<'_> {
        let mut rand = SmallRng::from_entropy();
        loop {
            let id = rand.gen::<u16>();
            if !self.used_pool.lock().await.contains(&id) {
                self.used_pool.lock().await.insert(id);
                return Id::new(self, id);
            }
        }
    }
}
