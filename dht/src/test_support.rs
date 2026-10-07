//! Test-only helpers.

use diesel::SqliteConnection;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel_migrations::MigrationHarness;

use crate::dht::{MIGRATIONS, SensibleOptions};

/// A single-connection in-memory pool (max_size 1 so every checkout shares the same
/// in-memory database) with the real migrations applied, using the production connection
/// customizer — the custom `xor()` SQL function lives there.
pub(crate) fn memory_pool() -> Pool<ConnectionManager<SqliteConnection>> {
    let manager = ConnectionManager::<SqliteConnection>::new(":memory:");
    let pool = Pool::builder()
        .max_size(1)
        .connection_customizer(Box::new(SensibleOptions))
        .build(manager)
        .unwrap();
    pool.get().unwrap().run_pending_migrations(MIGRATIONS).unwrap();
    pool
}
