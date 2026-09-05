//! Test-only helpers. The DDL below mirrors `migrations/` — when a migration changes the
//! schema, change it here once instead of in every test module.

use diesel::connection::SimpleConnection;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::SqliteConnection;

use crate::dht::SensibleOptions;

const SCHEMA_DDL: &str = "
    CREATE TABLE misc (key TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE node (
        id BLOB NOT NULL PRIMARY KEY,
        bucket INT NOT NULL,
        last_contacted BIGINT NOT NULL,
        ip_addr TEXT NOT NULL,
        port INT NOT NULL,
        failed_requests INT NOT NULL,
        removed BOOLEAN NOT NULL DEFAULT FALSE,
        last_sent BIGINT,
        added BIGINT NOT NULL DEFAULT (unixepoch('subsec') * 1000)
    );
    CREATE TABLE swarm (info_hash BLOB PRIMARY KEY);
    CREATE TABLE peer (
        ip_addr TEXT NOT NULL,
        port INTEGER NOT NULL,
        last_announced BIGINT NOT NULL,
        swarm BLOB NOT NULL REFERENCES swarm(info_hash) ON DELETE CASCADE ON UPDATE CASCADE,
        PRIMARY KEY (ip_addr, port, swarm)
    );
";

/// A single-connection in-memory pool (max_size 1 so every checkout shares the same
/// in-memory database), using the production connection customizer — the custom `xor()`
/// SQL function lives there.
pub(crate) fn memory_pool() -> Pool<ConnectionManager<SqliteConnection>> {
    let manager = ConnectionManager::<SqliteConnection>::new(":memory:");
    let pool = Pool::builder()
        .max_size(1)
        .connection_customizer(Box::new(SensibleOptions))
        .build(manager)
        .unwrap();
    pool.get().unwrap().batch_execute(SCHEMA_DDL).unwrap();
    pool
}
