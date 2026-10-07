//! Test-only helpers.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use diesel::SqliteConnection;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel_migrations::MigrationHarness;
use tokio::net::UdpSocket;

use crate::dht::{DhtSession, MIGRATIONS, SensibleOptions};

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

/// A running node on loopback, with its own database in `dir`; stops when dropped
pub(crate) struct Node {
    pub session: Arc<DhtSession>,
    _run: tokio::task::JoinHandle<()>,
}

impl Drop for Node {
    fn drop(&mut self) {
        self._run.abort();
    }
}

pub(crate) async fn node(dir: &Path, name: &str, bind: SocketAddr) -> Node {
    let socket = UdpSocket::bind(bind).await.unwrap();
    let db = dir.join(format!("{name}.db"));
    let session = Arc::new(DhtSession::with_stable_id(socket, None, db.to_str().unwrap()).unwrap());
    node_of(session)
}

/// Runs `session`
pub(crate) fn node_of(session: Arc<DhtSession>) -> Node {
    let run = tokio::spawn({
        let session = session.clone();
        async move { session.run().await }
    });
    Node { session, _run: run }
}

/// Whether `holds` comes true within two seconds, for what other nodes do in their own time
pub(crate) async fn eventually(holds: impl Fn() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    while !holds() {
        if tokio::time::Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    true
}

/// An empty directory of its own for a test
pub(crate) fn scratch_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("midwest-mainline-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
