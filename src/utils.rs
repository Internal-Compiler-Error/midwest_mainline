use async_trait::async_trait;
use std::{future::Future, time::SystemTime};
use tokio::task::{JoinError, JoinSet};

#[async_trait]
pub trait ParSpawnAndAwait {
    type Awaited;

    async fn par_spawn_and_await(self) -> Result<Self::Awaited, JoinError>;
}

#[async_trait]
impl<F, R> ParSpawnAndAwait for Vec<F>
where
    R: Send + 'static,
    F: Future<Output = R> + Send + 'static,
{
    type Awaited = Vec<R>;

    async fn par_spawn_and_await(self) -> Result<Self::Awaited, JoinError> {
        let mut set = JoinSet::new();
        for task in self {
            set.spawn(task);
        }
        Ok(set.join_all().await)
    }
}

pub fn unix_timestmap_ms() -> i64 {
    let timestamp_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("Time was before unix epoch, we don't deal with such exotic cases")
        .as_millis();
    // see models.rs for why it's a stupid i64
    i64::try_from(timestamp_ms).expect("Timestmap couldn't fit into a i64, we don't support such exotic cases")
}
