use async_trait::async_trait;
use std::future::Future;
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
