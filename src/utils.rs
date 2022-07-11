use std::future::Future;
use async_trait::async_trait;
use tokio::task::JoinError;

#[async_trait]
pub trait ParSpawnAndAwait {
    type Awaited;

    async fn par_spawn_and_await(self) -> Result<Self::Awaited, JoinError>;
}

#[async_trait]
impl<F, R> ParSpawnAndAwait for Vec<F>
    where R: Send + 'static,
          F: Future<Output=R> + Send + 'static,
{
    type Awaited = Vec<R>;

    async fn par_spawn_and_await(self) -> Result<Self::Awaited, JoinError> {
        let handles: Vec<_> = self
            .into_iter()
            .map(|f| tokio::spawn(f))
            .collect();

        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await?);
        }

        Ok(results)
    }
}
