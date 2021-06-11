use crate::chain_api::{ChainApi, Response, RewardsSlashesPage, TransfersPage};
use crate::database::Database;
use crate::{Context, Result};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};

const ROW_AMOUNT: usize = 10;
const FAILED_TASK_SLEEP: u64 = 30;
const LOOP_INTERVAL: u64 = 300;

pub struct TransferFetcher {
    db: Database,
    api: Arc<ChainApi>,
}

#[async_trait]
impl FetchChainData for TransferFetcher {
    type Data = Response<TransfersPage>;

    fn new(db: Database, api: Arc<ChainApi>) -> Self {
        TransferFetcher { db: db, api: api }
    }
    async fn fetch_data(&self, context: &Context, row: usize, page: usize) -> Result<Self::Data> {
        self.api.request_transfer(context, row, page).await
    }
    async fn store_data(&self, context: &Context, data: &Self::Data) -> Result<usize> {
        self.db.store_transfer_event(context, data).await
    }
}

pub struct RewardsSlashesFetcher {
    db: Database,
    api: Arc<ChainApi>,
}

#[async_trait]
impl FetchChainData for RewardsSlashesFetcher {
    type Data = Response<RewardsSlashesPage>;

    fn new(db: Database, api: Arc<ChainApi>) -> Self {
        RewardsSlashesFetcher { db: db, api: api }
    }
    async fn fetch_data(&self, context: &Context, row: usize, page: usize) -> Result<Self::Data> {
        self.api.request_reward_slash(context, row, page).await
    }
    async fn store_data(&self, context: &Context, data: &Self::Data) -> Result<usize> {
        self.db.store_reward_slash_event(context, data).await
    }
}

#[async_trait]
pub trait FetchChainData {
    type Data: Send + Sync + std::fmt::Debug + DataInfo;

    fn new(db: Database, api: Arc<ChainApi>) -> Self;
    async fn fetch_data(&self, _: &Context, row: usize, page: usize) -> Result<Self::Data>;
    async fn store_data(&self, _: &Context, data: &Self::Data) -> Result<usize>;
}

pub trait DataInfo {
    fn is_empty(&self) -> bool;
    fn new_count(&self) -> usize;
}

#[async_trait]
impl DataInfo for Response<TransfersPage> {
    fn is_empty(&self) -> bool {
        <Self as DataInfo>::new_count(self) == 0
    }
    fn new_count(&self) -> usize {
        if let Some(transfers) = &self.data.transfers {
            transfers.len()
        } else {
            0
        }
    }
}

#[async_trait]
impl DataInfo for Response<RewardsSlashesPage> {
    fn is_empty(&self) -> bool {
        <Self as DataInfo>::new_count(self) == 0
    }
    fn new_count(&self) -> usize {
        if let Some(rewards_slashes) = &self.data.list {
            rewards_slashes.len()
        } else {
            0
        }
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum Module {
    Transfer,
    RewardsSlashes,
}

pub struct ScrapingService<'a> {
    db: Database,
    api: Arc<ChainApi>,
    contexts: Arc<RwLock<Vec<Context>>>,
    running: HashSet<&'a Module>,
}

impl<'a> ScrapingService<'a> {
    pub fn new(db: Database) -> Self {
        ScrapingService {
            db: db,
            api: Arc::new(ChainApi::new()),
            contexts: Arc::new(RwLock::new(vec![])),
            running: HashSet::new(),
        }
    }
    pub async fn add_contexts(&mut self, mut contexts: Vec<Context>) {
        self.contexts.write().await.append(&mut contexts);
    }
    pub async fn run(&mut self, module: &'a Module) -> Result<()> {
        if self.running.contains(module) {
            return Err(anyhow!(
                "configuration contains the same module multiple times"
            ));
        }

        self.running.insert(module);

        match module {
            Module::Transfer => self.run_fetcher::<TransferFetcher>().await,
            Module::RewardsSlashes => self.run_fetcher::<RewardsSlashesFetcher>().await,
        }

        Ok(())
    }
    async fn run_fetcher<T>(&self)
    where
        T: 'static + Send + Sync + FetchChainData,
    {
        async fn local<T>(fetcher: &T, contexts: &Arc<RwLock<Vec<Context>>>) -> Result<()>
        where
            T: 'static + Send + Sync + FetchChainData,
        {
            let mut page: usize = 1;

            loop {
                // This `read()` can result in a quite long-running lock.
                // However, it is not expected that `Self::add_contexts` will be
                // called after a fetcher is running, since those are loaded on
                // application startup.
                for context in contexts.read().await.iter() {
                    let resp = fetcher.fetch_data(context, ROW_AMOUNT, page).await?;

                    loop {
                        // No new extrinsics were found, continue with next account.
                        if resp.is_empty() {
                            debug!(
                                "No new transactions were found for {:?}, moving on...",
                                context
                            );
                            break;
                        }

                        // The cache tries to filter all unprocessed extrinsics,
                        // but the cache is not persisted and is wiped on
                        // application shutdown. The database method will return
                        // how many extrinsics have been *newly* inserted into
                        // the database. If it's 0, then no new extrinsics were
                        // detected. Continue with the next account.
                        if fetcher.store_data(context, &resp).await? == 0 {
                            break;
                        }

                        info!("New transactions found for {:?}: {:?}", context, resp);

                        // If new extrinsics were all on one page, continue with
                        // the next account. Otherwise, fetch the next page.
                        if resp.new_count() < ROW_AMOUNT {
                            break;
                        }

                        page += 1;
                    }

                    // Reset to page 1.
                    page = 1;
                }

                // Once all accounts have been processed, pause so other active
                // fetchers are not blocked (by the time guard) from executing
                // requests.
                sleep(Duration::from_secs(LOOP_INTERVAL)).await;
            }
        }

        let fetcher = T::new(self.db.clone(), Arc::clone(&self.api));
        let contexts = Arc::clone(&self.contexts);
        tokio::spawn(async move {
            loop {
                if let Err(err) = local(&fetcher, &contexts).await {
                    error!("Failed task while running fetcher: {:?}", err);
                }

                sleep(Duration::from_secs(FAILED_TASK_SLEEP)).await;
            }
        });
    }
    pub async fn wait_blocking(&self) {
        loop {
            sleep(Duration::from_secs(u64::MAX)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{db, init};
    use std::vec;

    #[tokio::test]
    #[ignore]
    async fn live_run_transfer_fetcher() {
        init();

        info!("Running live test for transfer fetcher");

        let db = db().await;

        let contexts = vec![Context::from(
            "11uMPbeaEDJhUxzU4ZfWW9VQEsryP9XqFcNRfPdYda6aFWJ",
        )];

        let mut service = ScrapingService::new(db);
        service.add_contexts(contexts).await;
        service.run_fetcher::<TransferFetcher>().await;
        service.wait_blocking().await;
    }

    #[tokio::test]
    #[ignore]
    async fn live_run_reward_slash_fetcher() {
        init();

        info!("Running live test for transfer fetcher");

        let db = db().await;

        let contexts = vec![Context::from(
            "11uMPbeaEDJhUxzU4ZfWW9VQEsryP9XqFcNRfPdYda6aFWJ",
        )];

        let mut service = ScrapingService::new(db);
        service.add_contexts(contexts).await;
        service.run_fetcher::<RewardsSlashesFetcher>().await;
        service.wait_blocking().await;
    }
}
