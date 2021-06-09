use crate::chain_api::{ChainApi, Response, TransfersPage};
use crate::database::Database;
use crate::{Context, Result};
use std::sync::Arc;
use tokio::time::{interval, Duration};

const ROW_AMOUNT: usize = 25;
const INTERVAL_SECS: u64 = 5;

pub struct TransferFetcher {
    api: ChainApi,
    db: Database,
}

#[async_trait]
impl FetchChainData for TransferFetcher {
    type Data = Response<TransfersPage>;

    async fn fetch_data(&self, context: &Context, row: usize, page: usize) -> Result<Self::Data> {
        self.api.request_extrinsics(context, row, page).await
    }
    async fn store_data(&self, context: &Context, data: &Self::Data) -> Result<usize> {
        self.db.store_extrinsic_event(context, data).await
    }
}

#[async_trait]
pub trait FetchChainData {
    type Data: DataInfo;

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
        self.data.transfers.len()
    }
}

pub struct ScrapingService {
    //db: Database,
//api: Arc<>
}

impl ScrapingService {
    pub fn new() -> Self {
        ScrapingService {}
    }
}

pub async fn run_transfer_listener<T>(contexts: Vec<Context>, mut fetcher: T) -> Result<()>
where
    T: FetchChainData,
{
    let mut page = 1;
    let mut interval = interval(Duration::from_secs(INTERVAL_SECS));

    loop {
        for context in &contexts {
            let resp = fetcher.fetch_data(context, ROW_AMOUNT, page).await?;

            loop {
                // No new extrinsics were found, continue with next account.
                if resp.is_empty() {
                    break;
                }
                // New extrinsics are all on one page. Insert those into the
                // database and continue with the next account.
                else if resp.new_count() < ROW_AMOUNT {
                    // The cache tries to filter all unprocessed extrinsics, but
                    // the cache is not persistent and is wiped on application
                    // shutdown. The database has a 'unique' constraint on
                    // extrinsics hashes and this method will return how many
                    // extrinsics have been *newly* inserted into the database.
                    // If it's 0, then no new extrinsics were detected. Continue
                    // with the next account.
                    if fetcher.store_data(context, &resp).await? == 0 {
                        break;
                    }
                }

                page += 1;
            }

            // Reset to page 1.
            page = 1;
        }
    }

    Ok(())
}
