use crate::chain_api::{
    ChainApi, NominationsPage, Response, RewardsSlashesPage, Transfer, TransfersPage,
};
use crate::database::{ContextData, Database, DatabaseReader};
use crate::{Context, Result, Timestamp};
use google_drive::GoogleDrive as RawGoogleDrive;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
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

    fn name() -> &'static str {
        "TransferFetcher"
    }
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

    fn name() -> &'static str {
        "RewardsSlashesFetcher"
    }
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

pub struct NominationsFetcher {
    db: Database,
    api: Arc<ChainApi>,
}

#[async_trait]
impl FetchChainData for NominationsFetcher {
    type Data = Response<NominationsPage>;

    fn name() -> &'static str {
        "NominationsFetcher"
    }
    fn new(db: Database, api: Arc<ChainApi>) -> Self {
        NominationsFetcher { db: db, api: api }
    }
    async fn fetch_data(&self, context: &Context, _row: usize, _page: usize) -> Result<Self::Data> {
        self.api.request_nominations(context).await
    }
    async fn store_data(&self, context: &Context, data: &Self::Data) -> Result<usize> {
        self.db.store_nomination_event(context, data).await
    }
}

#[async_trait]
pub trait FetchChainData {
    type Data: Send + Sync + std::fmt::Debug + DataInfo;

    fn name() -> &'static str;
    fn new(db: Database, api: Arc<ChainApi>) -> Self;
    async fn fetch_data(&self, _: &Context, row: usize, page: usize) -> Result<Self::Data>;
    async fn store_data(&self, _: &Context, data: &Self::Data) -> Result<usize>;
}

pub trait DataInfo {
    fn is_empty(&self) -> bool;
}

#[async_trait]
impl DataInfo for Response<TransfersPage> {
    fn is_empty(&self) -> bool {
        self.data.transfers.is_none()
    }
}

#[async_trait]
impl DataInfo for Response<RewardsSlashesPage> {
    fn is_empty(&self) -> bool {
        self.data.list.is_none()
    }
}

#[async_trait]
impl DataInfo for Response<NominationsPage> {
    fn is_empty(&self) -> bool {
        self.data.list.is_none()
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Module {
    Transfer,
    RewardsSlashes,
    Nominations,
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
            Module::Nominations => self.run_fetcher::<NominationsFetcher>().await,
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
                    loop {
                        let resp = fetcher.fetch_data(context, ROW_AMOUNT, page).await?;

                        // No entires were found, continue with next account.
                        if resp.is_empty() {
                            debug!(
                                "{}: No new entries were found for {:?}, moving on...",
                                T::name(),
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
                        let newly_inserted = fetcher.store_data(context, &resp).await?;
                        if newly_inserted == 0 {
                            debug!(
                                "{}: No new entries were found for {:?}, moving on...",
                                T::name(),
                                context
                            );
                            break;
                        }

                        info!(
                            "{}: {} new entries found for {:?}",
                            T::name(),
                            newly_inserted,
                            context
                        );

                        // If new extrinsics were all on one page, continue with
                        // the next account. Otherwise, fetch the next page.
                        if newly_inserted < ROW_AMOUNT {
                            debug!(
                                "{}: All new entries have been fetched for {:?}, \
                            continuing with the next accounts.",
                                T::name(),
                                context
                            );
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
                    error!(
                        "Failed task while running fetcher '{}': {:?}",
                        T::name(),
                        err
                    );
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

pub struct ReportGenerator {
    db: DatabaseReader,
    contexts: Arc<RwLock<Vec<Context>>>,
}

impl ReportGenerator {
    pub fn new(db: DatabaseReader) -> Self {
        ReportGenerator {
            db: db,
            contexts: Default::default(),
        }
    }
    pub async fn add_contexts(&mut self, mut contexts: Vec<Context>) {
        self.contexts.write().await.append(&mut contexts);
    }
    async fn run_generator<T, P>(&self, generator: T, publisher: P, info: <P as Publisher>::Info)
    where
        T: 'static + Send + Sync + GenerateReport<P>,
        P: 'static + Send + Sync + Publisher,
        <T as GenerateReport<P>>::Data: Send + Sync,
        <T as GenerateReport<P>>::Report: Send + Sync,
        <P as Publisher>::Info: Send + Sync + Clone,
    {
        async fn local<T, P>(
            generator: &T,
            publisher: Arc<P>,
            info: <P as Publisher>::Info,
        ) -> Result<()>
        where
            P: 'static + Send + Sync + Publisher,
            T: 'static + Send + Sync + GenerateReport<P>,
            <P as Publisher>::Info: Send + Sync + Clone,
        {
            loop {
                if let Some(data) = generator.fetch_data().await? {
                    for report in generator.generate(&data).await? {
                        generator
                            .publish(Arc::clone(&publisher), info.clone(), report)
                            .await?;
                    }
                }

                sleep(Duration::from_secs(LOOP_INTERVAL)).await;
            }
        }

        let publisher = Arc::new(publisher);
        tokio::spawn(async move {
            loop {
                if let Err(err) =
                    local::<T, P>(&generator, Arc::clone(&publisher), info.clone()).await
                {
                    error!(
                        "Failed task while running report generator '{}': {:?}",
                        T::name(),
                        err
                    );
                }

                sleep(Duration::from_secs(FAILED_TASK_SLEEP)).await;
            }
        });
    }
}

#[async_trait]
trait GenerateReport<T: Publisher> {
    type Data;
    type Report;

    fn name() -> &'static str;
    async fn fetch_data(&self) -> Result<Option<Self::Data>>;
    async fn generate(&self, data: &Self::Data) -> Result<Vec<Self::Report>>;
    async fn publish(
        &self,
        publisher: Arc<T>,
        info: <T as Publisher>::Info,
        report: Self::Report,
    ) -> Result<()>;
}

#[async_trait]
trait Publisher {
    type Data;
    type Info;

    async fn upload_data(&self, info: Self::Info, data: Self::Data) -> Result<()>;
}

pub struct GoogleDrive {
    drive: RawGoogleDrive,
}

#[async_trait]
impl Publisher for GoogleDrive {
    type Data = StoragePayload;
    type Info = ();

    async fn upload_data(&self, info: Self::Info, data: Self::Data) -> Result<()> {
        self.drive
            .upload_to_cloud_storage(
                &data.bucket,
                &data.name,
                &data.mime_type,
                &data.body,
                data.is_public,
            )
            .await
            .map(|_| ())
            .map_err(|err| err.into())
    }
}

impl From<TransferReportRaw> for StoragePayload {
    fn from(val: TransferReportRaw) -> Self {
        unimplemented!()
    }
}

pub struct StoragePayload {
    bucket: String,
    name: String,
    mime_type: String,
    body: Vec<u8>,
    is_public: bool,
}

pub struct TransferReportGenerator<'a> {
    report_range: u64,
    last_report: Option<Timestamp>,
    reader: DatabaseReader,
    contexts: Arc<RwLock<Vec<Context>>>,
    _p: PhantomData<&'a ()>,
}

impl<'a> TransferReportGenerator<'a> {
    pub fn new(db: DatabaseReader, report_range: u64, contexts: Arc<RwLock<Vec<Context>>>) -> Self {
        TransferReportGenerator {
            report_range: report_range,
            last_report: None,
            reader: db,
            contexts: contexts,
            _p: PhantomData,
        }
    }
}

pub enum TransferReportRaw {
    All(String),
    Summary(String),
}

#[async_trait]
impl<'a, T> GenerateReport<T> for TransferReportGenerator<'a>
where
    T: 'static + Send + Sync + Publisher,
    <T as Publisher>::Data: Send + Sync + From<TransferReportRaw>,
    <T as Publisher>::Info: Send + Sync,
{
    type Data = Vec<ContextData<'a, Transfer>>;
    type Report = TransferReportRaw;

    fn name() -> &'static str {
        "TransferReportGenerator"
    }
    async fn fetch_data(&self) -> Result<Option<Self::Data>> {
        let now = Timestamp::now();
        let last_report = self.last_report.unwrap_or(Timestamp::from(0));

        if last_report < (now - Timestamp::from(self.report_range)) {
            let contexts = self.contexts.read().await;
            let data = self
                .reader
                .fetch_transfers(contexts.as_slice(), last_report, now)
                .await?;

            Ok(Some(data))
        } else {
            Ok(None)
        }
    }
    async fn generate(&self, data: &Self::Data) -> Result<Vec<Self::Report>> {
        let contexts = self.contexts.read().await;

        // List all transfers.
        let mut raw_all =
            String::from("Block Number,Block Timestamp,From,To,Amount,Extrinsic Index,Success");
        // Create summary of all accounts.
        let mut summary: HashMap<Context, u64> = HashMap::new();

        for entry in data {
            // TODO: Improve performance here.
            let context = contexts
                .iter()
                .find(|c| c.stash == entry.context_id.stash.clone().into_owned())
                .ok_or(anyhow!("No context found while generating reports"))?;

            let amount = entry.data.amount.parse::<u64>()?;

            let data = entry.data.to_owned();
            raw_all.push_str(&format!(
                "{},{},{},{},{},{},{}",
                data.block_num,
                data.block_timestamp,
                data.from,
                data.to,
                data.amount,
                data.extrinsic_index,
                data.success,
            ));

            // Sum amount for each context.
            summary
                .entry(context.clone())
                .and_modify(|a| *a += amount)
                .or_insert(amount);
        }

        let mut raw_summary = String::from("Network,Address,Description,Amount\n");

        for (context, amount) in summary {
            raw_summary.push_str(&format!(
                "{},{},{},{}\n",
                context.network.as_str(),
                context.stash,
                context.description,
                amount
            ))
        }

        Ok(vec![
            TransferReportRaw::All(raw_all),
            TransferReportRaw::Summary(raw_summary),
        ])
    }
    async fn publish(
        &self,
        publisher: Arc<T>,
        info: <T as Publisher>::Info,
        report: Self::Report,
    ) -> Result<()> {
        publisher
            .upload_data(info, <T as Publisher>::Data::from(report))
            .await
            .map(|_| ())
            .map(|err| err.into())
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
