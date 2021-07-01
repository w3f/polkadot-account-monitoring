use super::GenerateReport;
use crate::chain_api::Transfer;
use crate::database::{ContextData, DatabaseReader};
use crate::publishing::GoogleStoragePayload;
use crate::publishing::Publisher;
use crate::{Context, Result, Timestamp};
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct TransferReport(String);

pub struct TransferReportGenerator<'a> {
    reader: DatabaseReader,
    contexts: Arc<RwLock<Vec<Context>>>,
    _p: PhantomData<&'a ()>,
}

impl<'a> TransferReportGenerator<'a> {
    pub fn new(db: DatabaseReader, contexts: Arc<RwLock<Vec<Context>>>) -> Self {
        TransferReportGenerator {
            reader: db,
            contexts: contexts,
            _p: PhantomData,
        }
    }
}

#[async_trait]
impl<'a, T> GenerateReport<T> for TransferReportGenerator<'a>
where
    T: 'static + Send + Sync + Publisher,
    <T as Publisher>::Data: Send + Sync + From<TransferReport>,
    <T as Publisher>::Info: Send + Sync,
{
    type Data = Vec<ContextData<'a, Transfer>>;
    type Report = TransferReport;

    fn name() -> &'static str {
        "TransferReportGenerator"
    }
    async fn fetch_data(&self) -> Result<Option<Self::Data>> {
        let contexts = self.contexts.read().await;
        let data = self
            .reader
            // Simply fetch everything as of now.
            .fetch_transfers(
                contexts.as_slice(),
                Timestamp::from(0),
                Timestamp::from(i64::MAX as u64),
            )
            .await?;

        if data.is_empty() {
            return Ok(None);
        } else {
            debug!(
                "{}: Fetched {} entries from database",
                <Self as GenerateReport<T>>::name(),
                data.len()
            );
        }

        Ok(Some(data))
    }
    async fn generate(&self, data: &Self::Data) -> Result<Vec<Self::Report>> {
        if data.is_empty() {
            return Ok(vec![]);
        }

        debug!(
            "{}: Generating reports of {} database entries",
            <Self as GenerateReport<T>>::name(),
            data.len()
        );

        let contexts = self.contexts.read().await;

        // List all transfers.
        let mut report =
            String::from("Network,Block Number,Block Timestamp,From,Description,To,Amount,Extrinsic Index,Success\n");

        for entry in data {
            // TODO: Improve performance here.
            let context = contexts
                .iter()
                .find(|c| c.stash == entry.context_id.stash.clone().into_owned())
                .ok_or(anyhow!("No context found while generating reports"))?;

            let data = entry.data.as_ref();
            report.push_str(&format!(
                "{},{},{},{},{},{},{},{},{}\n",
                context.network.as_str(),
                data.block_num,
                data.block_timestamp,
                data.from,
                context.description,
                data.to,
                data.amount,
                data.extrinsic_index,
                data.success,
            ));
        }

        Ok(vec![TransferReport(report)])
    }
    async fn publish(
        &self,
        publisher: Arc<T>,
        info: <T as Publisher>::Info,
        report: Self::Report,
    ) -> Result<()> {
        publisher
            .upload_data(info, <T as Publisher>::Data::from(report))
            .await?;

        info!("Uploaded new report");

        Ok(())
    }
}

impl From<TransferReport> for GoogleStoragePayload {
    fn from(val: TransferReport) -> Self {
        let date = chrono::offset::Utc::now().to_rfc3339();

        GoogleStoragePayload {
            name: format!("{}_report_transfer.csv", date),
            mime_type: "application/vnd.google-apps.document".to_string(),
            body: val.0.into_bytes(),
            is_public: false,
        }
    }
}
