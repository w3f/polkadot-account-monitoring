use super::{GenerateReport, Offset};
use crate::chain_api::Transfer;
use crate::core::{ReportModuleId, ReportTransferConfig};
use crate::database::{ContextData, DatabaseReader};
use crate::publishing::GoogleStoragePayload;
use crate::publishing::Publisher;
use crate::{Context, Result, Timestamp};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub enum TransferReportRaw {
    All(String),
    Summary(String),
}

pub struct TransferReportGenerator<'a> {
    reader: DatabaseReader,
    contexts: Arc<RwLock<Vec<Context>>>,
    module_id: ReportModuleId,
    _p: PhantomData<&'a ()>,
}

impl<'a> TransferReportGenerator<'a> {
    pub fn new(
        db: DatabaseReader,
        contexts: Arc<RwLock<Vec<Context>>>,
        module_id: ReportModuleId,
    ) -> Self {
        TransferReportGenerator {
            reader: db,
            contexts: contexts,
            module_id: module_id,
            _p: PhantomData,
        }
    }
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
    type Config = ReportTransferConfig;

    fn name() -> &'static str {
        "TransferReportGenerator"
    }
    async fn qualifies(&self) -> Result<Option<Offset>> {
        unimplemented!()
    }
    async fn fetch_data(&self, offset: &Offset) -> Result<Option<Self::Data>> {
        let now = Timestamp::now();
        // TODO: Delete this
        let last_report = Timestamp::now();

        // TODO: Delete this
        if last_report < (now - Timestamp::from(0)) {
            let contexts = self.contexts.read().await;
            let data = self
                .reader
                .fetch_transfers(contexts.as_slice(), last_report, now)
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

            // TODO: Update `last_report`

            Ok(Some(data))
        } else {
            Ok(None)
        }
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
        let mut raw_all =
            String::from("Block Number,Block Timestamp,From,To,Amount,Extrinsic Index,Success\n");
        // Create summary of all accounts.
        let mut summary: HashMap<Context, f64> = HashMap::new();

        for entry in data {
            // TODO: Improve performance here.
            let context = contexts
                .iter()
                .find(|c| c.stash == entry.context_id.stash.clone().into_owned())
                .ok_or(anyhow!("No context found while generating reports"))?;

            let amount = entry.data.amount.parse::<f64>()?;

            let data = entry.data.to_owned();
            raw_all.push_str(&format!(
                "{},{},{},{},{},{},{}\n",
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
            .await?;

        info!("Uploaded new report");

        Ok(())
    }
}

impl From<TransferReportRaw> for GoogleStoragePayload {
    fn from(val: TransferReportRaw) -> Self {
        use TransferReportRaw::*;

        match val {
            All(content) => GoogleStoragePayload {
                name: "report_transfer_all.csv".to_string(),
                mime_type: "application/vnd.google-apps.document".to_string(),
                body: content.into_bytes(),
                is_public: false,
            },
            Summary(content) => GoogleStoragePayload {
                name: "report_transfer_summary.csv".to_string(),
                mime_type: "application/vnd.google-apps.document".to_string(),
                body: content.into_bytes(),
                is_public: false,
            },
        }
    }
}
