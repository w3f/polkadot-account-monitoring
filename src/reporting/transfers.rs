use super::{GenerateReport, Occurrence, TimeBatches};
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
pub enum TransferReport {
    All(TransferReportEntry),
    Summary(TransferReportEntry),
}

#[derive(Debug, Clone)]
pub struct TransferReportEntry {
    name: String,
    raw: String,
}

pub struct TransferReportGenerator<'a> {
    reader: DatabaseReader,
    contexts: Arc<RwLock<Vec<Context>>>,
    module_id: ReportModuleId,
    occurrences: Vec<Occurrence>,
    _p: PhantomData<&'a ()>,
}

impl<'a> TransferReportGenerator<'a> {
    pub fn new(
        db: DatabaseReader,
        contexts: Arc<RwLock<Vec<Context>>>,
        module_id: ReportModuleId,
        occurrences: Vec<Occurrence>,
    ) -> Self {
        TransferReportGenerator {
            reader: db,
            contexts: contexts,
            module_id: module_id,
            occurrences: occurrences,
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
    type Config = ReportTransferConfig;

    fn name() -> &'static str {
        "TransferReportGenerator"
    }
    async fn time_batches(&self) -> Result<TimeBatches> {
        unimplemented!()
        /*
        let mut offsets = vec![];
        for occurrence in &self.occurrences {
            let offset = self
                .reader
                .fetch_checkpoint_offset(&self.module_id, &occurrence)
                .await?;
            if offset > 0 {
                //offsets.push(TimeBatches::new(offset, *occurrence));
            }
        }

        Ok(offsets)
        */
    }
    async fn fetch_data(&self, batches: &TimeBatches) -> Result<Option<Self::Data>> {
        let contexts = self.contexts.read().await;
        let (from, to) = {
            if let Some(min_max) = batches.min_max() {
                min_max
            } else {
                return Ok(None)
            }
        };
        let data = self
            .reader
            .fetch_transfers(contexts.as_slice(), from, to)
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
    }
    async fn generate(&self, offset: &TimeBatches, data: &Self::Data) -> Result<Vec<Self::Report>> {
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
            TransferReport::All(TransferReportEntry {
                name: "".to_string(),
                raw: raw_all,
            }),
            TransferReport::Summary(TransferReportEntry {
                name: "".to_string(),
                raw: raw_summary,
            }),
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
    async fn track_latest(&self, batches: &TimeBatches) -> Result<()> {
        unimplemented!()
    }
}

impl From<TransferReport> for GoogleStoragePayload {
    fn from(val: TransferReport) -> Self {
        use TransferReport::*;

        match val {
            All(entry) => GoogleStoragePayload {
                name: format!("report_transfer_all_{}.csv", entry.name),
                mime_type: "application/vnd.google-apps.document".to_string(),
                body: entry.raw.into_bytes(),
                is_public: false,
            },
            Summary(entry) => GoogleStoragePayload {
                name: format!("report_transfer_summary_{}.csv", entry.name),
                mime_type: "application/vnd.google-apps.document".to_string(),
                body: entry.raw.into_bytes(),
                is_public: false,
            },
        }
    }
}
