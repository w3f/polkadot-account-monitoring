use super::GenerateReport;
use crate::chain_api::RewardSlash;
use crate::database::{ContextData, DatabaseReader};
use crate::publishing::GoogleStoragePayload;
use crate::publishing::Publisher;
use crate::{BlockNumber, Context, Result, Timestamp};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct RewardSlashReport(String);

pub struct RewardSlashReportGenerator<'a> {
    reader: DatabaseReader,
    contexts: Arc<RwLock<Vec<Context>>>,
    _p: PhantomData<&'a ()>,
}

impl<'a> RewardSlashReportGenerator<'a> {
    pub fn new(db: DatabaseReader, contexts: Arc<RwLock<Vec<Context>>>) -> Self {
        RewardSlashReportGenerator {
            reader: db,
            contexts: contexts,
            _p: PhantomData,
        }
    }
}

#[async_trait]
impl<'a, T> GenerateReport<T> for RewardSlashReportGenerator<'a>
where
    T: 'static + Send + Sync + Publisher,
    <T as Publisher>::Data: Send + Sync + From<RewardSlashReport>,
    <T as Publisher>::Info: Send + Sync,
{
    type Data = Vec<ContextData<'a, RewardSlash>>;
    type Report = RewardSlashReport;

    fn name() -> &'static str {
        "RewardSlashReportGenerator"
    }
    async fn fetch_data(&self) -> Result<Option<Self::Data>> {
        let contexts = self.contexts.read().await;
        let data = self
            .reader
            // Simply fetch everything as of now.
            .fetch_rewards_slashes(
                contexts.as_slice(),
                BlockNumber::from(0),
                BlockNumber::from(u64::MAX),
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
        #[derive(Debug, Clone, Deserialize)]
        struct Params {
            #[serde(rename = "type")]
            ty: String,
            value: String,
        }

        if data.is_empty() {
            return Ok(vec![]);
        }

        debug!(
            "{}: Generating reports of {} database entries",
            <Self as GenerateReport<T>>::name(),
            data.len()
        );

        let contexts = self.contexts.read().await;

        let mut raw_report =
            String::from("Network,Block Number,Address,Description,Event,Value\n");

        for entry in data {
            // TODO: Improve performance here.
            let context = contexts
                .iter()
                .find(|c| c.stash == entry.context_id.stash.clone().into_owned())
                .ok_or(anyhow!("No context found while generating reports"))?;

            let data = entry.data.as_ref();
            raw_report.push_str(&format!(
                "{},{},{},{},{},{}\n",
                context.network.as_str(),
                data.block_num,
                context.stash,
                context.description,
                data.event_id,
                {
                    let params: Vec<Params> = serde_json::from_str(&data.params)?;
                    let param =
                        params
                            .iter()
                            .find(|param| param.ty == "Balance")
                            .ok_or(anyhow!(
                                "balance not found when generating reports for rewards/slashes"
                            ))?;
                    param.value.to_owned()
                }
            ));
        }

        Ok(vec![RewardSlashReport(raw_report)])
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

impl From<RewardSlashReport> for GoogleStoragePayload {
    fn from(val: RewardSlashReport) -> Self {
        let date = chrono::offset::Utc::now().to_rfc3339();

        GoogleStoragePayload {
            name: format!("rewards_slashes_all_{}.csv", date),
            mime_type: "application/vnd.google-apps.document".to_string(),
            body: val.0.into_bytes(),
            is_public: false,
        }
    }
}
