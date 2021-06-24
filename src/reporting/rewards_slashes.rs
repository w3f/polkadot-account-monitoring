/*  */
use super::GenerateReport;
use crate::chain_api::RewardSlash;
use crate::database::{ContextData, DatabaseReader};
use crate::publishing::GoogleStoragePayload;
use crate::publishing::Publisher;
use crate::{BlockNumber, Context, Network, Result};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::RwLock;

pub enum RewardSlashReport {
    All(String),
    Summary(String),
}

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
                BlockNumber::from(i64::MAX as u64),
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
        let mut raw_all = String::from("Network,Block Number,Address,Description,Event,Value\n");
        let mut summary: HashMap<Context, (f64, f64)> = HashMap::new();

        for entry in data {
            // TODO: Improve performance here.
            let context = contexts
                .iter()
                .find(|c| c.stash == entry.context_id.stash.clone().into_owned())
                .ok_or(anyhow!("No context found while generating reports"))?;

            let data = entry.data.as_ref();
            let amount = data.amount.parse::<f64>()?;
            let amount = match context.network {
                Network::Kusama => amount / 1_000_000_000_000.0,
                Network::Polkadot => amount / 10_000_000_000.0,
            };

            raw_all.push_str(&format!(
                "{},{},{},{},{},{}\n",
                context.network.as_str(),
                data.block_num,
                context.stash,
                context.description,
                data.event_id,
                amount,
            ));

            let event_id = match data.event_id.as_str() {
                "Reward" => true,
                "Slash" => false,
                _ => return Err(anyhow!("Received unknown event id")),
            };

            summary
                .entry(context.clone())
                .and_modify(|(r, s)| match event_id {
                    true => *r += amount,
                    false => *s += amount,
                })
                .or_insert({
                    match event_id {
                        true => (amount, 0.0),
                        false => (0.0, amount),
                    }
                });
        }

        let mut raw_summary = String::from("Network,Address,Description,Reward,Slash\n");
        for (context, (reward, slash)) in summary {
            raw_summary.push_str(&format!(
                "{},{},{},{},{}\n",
                context.network.as_str(),
                context.stash,
                context.description,
                reward,
                slash,
            ));
        }

        Ok(vec![
            RewardSlashReport::All(raw_all),
            RewardSlashReport::Summary(raw_summary),
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

impl From<RewardSlashReport> for GoogleStoragePayload {
    fn from(val: RewardSlashReport) -> Self {
        let date = chrono::offset::Utc::now().to_rfc3339();

        match val {
            RewardSlashReport::All(content) => GoogleStoragePayload {
                name: format!("rewards_slashes_all_{}.csv", date),
                mime_type: "application/vnd.google-apps.document".to_string(),
                body: content.into_bytes(),
                is_public: false,
            },
            RewardSlashReport::Summary(content) => GoogleStoragePayload {
                name: format!("rewards_slashes_summary_{}.csv", date),
                mime_type: "application/vnd.google-apps.document".to_string(),
                body: content.into_bytes(),
                is_public: false,
            },
        }
    }
}
