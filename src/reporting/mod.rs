use crate::database::DatabaseReader;
use crate::publishing::Publisher;
use crate::Context;
use crate::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use chrono::{DateTime, Utc};

mod transfers;

pub use transfers::{TransferReport, TransferReportGenerator};

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Occurrence {
    Daily,
    Weekly,
    Monthly,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimeBatches {
    pub batches: Vec<(DateTime<Utc>, DateTime<Utc>)>,
}

impl TimeBatches {
    pub fn min_max(&self) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
        Some((
            *self.batches.iter().map(|(from, _)| from).min()?,
            *self.batches.iter().map(|(_, to)| to).max()?,
        ))
    }
}

// TODO: Is this type constraint required here?
#[async_trait]
pub trait GenerateReport<T: Publisher> {
    type Data;
    type Report;
    type Config;

    fn name() -> &'static str;
    async fn time_batches(&self) -> Result<TimeBatches>;
    async fn fetch_data(&self, batches: &TimeBatches) -> Result<Option<Self::Data>>;
    async fn generate(&self, batches: &TimeBatches, data: &Self::Data) -> Result<Vec<Self::Report>>;
    async fn publish(
        &self,
        publisher: Arc<T>,
        info: <T as Publisher>::Info,
        report: Self::Report,
    ) -> Result<()>;
    async fn track_latest(&self, batches: &TimeBatches) -> Result<()>;
}
