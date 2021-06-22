use crate::database::DatabaseReader;
use crate::publishing::Publisher;
use crate::Context;
use crate::Result;
use std::sync::Arc;
use tokio::sync::RwLock;

mod transfers;

pub use transfers::{TransferReport, TransferReportGenerator};

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Occurrence {
    Daily,
    Weekly,
    Monthly,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Offset {
    offset: u32,
    occurrence: Occurrence,
}

impl Offset {
    pub fn new(offset: u32, occurrence: Occurrence) -> Self {
        Offset {
            offset: offset,
            occurrence: occurrence,
        }
    }
}

// TODO: Is this type constraint required here?
#[async_trait]
pub trait GenerateReport<T: Publisher> {
    type Data;
    type Report;
    type Config;

    fn name() -> &'static str;
    async fn qualifies(&self) -> Result<Vec<Offset>>;
    async fn fetch_data(&self, offset: &Offset) -> Result<Option<Self::Data>>;
    async fn generate(&self, data: &Self::Data) -> Result<Vec<Self::Report>>;
    async fn publish(
        &self,
        publisher: Arc<T>,
        info: <T as Publisher>::Info,
        report: Self::Report,
    ) -> Result<()>;
    async fn track_offset(&self, offset: Offset) -> Result<()>;
}
