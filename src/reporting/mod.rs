use crate::database::DatabaseReader;
use crate::publishing::Publisher;
use crate::Context;
use crate::Result;
use std::sync::Arc;
use tokio::sync::RwLock;

mod transfers;

pub use transfers::{TransferReportGenerator, TransferReportRaw};

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Occurrence {
    Daily,
    Weekly,
    Monthly,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Offset(u32);

// TODO: Is this type constraint required here?
#[async_trait]
pub trait GenerateReport<T: Publisher> {
    type Data;
    type Report;
    type Config;

    fn name() -> &'static str;
    fn new(db: DatabaseReader, contexts: Arc<RwLock<Vec<Context>>>, config: Self::Config) -> Self;
    async fn qualifies(&self) -> Result<Option<Offset>>;
    async fn fetch_data(&self, offset: &Offset) -> Result<Option<Self::Data>>;
    async fn generate(&self, data: &Self::Data) -> Result<Vec<Self::Report>>;
    async fn publish(
        &self,
        publisher: Arc<T>,
        info: <T as Publisher>::Info,
        report: Self::Report,
    ) -> Result<()>;
}
