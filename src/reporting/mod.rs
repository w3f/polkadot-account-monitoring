use crate::database::DatabaseReader;
use crate::publishing::Publisher;
use crate::Context;
use crate::Result;
use std::sync::Arc;
use tokio::sync::RwLock;

mod transfers;

pub use transfers::{TransferReportGenerator, TransferReportRaw};

// TODO: Is this type constraint required here?
#[async_trait]
pub trait GenerateReport<T: Publisher> {
    type Data;
    type Report;
    type Config;

    fn name() -> &'static str;
    fn new(db: DatabaseReader, contexts: Arc<RwLock<Vec<Context>>>, config: Self::Config) -> Self;
    async fn fetch_data(&self) -> Result<Option<Self::Data>>;
    async fn generate(&self, data: &Self::Data) -> Result<Vec<Self::Report>>;
    async fn publish(
        &self,
        publisher: Arc<T>,
        info: <T as Publisher>::Info,
        report: Self::Report,
    ) -> Result<()>;
}
