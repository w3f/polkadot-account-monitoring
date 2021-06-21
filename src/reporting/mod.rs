use crate::Result;
use crate::publishing::Publisher;
use crate::database::DatabaseReader;
use crate::Context;
use tokio::sync::RwLock;
use std::sync::Arc;

mod transfers;

pub use transfers::{TransferReportRaw, TransferReportGenerator};

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
