use crate::publishing::Publisher;
use crate::Result;
use std::sync::Arc;

mod transfers;
mod rewards_slashes;

pub use transfers::{TransferReportGenerator, TransferReportRaw};

// TODO: Is this type constraint required here?
#[async_trait]
pub trait GenerateReport<T: Publisher> {
    type Data;
    type Report;

    fn name() -> &'static str;
    async fn fetch_data(&self) -> Result<Option<Self::Data>>;
    async fn generate(&self, data: &Self::Data) -> Result<Vec<Self::Report>>;
    async fn publish(
        &self,
        publisher: Arc<T>,
        info: <T as Publisher>::Info,
        report: Self::Report,
    ) -> Result<()>;
}
