use super::GenerateReport;
use crate::chain_api::Nomination;
use crate::database::{ContextData, DatabaseReader};
use crate::publishing::{GoogleStoragePayload, Publisher};
use crate::{Context, Result};
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct NominationReportGenerator<'a> {
    reader: DatabaseReader,
    contexts: Arc<RwLock<Vec<Context>>>,
    _p: PhantomData<&'a ()>,
}

pub struct NominationReport(String);

#[async_trait]
impl<'a, T> GenerateReport<T> for NominationReportGenerator<'a>
where
    T: 'static + Send + Sync + Publisher,
    <T as Publisher>::Data: Send + Sync + From<NominationReport>,
    <T as Publisher>::Info: Send + Sync,
{
    type Data = Vec<ContextData<'a, Nomination>>;
    type Report = NominationReport;

    fn name() -> &'static str {
        "NominationReportGenerator"
    }
    async fn fetch_data(&self) -> Result<Option<Self::Data>> {
        let contexts = self.contexts.read().await;
        let data = self
            .reader
            // Simply fetch everything as of now.
            .fetch_nominations(contexts.as_slice())
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
        unimplemented!()
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

impl From<NominationReport> for GoogleStoragePayload {
    fn from(val: NominationReport) -> Self {
        unimplemented!()
    }
}
