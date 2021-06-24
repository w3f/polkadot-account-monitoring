use super::GenerateReport;
use crate::database::{DatabaseReader, ContextData};
use crate::{Context, Result};
use crate::publishing::{Publisher, GoogleStoragePayload};
use crate::chain_api::Nomination;
use tokio::sync::RwLock;
use std::sync::Arc;
use std::marker::PhantomData;

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
	unimplemented!()
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
	unimplemented!()
    }
}

impl From<NominationReport> for GoogleStoragePayload {
    fn from(val: NominationReport) -> Self {
	unimplemented!()
    }
}
