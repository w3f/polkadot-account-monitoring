use super::GenerateReport;
use crate::chain_api::Transfer;
use crate::database::{ContextData, DatabaseReader};
use crate::publishing::GoogleStoragePayload;
use crate::publishing::Publisher;
use crate::{Context, Result, Timestamp};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct RewardSlashGenerator<'a> {
    reader: DatabaseReader,
    contexts: Arc<RwLock<Vec<Context>>>,
    _p: PhantomData<&'a ()>,
}

impl<'a> RewardSlashGenerator<'a> {
    pub fn new(db: DatabaseReader, contexts: Arc<RwLock<Vec<Context>>>) -> Self {
        RewardSlashGenerator {
            reader: db,
            contexts: contexts,
            _p: PhantomData,
        }
    }
}

#[async_trait]
impl<'a, T> GenerateReport<T> for RewardSlashGenerator<'a>
where
    T: 'static + Send + Sync + Publisher,
    <T as Publisher>::Data: Send + Sync + From<()>,
    <T as Publisher>::Info: Send + Sync,
{
    type Data = Vec<ContextData<'a, Transfer>>;
    type Report = ();

    fn name() -> &'static str {
        "RewardSlashGenerator"
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
