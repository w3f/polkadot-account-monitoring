use super::GenerateReport;
use crate::chain_api::Nomination;
use crate::database::{ContextData, DatabaseReader};
use crate::publishing::{GoogleStoragePayload, Publisher};
use crate::{Context, Result};
use chrono::{TimeZone, Utc};
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct NominationReport(String);

pub struct NominationReportGenerator<'a> {
    reader: DatabaseReader,
    contexts: Arc<RwLock<Vec<Context>>>,
    _p: PhantomData<&'a ()>,
}

impl<'a> NominationReportGenerator<'a> {
    pub fn new(db: DatabaseReader, contexts: Arc<RwLock<Vec<Context>>>) -> Self {
        NominationReportGenerator {
            reader: db,
            contexts: contexts,
            _p: PhantomData,
        }
    }
}

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
        if data.is_empty() {
            return Ok(vec![]);
        }

        debug!(
            "{}: Generating reports of {} database entries",
            <Self as GenerateReport<T>>::name(),
            data.len()
        );

        let contexts = self.contexts.read().await;

        let mut report =
            String::from("Detected,Network,Address,Description,Validator,Display Name\n");

        for entry in data {
            // TODO: Improve performance here.
            let context = contexts
                .iter()
                .find(|c| c.stash == entry.context_id.stash.clone().into_owned())
                .ok_or(anyhow!("No context found while generating reports"))?;

            let data = entry.data.as_ref();
            report.push_str(&format!(
                "{},{},{},{},{},{}\n",
                Utc.timestamp(entry.timestamp.as_secs() as i64, 0)
                    .to_rfc3339(),
                context.network.as_str(),
                context.stash,
                context.description,
                data.stash_account_display.address,
                data.stash_account_display.display,
            ))
        }

        Ok(vec![NominationReport(report)])
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
        let date = chrono::offset::Utc::now().to_rfc3339();

        GoogleStoragePayload {
            name: format!("{}_nominations.csv", date),
            mime_type: "application/vnd.google-apps.document".to_string(),
            body: val.0.into_bytes(),
            is_public: false,
        }
    }
}
