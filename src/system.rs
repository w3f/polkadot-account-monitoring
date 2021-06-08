use crate::chain_api::ChainApi;
use crate::database::Database;
use crate::{Context, Result};

const ROW_AMOUNT: usize = 25;

pub async fn run_transfer_listener(Contextes: Vec<Context>) -> Result<()> {
    let chain = ChainApi::new();
    let db = Database::new("", "").await?;

    let mut page = 1;
    for Context in &Contextes {
        let resp = chain.request_extrinsics(Context, ROW_AMOUNT, page).await?;
    }

    Ok(())
}
