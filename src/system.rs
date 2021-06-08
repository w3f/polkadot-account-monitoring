use crate::chain_api::ChainApi;
use crate::database::Database;
use crate::{Context, Result};

const ROW_AMOUNT: usize = 25;

pub async fn run_transfer_listener(contextes: Vec<Context>) -> Result<()> {
    let mut chain = ChainApi::new();
    let db = Database::new("", "").await?;

    let mut page = 1;
    for context in &contextes {
        let resp = chain.request_extrinsics(context, ROW_AMOUNT, page).await?;
    }

    Ok(())
}
