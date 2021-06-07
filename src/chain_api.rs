use crate::{Address, Result};
use reqwest::Client;

pub struct ChainApi {
    client: Client,
}

impl ChainApi {
    pub fn new() -> Self {
        ChainApi {
            client: Client::new(),
        }
    }
    async fn request_transfers(
        &self,
        address: Address,
        row: usize,
        page: usize,
    ) -> Result<Vec<Transfer>> {
        #[derive(Serialize)]
        struct Body<'a> {
            address: &'a str,
            row: usize,
            page: usize,
        }

        Ok(self
            .client
            .post("https://polkadot.api.subscan.io/api/scan/transfers")
            .json(&Body {
                address: address.as_str(),
                row: row,
                page: page,
            })
            .send()
            .await?
            .json()
            .await?)
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transfer {
    pub amount: String,
    #[serde(rename = "block_num")]
    pub block_num: i64,
    #[serde(rename = "block_timestamp")]
    pub block_timestamp: i64,
    #[serde(rename = "extrinsic_index")]
    pub extrinsic_index: String,
    pub fee: String,
    pub from: String,
    #[serde(rename = "from_account_display")]
    pub from_account_display: FromAccountDisplay,
    pub hash: String,
    pub module: String,
    pub nonce: i64,
    pub success: bool,
    pub to: String,
    #[serde(rename = "to_account_display")]
    pub to_account_display: ToAccountDisplay,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FromAccountDisplay {
    #[serde(rename = "account_index")]
    pub account_index: String,
    pub address: String,
    pub display: String,
    pub identity: bool,
    //pub judgements: ::serde_json::Value,
    pub parent: String,
    #[serde(rename = "parent_display")]
    pub parent_display: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToAccountDisplay {
    #[serde(rename = "account_index")]
    pub account_index: String,
    pub address: String,
    pub display: String,
    pub identity: bool,
    //pub judgements: ::serde_json::Value,
    pub parent: String,
    #[serde(rename = "parent_display")]
    pub parent_display: String,
}
