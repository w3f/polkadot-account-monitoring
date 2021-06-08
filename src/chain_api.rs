use crate::{Context, Result};
use reqwest::Client;
use serde::{de::DeserializeOwned, Serialize};

pub struct ChainApi {
    client: Client,
}

impl ChainApi {
    pub fn new() -> Self {
        ChainApi {
            client: Client::new(),
        }
    }
    async fn post<T, R>(&self, url: &str, param: &T) -> Result<R>
    where
        T: Serialize,
        R: DeserializeOwned,
    {
        self.client
            .post(url)
            .json(param)
            .send()
            .await?
            .json()
            .await
            .map_err(|err| err.into())
    }
    pub async fn request_transfers(
        &self,
        stash: &Context,
        row: usize,
        page: usize,
    ) -> Result<Response<TransferPage>> {
        self.post(
            "https://polkadot.api.subscan.io/api/scan/transfers",
            &PageBody {
                address: stash.as_str(),
                row: row,
                page: page,
            },
        )
        .await
    }
    pub async fn request_reward_slash(
        &self,
        stash: &Context,
        row: usize,
        page: usize,
    ) -> Result<Response<RewardSlashPage>> {
        self.post(
            "https://polkadot.api.subscan.io/api/scan/account/reward_slash",
            &PageBody {
                address: stash.as_str(),
                row: row,
                page: page,
            },
        )
        .await
    }
}

#[derive(Serialize)]
struct PageBody<'a> {
    address: &'a str,
    row: usize,
    page: usize,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Response<T> {
    code: usize,
    data: T,
    message: String,
    ttl: usize,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TransferPage {
    pub count: usize,
    pub transfers: Vec<Transfer>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
#[serde(rename_all = "snake_case")]
pub struct FromAccountDisplay {
    #[serde(rename = "account_index")]
    pub account_index: String,
    pub Context: String,
    pub display: String,
    pub identity: bool,
    //pub judgements: ::serde_json::Value,
    pub parent: String,
    #[serde(rename = "parent_display")]
    pub parent_display: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ToAccountDisplay {
    #[serde(rename = "account_index")]
    pub account_index: String,
    pub Context: String,
    pub display: String,
    pub identity: bool,
    //pub judgements: ::serde_json::Value,
    pub parent: String,
    #[serde(rename = "parent_display")]
    pub parent_display: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RewardSlashPage {
    count: usize,
    list: Vec<RewardSlash>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RewardSlash {
    pub event_index: String,
    pub block_num: i64,
    pub extrinsic_idx: i64,
    pub module_id: String,
    pub event_id: String,
    pub params: String,
    pub extrinsic_hash: String,
    pub event_idx: i64,
}
