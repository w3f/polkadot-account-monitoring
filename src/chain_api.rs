use crate::{Context, Result};
use reqwest::header::{CONTENT_TYPE, USER_AGENT};
use reqwest::Client;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

const REQUEST_TIMEOUT: u64 = 5;

pub struct ChainApi {
    client: Client,
    guard_lock: Arc<Mutex<()>>,
}

impl ChainApi {
    pub fn new() -> Self {
        ChainApi {
            client: Client::new(),
            guard_lock: Arc::new(Mutex::new(())),
        }
    }
    async fn time_guard(&self) {
        let mutex = Arc::clone(&self.guard_lock);
        let guard = mutex.lock_owned().await;

        tokio::spawn(async move {
            // Capture guard, drops after sleeping period;
            let _ = guard;
            sleep(Duration::from_secs(REQUEST_TIMEOUT)).await;
        });
    }
    async fn post<T, R>(&self, url: &str, param: &T) -> Result<R>
    where
        T: Serialize,
        R: DeserializeOwned,
    {
        let headers = [
            ("X-API-Key".parse()?, "YOUR_KEY".parse()?),
            (CONTENT_TYPE, "application/json".parse()?),
            (USER_AGENT, "curl/7.68.0".parse()?),
        ]
        .iter()
        .cloned()
        .collect();

        self.time_guard().await;

        self.client
            .post(url)
            .headers(headers)
            .json(param)
            .send()
            .await?
            .json()
            .await
            .map_err(|err| err.into())
    }
    pub async fn request_transfer(
        &self,
        context: &Context,
        row: usize,
        page: usize,
    ) -> Result<Response<TransfersPage>> {
        Ok(self
            .post(
                &format!(
                    "https://{}.api.subscan.io/api/scan/transfers",
                    context.network().as_str()
                ),
                &PageBody {
                    address: context.as_str(),
                    row: row,
                    page: page,
                },
            )
            .await?)
    }
    pub async fn request_reward_slash(
        &self,
        context: &Context,
        row: usize,
        page: usize,
    ) -> Result<Response<RewardsSlashesPage>> {
        Ok(self
            .post(
                &format!(
                    "https://{}.api.subscan.io/api/scan/account/reward_slash",
                    context.network().as_str()
                ),
                &PageBody {
                    address: context.as_str(),
                    row: row,
                    page: page,
                },
            )
            .await?)
    }
    pub async fn request_nominations(
        &self,
        context: &Context,
        row: usize,
        page: usize,
    ) -> Result<Response<NominationsPage>> {
        Ok(self
            .post(
                &format!(
                    "https://{}.api.subscan.io/api/scan/staking/nominators",
                    context.network().as_str()
                ),
                &PageBody {
                    address: context.as_str(),
                    row: row,
                    page: page,
                },
            )
            .await?)
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
    pub code: usize,
    pub data: T,
    pub message: String,
    pub ttl: usize,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransfersPage {
    pub count: i64,
    pub transfers: Option<Vec<Transfer>>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Transfer {
    pub amount: String,
    pub block_num: i64,
    pub block_timestamp: i64,
    pub extrinsic_index: ExtrinsicIndex,
    pub fee: String,
    pub from: String,
    pub from_account_display: FromAccountDisplay,
    pub hash: String,
    pub module: String,
    pub nonce: i64,
    pub success: bool,
    pub to: String,
    pub to_account_display: ToAccountDisplay,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FromAccountDisplay {
    pub address: String,
    pub display: String,
    pub judgements: ::serde_json::Value,
    pub account_index: String,
    pub identity: bool,
    pub parent: Option<Parent>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToAccountDisplay {
    pub address: String,
    pub display: String,
    pub judgements: ::serde_json::Value,
    pub account_index: String,
    pub identity: bool,
    pub parent: Option<Parent>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Parent {
    pub address: String,
    pub display: String,
    pub sub_symbol: String,
    pub identity: bool,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NominationsPage {
    pub count: i64,
    pub list: Option<Vec<Validator>>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Validator {
    pub rank_validator: i64,
    pub nickname: String,
    pub validator_stash: String,
    pub validator_controller: String,
    pub bonded_nominators: String,
    pub bonded_owner: String,
    pub count_nominators: i64,
    pub validator_prefs_value: i64,
}

#[derive(Default, Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ExtrinsicIndex(String);

#[derive(Default, Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ExtrinsicHash(String);

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RewardsSlashesPage {
    count: i64,
    pub list: Option<Vec<RewardSlash>>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RewardSlash {
    pub event_index: String,
    pub block_num: i64,
    pub extrinsic_idx: i64,
    pub module_id: String,
    pub event_id: String,
    pub params: String,
    pub extrinsic_hash: ExtrinsicHash,
    pub event_idx: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    impl From<String> for ExtrinsicIndex {
        fn from(val: String) -> Self {
            ExtrinsicIndex(val)
        }
    }

    impl From<String> for ExtrinsicHash {
        fn from(val: String) -> Self {
            ExtrinsicHash(val)
        }
    }
}
