use crate::{Context, Result};
use reqwest::header::{CONTENT_TYPE, USER_AGENT};
use reqwest::Client;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, Duration};

const REQUEST_TIMEOUT: u64 = 5;

pub struct ChainApi {
    client: Client,
    cache_extrinsics: Arc<RwLock<HashSet<ExtrinsicHash>>>,
    cache_index: Arc<RwLock<HashSet<ExtrinsicIndex>>>,
    guard_lock: Arc<Mutex<()>>,
}

impl ChainApi {
    pub fn new() -> Self {
        ChainApi {
            client: Client::new(),
            cache_extrinsics: Arc::new(RwLock::new(HashSet::new())),
            cache_index: Arc::new(RwLock::new(HashSet::new())),
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
        self.time_guard().await;

        let mut resp: Response<TransfersPage> = self
            .post(
                "https://polkadot.api.subscan.io/api/scan/transfers",
                &PageBody {
                    address: context.as_str(),
                    row: row,
                    page: page,
                },
            )
            .await?;

        if resp.data.transfers.is_none() {
            return Ok(resp);
        }

        // Only keep unprocessed extrinsic indexes.
        {
            let cache = self.cache_index.read().await;
            resp.data
                .transfers
                .as_mut()
                .unwrap()
                .retain(|transfer| !cache.contains(&transfer.extrinsic_index));
        }

        // Cache new transfer hashes.
        {
            let mut cache = self.cache_index.write().await;
            resp.data
                .transfers
                .as_ref()
                .unwrap()
                .iter()
                .for_each(|transfer| {
                    cache.insert(transfer.extrinsic_index.clone());
                });
        }

        Ok(resp)
    }
    pub async fn request_reward_slash(
        &mut self,
        stash: &Context,
        row: usize,
        page: usize,
    ) -> Result<Response<RewardsSlashesPage>> {
        self.time_guard().await;

        let mut resp: Response<RewardsSlashesPage> = self
            .post(
                "https://polkadot.api.subscan.io/api/scan/account/reward_slash",
                &PageBody {
                    address: stash.as_str(),
                    row: row,
                    page: page,
                },
            )
            .await?;

        if resp.data.list.is_none() {
            return Ok(resp);
        }

        // Only keep unprocessed extrinsic hashes.
        {
            let cache = self.cache_extrinsics.read().await;
            resp.data
                .list
                .as_mut()
                .unwrap()
                .retain(|reward_slash| !cache.contains(&reward_slash.extrinsic_hash));
        }

        // Cache new extrinsic hashes.
        {
            let mut cache = self.cache_extrinsics.write().await;
            resp.data
                .list
                .as_ref()
                .unwrap()
                .iter()
                .for_each(|reward_slash| {
                    cache.insert(reward_slash.extrinsic_hash.clone());
                });
        }

        Ok(resp)
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

#[derive(Default, Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ExtrinsicIndex(String);

#[derive(Default, Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ExtrinsicHash(String);

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RewardsSlashesPage {
    count: usize,
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
