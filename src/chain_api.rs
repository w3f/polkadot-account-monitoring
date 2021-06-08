use crate::{Context, Result};
use reqwest::Client;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashSet;

pub struct ChainApi {
    client: Client,
    cache_extrinsics: HashSet<ExtrinsicHash>,
}

impl ChainApi {
    pub fn new() -> Self {
        ChainApi {
            client: Client::new(),
            cache_extrinsics: HashSet::new(),
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
    pub async fn request_extrinsics(
        &mut self,
        stash: &Context,
        row: usize,
        page: usize,
    ) -> Result<Response<ExtrinsicsPage>> {
        let mut resp: Response<ExtrinsicsPage> = self
            .post(
                "https://polkadot.api.subscan.io/api/scan/extrinsics",
                &PageBody {
                    address: stash.as_str(),
                    row: row,
                    page: page,
                },
            )
            .await?;

        // Only keep unprocessed extrinsic hashes.
        resp.data
            .extrinsics
            .retain(|extrinsic| self.cache_extrinsics.contains(&extrinsic.extrinsic_hash));

        // Cache new extrinsic hashes.
        resp.data.extrinsics.iter().for_each(|extrinsic| {
            self.cache_extrinsics
                .insert(extrinsic.extrinsic_hash.clone());
        });

        Ok(resp)
    }
    pub async fn request_reward_slash(
        &mut self,
        stash: &Context,
        row: usize,
        page: usize,
    ) -> Result<Response<RewardsSlashesPage>> {
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

        // Only keep unprocessed extrinsic hashes.
        resp.data
            .list
            .retain(|reward_slash| self.cache_extrinsics.contains(&reward_slash.extrinsic_hash));

        // Cache new extrinsic hashes.
        resp.data.list.iter().for_each(|reward_slash| {
            self.cache_extrinsics
                .insert(reward_slash.extrinsic_hash.clone());
        });

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
    code: usize,
    data: T,
    message: String,
    ttl: usize,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtrinsicsPage {
    pub count: i64,
    pub extrinsics: Vec<Extrinsic>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Extrinsic {
    pub account_display: serde_json::Value,
    pub account_id: String,
    pub account_index: String,
    pub block_num: i64,
    pub block_timestamp: i64,
    pub call_module: String,
    pub call_module_function: String,
    pub extrinsic_hash: ExtrinsicHash,
    pub extrinsic_index: String,
    pub fee: String,
    pub nonce: i64,
    pub params: String,
    pub signature: String,
    pub success: bool,
}

#[derive(Default, Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ExtrinsicHash(String);

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RewardsSlashesPage {
    count: usize,
    pub list: Vec<RewardSlash>,
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
    pub extrinsic_hash: ExtrinsicHash,
    pub event_idx: i64,
}
