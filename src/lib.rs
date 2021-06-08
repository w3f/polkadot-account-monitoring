#[macro_use]
extern crate serde;

use anyhow::Error;

mod chain_api;
mod database;
mod system;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Stash {
    network: Network,
    stash: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Network {
    Polkadot,
    Kusama,
}

impl Stash {
    pub fn as_str(&self) -> &str {
        self.stash.as_str()
    }
}
