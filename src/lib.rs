#[macro_use]
extern crate serde;

use anyhow::Error;

mod chain_api;
mod database;

type Result<T> = std::result::Result<T, Error>;

struct Address {
    network: Network,
    address: String,
}

enum Network {
    Polkadot,
    Kusama,
}

impl Address {
    pub fn as_str(&self) -> &str {
        self.address.as_str()
    }
}
