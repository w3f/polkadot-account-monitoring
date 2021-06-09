#[macro_use]
extern crate serde;
#[macro_use]
extern crate async_trait;

use anyhow::Error;

mod chain_api;
mod database;
mod system;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Context {
    stash: String,
    network: Network,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Network {
    Polkadot,
    Kusama,
}

impl Context {
    pub fn as_str(&self) -> &str {
        self.stash.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Context {
        pub fn alice() -> Self {
            Context {
                stash: "1a2YiGNu1UUhJtihq8961c7FZtWGQuWDVMWTNBKJdmpGhZP".to_string(),
                network: Network::Polkadot,
            }
        }
        pub fn bob() -> Self {
            Context {
                stash: "1b3NhsSEqWSQwS6nPGKgCrSjv9Kp13CnhraLV5Coyd8ooXB".to_string(),
                network: Network::Polkadot,
            }
        }
        pub fn eve() -> Self {
            Context {
                stash: "1cNyFSmLW4ofr7xh38za6JxLFxcu548LPcfc1E6L9r57SE3".to_string(),
                network: Network::Polkadot,
            }
        }
    }
}
