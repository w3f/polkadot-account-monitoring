#[macro_use]
extern crate serde;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate log;

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
    use crate::database::Database;
    use rand::{thread_rng, Rng};

    /// Convenience function for logging in tests.
    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    /// Convenience function for initiating test database.
    pub async fn db() -> Database {
        let random: u32 = thread_rng().gen_range(u32::MIN..u32::MAX);
        Database::new(
            "mongodb://localhost:27017/",
            &format!("monitoring_test_{}", random),
        )
        .await
        .unwrap()
    }

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
