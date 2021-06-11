#[macro_use]
extern crate serde;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate log;
#[macro_use]
extern crate anyhow;

use anyhow::Error;
use database::Database;
use std::collections::HashSet;
use std::fs::read_to_string;
use system::{Module, ScrapingService};

mod chain_api;
mod database;
mod system;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Config {
    database: DatabaseConfig,
    modules: Vec<Module>,
    accounts: Vec<Context>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct DatabaseConfig {
    uri: String,
    name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Context {
    stash: String,
    network: Network,
}

impl Context {
    pub fn as_str(&self) -> &str {
        self.stash.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum Network {
    Polkadot,
    Kusama,
}

pub async fn run() -> Result<()> {
    info!("Reading config from 'config/config.yml'");
    let content = read_to_string("config/config.yml")?;
    let config: Config = serde_yaml::from_str(&content)?;

    info!("Setting up database");
    let db = Database::new(&config.database.uri, &config.database.name).await?;

    info!("Setting up scraping service");
    let mut service = ScrapingService::new(db);

    let account_count = config.accounts.len();
    if account_count == 0 {
        return Err(anyhow!("no accounts were specified to monitor"));
    } else {
        info!("Adding {} accounts to monitor", account_count)
    }

    service.add_contexts(config.accounts).await;

    for module in &config.modules {
        service.run(module);
    }

    service.wait_blocking().await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use log::LevelFilter;
    use rand::{thread_rng, Rng};

    /// Convenience function for logging in tests.
    pub fn init() {
        let _ = env_logger::builder()
            .filter_level(LevelFilter::Debug)
            .init();
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

    impl<'a> From<&'a str> for Context {
        fn from(val: &'a str) -> Self {
            Context {
                stash: val.to_string(),
                network: Network::Polkadot,
            }
        }
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
