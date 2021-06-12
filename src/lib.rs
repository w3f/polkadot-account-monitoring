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
use log::LevelFilter;
use std::fs::read_to_string;
use system::{Module, ScrapingService};

mod chain_api;
mod database;
mod system;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Config {
    database: DatabaseConfig,
    active_modules: Vec<Module>,
    log_level: LevelFilter,
    accounts_file: String,
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
    description: String,
}

impl Context {
    pub fn as_str(&self) -> &str {
        self.stash.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Network {
    Polkadot,
    Kusama,
}

pub async fn run() -> Result<()> {
    println!("Reading config from 'config/config.yml'");
    let content = read_to_string("config/config.yml")?;
    let config: Config = serde_yaml::from_str(&content)?;

    println!("Starting logger");
    env_logger::builder().filter_level(config.log_level).init();

    info!("Reading accounts file");
    let content = read_to_string(config.accounts_file)?;
    let accounts: Vec<Context> = serde_yaml::from_str(&content)?;

    info!("Setting up database");
    let db = Database::new(&config.database.uri, &config.database.name).await?;

    info!("Setting up scraping service");
    let mut service = ScrapingService::new(db);

    let account_count = accounts.len();
    if account_count == 0 {
        return Err(anyhow!("no accounts were specified to monitor"));
    } else {
        info!("Adding {} accounts to monitor", account_count)
    }

    service.add_contexts(accounts).await;

    for module in &config.active_modules {
        service.run(module).await?;
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
                description: "".to_string(),
            }
        }
    }

    impl Context {
        pub fn alice() -> Self {
            Context {
                stash: "1a2YiGNu1UUhJtihq8961c7FZtWGQuWDVMWTNBKJdmpGhZP".to_string(),
                network: Network::Polkadot,
                description: "".to_string(),
            }
        }
        pub fn bob() -> Self {
            Context {
                stash: "1b3NhsSEqWSQwS6nPGKgCrSjv9Kp13CnhraLV5Coyd8ooXB".to_string(),
                network: Network::Polkadot,
                description: "".to_string(),
            }
        }
        pub fn eve() -> Self {
            Context {
                stash: "1cNyFSmLW4ofr7xh38za6JxLFxcu548LPcfc1E6L9r57SE3".to_string(),
                network: Network::Polkadot,
                description: "".to_string(),
            }
        }
    }
}

#[test]
#[ignore]
fn parse_file() {
    let descs = read_to_string("descs.txt").unwrap();
    let addrs = read_to_string("addrs.txt").unwrap();

    let descs = descs.lines().into_iter();
    let addrs = addrs.lines().into_iter();

    for (desc, addr) in descs.zip(addrs) {
        println!(
            "{}",
            serde_yaml::to_string(&vec![Context {
                stash: addr.into(),
                network: Network::Kusama,
                description: format!("{}", desc),
            }])
            .unwrap()
        )
    }
}
