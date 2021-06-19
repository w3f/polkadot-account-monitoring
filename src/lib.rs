#[macro_use]
extern crate serde;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate log;
#[macro_use]
extern crate anyhow;

use self::core::{
    GoogleDrive, GoogleDriveUploadInfo, ReportGenerator, ReportModule, ScrapingModule,
    ScrapingService,
};
use anyhow::Error;
use database::Database;
use log::LevelFilter;
use std::fmt;
use std::ops::Sub;
use std::sync::Arc;
use std::{borrow::Cow, fs::read_to_string};
use tokio::time::{sleep, Duration};

mod chain_api;
mod core;
mod database;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Default, Copy, Serialize, Deserialize)]
pub struct BlockNumber(u64);

impl From<u64> for BlockNumber {
    fn from(val: u64) -> Self {
        BlockNumber(val)
    }
}

impl fmt::Display for BlockNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Default, Copy, PartialOrd, Serialize, Deserialize)]
pub struct Timestamp(u64);

impl Timestamp {
    pub fn now() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};

        let start = SystemTime::now();
        let time = start
            .duration_since(UNIX_EPOCH)
            .expect("Failed to calculate UNIX time")
            .as_secs();

        Timestamp(time)
    }
}

impl Sub for Timestamp {
    type Output = Self;

    fn sub(self, other: Self) -> Self::Output {
        Timestamp(self.0 - other.0)
    }
}

impl From<u64> for Timestamp {
    fn from(val: u64) -> Self {
        Timestamp(val)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Config {
    database: DatabaseConfig,
    collection: Option<CollectionConfig>,
    report: Option<ReportConfig>,
    log_level: LevelFilter,
    accounts_file: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct CollectionConfig {
    modules: Vec<ScrapingModule>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ReportConfig {
    modules: Vec<ReportModule>,
    publisher: PublisherConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "config")]
enum PublisherConfig {
    GoogleDrive(GoogleDriveConfig),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct GoogleDriveConfig {
    bucket_name: String,
    gcp_secret_path: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct DatabaseConfig {
    uri: String,
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Context {
    pub stash: String,
    pub network: Network,
    pub description: String,
}

impl Context {
    pub fn as_str(&self) -> &str {
        self.stash.as_str()
    }
    pub fn id<'a>(&'a self) -> ContextId<'a> {
        ContextId {
            stash: Cow::Borrowed(&self.stash),
            network: self.network,
        }
    }
    pub fn network(&self) -> Network {
        self.network
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextId<'a> {
    pub stash: Cow<'a, String>,
    pub network: Network,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Network {
    Polkadot,
    Kusama,
}

impl Network {
    pub fn as_str(&self) -> &str {
        match self {
            Network::Polkadot => "polkadot",
            Network::Kusama => "kusama",
        }
    }
}

pub async fn run() -> Result<()> {
    println!("Reading config from 'config/config.yml'");
    let content = read_to_string("config/config.yml")?;
    let config: Config = serde_yaml::from_str(&content)?;

    println!("Starting logger");
    env_logger::builder()
        .filter_module("system", config.log_level)
        .init();

    info!("Reading accounts file");
    let content = read_to_string(config.accounts_file)?;
    let accounts: Vec<Context> = serde_yaml::from_str(&content)?;

    info!("Setting up database");
    let db = Database::new(&config.database.uri, &config.database.name).await?;
    let reader = db.reader();

    let account_count = accounts.len();
    if account_count == 0 {
        return Err(anyhow!("no accounts were specified to monitor"));
    } else {
        info!("Adding {} accounts to monitor", account_count)
    }

    if let Some(coll_config) = config.collection {
        info!("Setting up scraping service");
        let mut service = ScrapingService::new(db);
        service.add_contexts(accounts.clone()).await;

        for module in &coll_config.modules {
            service.run(module).await?;
        }
    } else {
        info!("No scraping modules are enabled");
    }

    info!("Setting up scraping service");
    if let Some(report_config) = config.report {
        info!("Setting up report generation service");
        let mut service = ReportGenerator::new(reader);
        service.add_contexts(accounts).await;

        let (publisher, publisher_config) = match report_config.publisher {
            PublisherConfig::GoogleDrive(config) => {
                let drive_config = GoogleDriveUploadInfo {
                    bucket_name: config.bucket_name,
                };

                (
                    Arc::new(GoogleDrive::new(&config.gcp_secret_path).await?),
                    drive_config,
                )
            }
        };

        for module in report_config.modules {
            service
                .run(module, Arc::clone(&publisher), publisher_config.clone())
                .await;
        }
    } else {
        info!("No report generation modules are enabled");
    }

    wait_blocking().await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::{Database, DatabaseReader};
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

    // TODO: Rename this
    pub async fn generator() -> DatabaseReader {
        let random: u32 = thread_rng().gen_range(u32::MIN..u32::MAX);
        DatabaseReader::new(
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

async fn wait_blocking() {
    loop {
        sleep(Duration::from_secs(u64::MAX)).await;
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
