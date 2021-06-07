use super::Result;
use mongodb::Client;

pub struct Database {
    db: Client,
}

impl Database {
    pub async fn new(uri: &str) -> Result<Self> {
        Ok(
            Database {
                db: Client::with_uri_str(uri).await?,
            }
        )
    }
}
