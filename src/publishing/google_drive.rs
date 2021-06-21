use super::Publisher;
use crate::Result;
use google_drive::GoogleDrive as RawGoogleDrive;
use std::sync::Arc;
use tokio::sync::{Mutex};
use tokio::time::{sleep, Duration};
use yup_oauth2::{read_service_account_key, ServiceAccountAuthenticator};

const PUBLISHER_REQUEST_TIMEOUT: u64 = 1;

pub struct GoogleDrive {
    drive: RawGoogleDrive,
    guard_lock: Arc<Mutex<()>>,
}

impl GoogleDrive {
    pub async fn new(path: &str) -> Result<Self> {
        let key = read_service_account_key(path).await?;
        let auth = ServiceAccountAuthenticator::builder(key).build().await?;
        let token = auth
            .token(&[
                "https://www.googleapis.com/auth/devstorage.read_write",
                "https://www.googleapis.com/auth/drive",
            ])
            .await?;

        if token.as_str().is_empty() {
            return Err(anyhow!("returned Google auth token is invalid"));
        }

        Ok(GoogleDrive {
            drive: RawGoogleDrive::new(token),
            guard_lock: Default::default(),
        })
    }
    async fn time_guard(&self) {
        let mutex = Arc::clone(&self.guard_lock);
        let guard = mutex.lock_owned().await;

        tokio::spawn(async move {
            // Capture guard, drops after sleeping period;
            let _ = guard;
            sleep(Duration::from_secs(PUBLISHER_REQUEST_TIMEOUT)).await;
        });
    }
}

#[async_trait]
impl Publisher for GoogleDrive {
    type Data = GoogleStoragePayload;
    type Info = GoogleDriveUploadInfo;

    async fn upload_data(&self, info: Self::Info, data: Self::Data) -> Result<()> {
        self.time_guard().await;

        self.drive
            .upload_to_cloud_storage(
                &info.bucket_name,
                &data.name,
                &data.mime_type,
                &data.body,
                data.is_public,
            )
            .await
            .map(|_| ())
            .map_err(|err| err.into())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GoogleStoragePayload {
    pub name: String,
    pub mime_type: String,
    pub body: Vec<u8>,
    pub is_public: bool,
}

// TODO: Rename, reference "config"
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GoogleDriveUploadInfo {
    pub bucket_name: String,
}
