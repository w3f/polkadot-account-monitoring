use crate::Result;
mod google_drive;

pub use self::google_drive::{GoogleDrive, GoogleStoragePayload, GoogleDriveUploadInfo};

#[async_trait]
pub trait Publisher {
    type Data;
    // TODO: Rename this to `Config`.
    type Info;

    async fn upload_data(&self, info: Self::Info, data: Self::Data) -> Result<()>;
}
