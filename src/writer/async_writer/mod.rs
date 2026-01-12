use bytes::Bytes;

use crate::reader::async_reader::AsyncRangeRead;

#[async_trait::async_trait]
pub trait AsyncStoreWrite: AsyncRangeRead + Send + Sync + 'static {
    async fn write(&self, data: Bytes) -> Result<(), std::io::Error>;
}
