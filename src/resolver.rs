use std::{
    io::{Seek, Write},
    marker::PhantomData,
};

use rkyv::{rancor, Archive};

use crate::{
    array::util::{ArchivedStore, Store, ZeroAccessor},
    reader::async_reader::AsyncChunkReader,
};

#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct Resolver<T: Archive + Clone> {
    marker: PhantomData<Store<T>>,
    pub offset: usize,
    pub size: usize,
}

impl<T: Archive + Clone> Resolver<T> {
    pub fn create_resolver<W: Write + Seek>(writer: &mut W, value: Store<T>) -> Self {
        let offset = writer.seek(std::io::SeekFrom::Current(0)).unwrap();
        let bytes = rkyv::to_bytes::<rancor::Error>(&value).unwrap();

        writer.write_all(&bytes).unwrap();

        Self {
            marker: PhantomData,
            offset: offset as usize,
            size: bytes.len(),
        }
    }

    pub async fn resolve(&self, reader: &AsyncChunkReader) -> Result<ZeroAccessor<T>, String> {
        let bytes = reader
            .read(self.offset as u64, self.size as u64)
            .await
            .map_err(|e| {
                format!(
                    "Failed to read bytes from offset {} with size {}: {}",
                    self.offset, self.size, e
                )
            })?;

        if bytes.is_empty() {
            return Err("No data read from the specified offset and size".to_string());
        }

        let archived_store: &ArchivedStore<T> =
            unsafe { rkyv::access_unchecked::<ArchivedStore<T>>(&bytes).clone() };

        let zero_accessor = archived_store.read();
        Ok(zero_accessor)
    }
}

impl<T: Archive + Clone> ArchivedResolver<T> {
    pub async fn resolve(&self, reader: &AsyncChunkReader) -> Result<ZeroAccessor<T>, String> {
        let bytes = reader
            .read(self.offset.to_native() as u64, self.size.to_native() as u64)
            .await
            .map_err(|e| {
                format!(
                    "Failed to read bytes from offset {} with size {}: {}",
                    self.offset, self.size, e
                )
            })?;

        if bytes.is_empty() {
            return Err("No data read from the specified offset and size".to_string());
        }

        let archived_store: &ArchivedStore<T> =
            unsafe { rkyv::access_unchecked::<ArchivedStore<T>>(&bytes).clone() };

        let zero_accessor = archived_store.read();
        Ok(zero_accessor)
    }
}
