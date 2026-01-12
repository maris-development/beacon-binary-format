#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

use std::sync::Arc;

use bytes::Bytes;

pub mod async_reader;

pub trait RangeRead: Send + Sync + 'static {
    fn file_size(&self) -> u64;
    fn read_range(&self, offset: u64, size: u64) -> Result<Bytes, std::io::Error>;
}

#[cfg(unix)]
impl RangeRead for std::fs::File {
    fn file_size(&self) -> u64 {
        self.metadata().map(|meta| meta.len()).unwrap()
    }

    fn read_range(&self, offset: u64, size: u64) -> Result<Bytes, std::io::Error> {
        let mut buf = vec![0u8; size as usize];
        self.read_at(&mut buf, offset)?;
        Ok(buf.into())
    }
}

#[cfg(windows)]
impl RangeRead for std::fs::File {
    fn file_size(&self) -> u64 {
        self.metadata().map(|meta| meta.len()).unwrap()
    }

    fn read_range(&self, offset: u64, size: u64) -> Result<Bytes, std::io::Error> {
        let mut buffer = vec![0u8; size as usize];
        self.seek_read(&mut buffer, offset)?;
        Ok(buffer.into())
    }
}

pub(crate) struct ChunkReader {
    underlying_reader: Arc<dyn RangeRead>,
    offset: u64,
    size: u64,
}

impl ChunkReader {
    pub fn new(underlying_reader: Arc<dyn RangeRead>, offset: u64, size: u64) -> Self {
        Self {
            underlying_reader,
            offset,
            size,
        }
    }

    pub fn chunk_size(&self) -> u64 {
        self.size
    }

    pub fn read(&self, offset: u64, size: u64) -> Result<Bytes, std::io::Error> {
        if offset + size > self.size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Read range exceeds chunk size",
            ));
        }
        self.underlying_reader
            .read_range(self.offset + offset, size)
    }

    pub fn rechunk(&self, offset: u64, size: u64) -> Self {
        if offset + size > self.size {
            panic!("Rechunking exceeds chunk size");
        }
        Self {
            underlying_reader: self.underlying_reader.clone(),
            offset: self.offset + offset,
            size,
        }
    }
}

impl RangeRead for ChunkReader {
    fn file_size(&self) -> u64 {
        self.size
    }

    fn read_range(&self, offset: u64, size: u64) -> Result<Bytes, std::io::Error> {
        if offset + size > self.size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Read range exceeds chunk size",
            ));
        }
        self.underlying_reader
            .read_range(self.offset + offset, size)
    }
}
