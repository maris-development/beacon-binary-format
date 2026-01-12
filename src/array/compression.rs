#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(derive(Debug))]
pub enum Compression {
    ZStd { level: i32, zdict: Option<Vec<u8>> },
    Lz4,
    Lz4hc,
}

impl Compression {
    pub fn zstd(level: i32) -> Self {
        Compression::ZStd { level, zdict: None }
    }

    pub fn lz4() -> Self {
        Compression::Lz4
    }

    pub fn lz4hc() -> Self {
        Compression::Lz4hc
    }

    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        match self {
            Compression::ZStd { level, zdict: _ } => {
                // Implement ZStd compression logic here
                let compressed = zstd::bulk::compress(data, *level).unwrap();
                Ok(compressed)
            }
            Compression::Lz4 => {
                let mut buffer = vec![];
                let _ =
                    lzzzz::lz4::compress_to_vec(data, &mut buffer, lzzzz::lz4::ACC_LEVEL_DEFAULT);
                Ok(buffer)
            }
            Compression::Lz4hc => {
                let mut buffer = vec![];
                let _ = lzzzz::lz4_hc::compress_to_vec(
                    data,
                    &mut buffer,
                    lzzzz::lz4_hc::CLEVEL_DEFAULT,
                );
                Ok(buffer)
            }
        }
    }
}

impl ArchivedCompression {
    pub fn decompress(&self, data: &[u8], uncompressed_size: usize) -> Result<Vec<u8>, String> {
        match self {
            ArchivedCompression::ZStd { level: _, zdict: _ } => {
                // Implement ZStd decompression logic here
                let uncompressed = zstd::bulk::decompress(data, uncompressed_size).unwrap();
                Ok(uncompressed)
            }
            ArchivedCompression::Lz4 => {
                let mut buffer = vec![0; uncompressed_size];
                lzzzz::lz4::decompress(data, &mut buffer).unwrap();
                Ok(buffer)
            }
            ArchivedCompression::Lz4hc => {
                let mut buffer = vec![0; uncompressed_size];
                lzzzz::lz4::decompress(data, &mut buffer).unwrap();
                Ok(buffer)
            }
        }
    }
}
