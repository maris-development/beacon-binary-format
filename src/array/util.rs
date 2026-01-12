use std::marker::PhantomData;

use arrow::{
    array::{Array, ArrayRef, RunArray, UInt32Array},
    datatypes::RunEndIndexType,
};
use bytes::Bytes;
use rkyv::{
    rancor::{self},
    ser::allocator::ArenaHandle,
    util::AlignedVec,
};

use crate::array::compression::Compression;

#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum Store<T: Clone + rkyv::Archive> {
    Compressed {
        phantom: PhantomData<T>,
        compression: Compression,
        uncompressed_size: usize,
        bytes: Vec<u8>,
    },
    Raw {
        buffer: Vec<u8>,
    },
}

impl<T: Clone + rkyv::Archive> Store<T>
where
    T: for<'a> rkyv::Serialize<
        rkyv::rancor::Strategy<
            rkyv::ser::Serializer<AlignedVec, ArenaHandle<'a>, rkyv::ser::sharing::Share>,
            rkyv::rancor::Error,
        >,
    >,
{
    pub fn new(value: T, compression: Option<Compression>) -> Self {
        let bytes = rkyv::to_bytes::<rancor::Error>(&value).expect("Failed to serialize value");
        if let Some(compression) = compression {
            let uncompressed_size = bytes.len();
            let compressed_bytes = compression.compress(&bytes).unwrap();
            Store::Compressed {
                phantom: PhantomData,
                compression,
                uncompressed_size,
                bytes: compressed_bytes,
            }
        } else {
            Store::Raw {
                buffer: bytes.to_vec(),
            }
        }
    }
}

impl<T: Clone + rkyv::Archive> ArchivedStore<T> {
    pub fn read(&self) -> ZeroAccessor<T> {
        match self {
            ArchivedStore::Compressed {
                phantom: _,
                compression,
                uncompressed_size,
                bytes,
            } => {
                let decompressed_bytes = compression
                    .decompress(bytes, uncompressed_size.to_native() as usize)
                    .map_err(|e| format!("Failed to decompress bytes: {}", e))
                    .unwrap();
                let buffer = Bytes::copy_from_slice(&decompressed_bytes);
                ZeroAccessor {
                    buffer,
                    _marker: PhantomData,
                }
            }
            ArchivedStore::Raw { buffer } => {
                let bytes = buffer.as_ref();
                ZeroAccessor {
                    buffer: Bytes::copy_from_slice(bytes),
                    _marker: PhantomData,
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ZeroAccessor<T: rkyv::Archive> {
    buffer: Bytes,
    _marker: PhantomData<T>,
}

impl<T: rkyv::Archive> ZeroAccessor<T> {
    pub fn underlying_bytes(&self) -> &[u8] {
        &self.buffer
    }

    pub fn as_ref(&self) -> &T::Archived {
        unsafe { rkyv::access_unchecked(&self.buffer) }
    }
}

pub fn flatten_run_array<R: RunEndIndexType>(run: &RunArray<R>) -> ArrayRef {
    // 1) logical indices [0, 1, 2, ..., len-1]
    let len = run.len();
    let logical: Vec<u32> = (0..(len as u32)).collect();

    // 2) map logical -> physical indices inside the values array
    let phys = run.get_physical_indices(&logical).expect("indices mapping");

    // 3) gather those physical indices from the values() array
    let idx = UInt32Array::from_iter_values(phys.into_iter().map(|i| i as u32));
    arrow::compute::take(run.values().as_ref(), &idx, None).expect("gather")
}
