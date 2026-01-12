#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray};
use arrow_schema::FieldRef;
use bytes::{Buf, Bytes};
use nd_arrow_array::{batch::NdRecordBatch, NdArrowArray};
use rkyv::{rancor, Deserialize};

use crate::{
    array::util::{ArchivedStore, ZeroAccessor},
    entry::{Entry, EntryKey},
    error::BBFError,
    footer::Footer,
    index::{
        pruning::{CombinedColumnStatistics, PruningIndexReader},
        ArchivedIndex, Index,
    },
    reader::async_reader::{
        array::AsyncArrayReader, cache::SharedCache, stream::AsyncStreamProducer,
    },
};

pub mod array;
pub mod cache;
pub mod stream;

#[async_trait::async_trait]
pub trait AsyncRangeRead: Send + Sync + 'static {
    async fn size(&self) -> Result<u64, std::io::Error>;
    async fn read_range(&self, offset: u64, size: u64) -> Result<Bytes, std::io::Error>;
}

#[cfg(unix)]
#[async_trait::async_trait]
impl AsyncRangeRead for Arc<std::fs::File> {
    async fn size(&self) -> Result<u64, std::io::Error> {
        let self = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let metadata = self.metadata()?;
            Ok(metadata.len())
        })
        .await?
    }

    async fn read_range(&self, offset: u64, size: u64) -> Result<Bytes, std::io::Error> {
        let self = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let mut buf = vec![0; size as usize];
            self.read_exact_at(&mut buf, offset)?;
            Ok(Bytes::from(buf))
        })
        .await?
    }
}

#[cfg(windows)]
#[async_trait::async_trait]
impl AsyncRangeRead for Arc<std::fs::File> {
    async fn size(&self) -> Result<u64, std::io::Error> {
        let self = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let metadata = self.metadata()?;
            Ok(metadata.len())
        })
        .await?
    }

    async fn read_range(&self, offset: u64, size: u64) -> Result<Bytes, std::io::Error> {
        let self = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let mut buf = vec![0; size as usize];
            self.seek_read(&mut buf, offset)?;
            Ok(Bytes::from(buf))
        })
        .await?
    }
}

#[derive(Clone)]
pub struct AsyncChunkReader {
    reader: Arc<dyn AsyncRangeRead>,
    offset: u64,
    chunk_size: u64,
}

impl AsyncChunkReader {
    pub fn new(reader: Arc<dyn AsyncRangeRead>, offset: u64, chunk_size: u64) -> Self {
        Self {
            reader,
            offset,
            chunk_size,
        }
    }

    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    pub async fn read(&self, offset: u64, size: u64) -> Result<Bytes, std::io::Error> {
        let absolute_offset = self.offset + offset;
        self.reader.read_range(absolute_offset, size).await
    }
}

pub struct AsyncBBFReader {
    footer: ZeroAccessor<Footer>,
    pub reader: Arc<dyn AsyncRangeRead>,
    shared_cache: SharedCache,
}

impl AsyncBBFReader {
    pub async fn new(
        reader: Arc<dyn AsyncRangeRead>,
        io_cache_size_mb: usize,
    ) -> Result<Self, BBFError> {
        let footer = Self::read_footer(reader.clone()).await?;

        let shared_cache = SharedCache::new(io_cache_size_mb * 1024 * 1024);

        Ok(Self {
            footer,
            reader,
            shared_cache,
        })
    }

    async fn validate_footer_magic(reader: Arc<dyn AsyncRangeRead>) -> Result<(), BBFError> {
        let magic_bytes = crate::consts::MAGIC_BYTES;
        let magic_size = magic_bytes.len();
        let magic_start = reader.size().await? - magic_size as u64;

        let mut buffer = vec![0u8; magic_size];
        reader
            .read_range(magic_start, magic_size as u64)
            .await
            .map_err(|e| BBFError::Reading(e.into()))?
            .copy_to_slice(&mut buffer);

        if buffer != magic_bytes {
            return Err(BBFError::Reading(
                format!(
                    "Invalid footer magic bytes: expected {:?}, got {:?}",
                    magic_bytes, buffer
                )
                .into(),
            ));
        }

        Ok(())
    }

    pub fn version(&self) -> &str {
        self.footer.as_ref().version.as_str()
    }

    pub fn footer(&self) -> &ZeroAccessor<Footer> {
        &self.footer
    }

    pub fn physical_entries(&self) -> Vec<EntryKey> {
        rkyv::deserialize::<Vec<EntryKey>, rancor::Error>(&self.footer.as_ref().entries).unwrap()
    }

    pub fn logical_entries(&self) -> Vec<EntryKey> {
        let physical_entries = self.physical_entries();
        let deletes = self.entries_logical_deletes();

        let mut logical_entries = Vec::new();
        for (i, entry) in physical_entries.iter().enumerate() {
            if !deletes[i] {
                logical_entries.push(entry.clone());
            }
        }
        logical_entries
    }

    pub fn entries_logical_deletes(&self) -> Vec<bool> {
        rkyv::deserialize::<Vec<bool>, rancor::Error>(&self.footer.as_ref().entries_deleted)
            .unwrap()
    }

    pub async fn pruning_index(&self) -> Option<AsyncPruningIndexReader> {
        let indexes = &self.footer.as_ref().indexes;
        if let Some(resolver) = indexes.get(crate::consts::PRUNING_INDEX_NAME) {
            let chunk_reader =
                AsyncChunkReader::new(self.reader.clone(), 0, self.reader.size().await.unwrap());
            let pruning_index = resolver.resolve(&chunk_reader).await.ok()?;
            Some(AsyncPruningIndexReader::new(
                self.reader.clone(),
                pruning_index,
            ))
        } else {
            None
        }
    }

    pub(crate) async fn read_footer(
        reader: Arc<dyn AsyncRangeRead>,
    ) -> Result<ZeroAccessor<Footer>, BBFError> {
        Self::validate_footer_magic(reader.clone()).await?;
        let footer_len_start = reader.size().await? - crate::consts::MAGIC_BYTES.len() as u64 - 8;
        let footer_len_bytes = reader
            .read_range(footer_len_start, 8)
            .await
            .map_err(|e| BBFError::Reading(e.into()))?;

        let footer_len = footer_len_bytes.as_ref().get_u64_le() as usize;

        let footer_start = footer_len_start - footer_len as u64;
        let footer_bytes = reader
            .read_range(footer_start, footer_len as u64)
            .await
            .map_err(|e| BBFError::Reading(e.into()))?;

        let store: &ArchivedStore<Footer> = unsafe { rkyv::access_unchecked(&footer_bytes) };
        let zero_accessor = store.read();

        Ok(zero_accessor)
    }

    pub fn arrow_schema(&self) -> arrow::datatypes::Schema {
        let mut fields = vec![];
        let schema_map = &self.footer.as_ref().schema;
        for (name, bbf_type) in schema_map.iter() {
            fields.push(arrow::datatypes::Field::new(
                name.to_string(),
                bbf_type.to_arrow(),
                true,
            ));
        }

        arrow::datatypes::Schema::new(fields)
    }

    pub async fn read(
        &self,
        projection: Option<Vec<usize>>,
        physical_entry_selection: Option<BooleanArray>,
    ) -> Result<AsyncStreamProducer<nd_arrow_array::batch::NdRecordBatch>, BBFError> {
        let projection = projection
            .unwrap_or_else(|| (0..self.footer.as_ref().schema.len()).collect::<Vec<_>>());

        let shared_cache = self.shared_cache.clone();
        let mut fields = vec![];
        let mut array_readers = vec![];
        for index in projection {
            if let Some((name, field)) = self.footer.as_ref().schema.get_index(index) {
                let resolver = self.footer.as_ref().array_columns.get(name).unwrap();
                let chunk_reader = AsyncChunkReader::new(
                    self.reader.clone(),
                    0,
                    self.reader.size().await.unwrap(),
                );
                let array_column = resolver.resolve(&chunk_reader).await.unwrap();
                let array_reader = AsyncArrayReader::new(
                    name.to_string(),
                    array_column,
                    shared_cache.clone(),
                    chunk_reader,
                );

                array_readers.push(array_reader);
                fields.push(Arc::new(arrow::datatypes::Field::new(
                    name.to_string(),
                    field.to_arrow(),
                    true,
                )));
            } else {
                return Err(BBFError::Reading(
                    format!(
                        "Projection index {} out of bounds for schema with {} fields",
                        index,
                        self.footer.as_ref().schema.len(),
                    )
                    .into(),
                ));
            }
        }

        let fields_ref = Arc::new(fields);
        let array_readers = Arc::new(array_readers);

        // Calculate the readable entries based on physical entries - deleted entries
        let physical_entries = self.physical_entries();
        let mut logical_deletes = self.entries_logical_deletes();

        // If an entry selection is provided, filter the readable entries accordingly
        if let Some(selection) = &physical_entry_selection {
            // Check that the length of the selection matches the number of readable entries
            if selection.len() != physical_entries.len() {
                return Err(BBFError::Reading(
                    format!(
                        "Entry selection length {} does not match number of readable entries {}",
                        selection.len(),
                        physical_entries.len(),
                    )
                    .into(),
                ));
            }

            assert_eq!(
                selection.len(),
                logical_deletes.len(),
                "Selection length must match logical deletes length"
            );
            for i in 0..selection.len() {
                if !selection.value(i) {
                    logical_deletes[i] = true;
                }
            }
        }

        // Apply the logical deletes to get the final readable entry indices
        let readable_entry_indices: Vec<usize> = logical_deletes
            .iter()
            .enumerate()
            .filter_map(|(i, deleted)| if !deleted { Some(i) } else { None })
            .collect();

        let mut tasks = vec![];
        for index in readable_entry_indices {
            let fields_ref = fields_ref.clone();
            let cloned_readers = array_readers.clone();
            let task = async move {
                let mut local_reads = vec![];
                for (_i, reader) in cloned_readers.iter().enumerate() {
                    let read = reader.read_array(index);
                    local_reads.push(read);
                }

                //await the reads
                let arrays = futures::future::join_all(local_reads).await;
                let mut fields = vec![];
                let mut nd_arrays: Vec<NdArrowArray> = vec![];
                for (i, array) in arrays.into_iter().enumerate() {
                    let array = array.unwrap();
                    fields.push(fields_ref[i].as_ref().clone());
                    if let Some(mut nd_array) = array {
                        // If data type differs from the fields ref, then cast it safely
                        if nd_array.data_type() != fields_ref[i].data_type() {
                            nd_array = NdArrowArray::new(
                                arrow::compute::cast(
                                    nd_array.as_arrow_array(),
                                    fields_ref[i].data_type(),
                                )
                                .unwrap(),
                                nd_array.dimensions().clone(),
                            )
                            .unwrap()
                        }
                        nd_arrays.push(nd_array);
                    } else {
                        nd_arrays.push(NdArrowArray::new_null_scalar(Some(
                            fields_ref[i].data_type().clone(),
                        )));
                    }
                }

                NdRecordBatch::new(fields, nd_arrays).unwrap()
            };

            tasks.push(task);
        }

        let stream = AsyncStreamProducer::new(tasks, 256);

        Ok(stream)
    }
}

pub struct AsyncPruningIndexReader {
    inner_reader: Arc<dyn AsyncRangeRead>,
    index: ZeroAccessor<Index>,
}

impl AsyncPruningIndexReader {
    pub fn new(inner_reader: Arc<dyn AsyncRangeRead>, index: ZeroAccessor<Index>) -> Self {
        Self {
            inner_reader,
            index,
        }
    }

    pub fn num_containers(&self) -> usize {
        match self.index.as_ref() {
            ArchivedIndex::PruningIndex(archived_index) => {
                archived_index.num_containers.to_native() as usize
            }
        }
    }

    pub async fn column(
        &self,
        column: &str,
    ) -> Result<Option<ZeroAccessor<CombinedColumnStatistics>>, BBFError> {
        match self.index.as_ref() {
            ArchivedIndex::PruningIndex(archived_index) => {
                let index_reader =
                    PruningIndexReader::new(archived_index, self.inner_reader.clone())
                        .await
                        .map_err(|e| {
                            BBFError::Reading(
                                format!("Failed to create PruningIndexReader: {}", e).into(),
                            )
                        })?;

                let statistics = index_reader
                    .fetch_column_statistics(column)
                    .await
                    .map_err(|e| {
                        BBFError::Reading(
                            format!("Failed to fetch column statistics: {}", e).into(),
                        )
                    });

                statistics
            }
            _ => return Err(BBFError::Reading("Index is not a PruningIndex".into())),
        }
    }
}
