use std::{
    collections::HashSet,
    io::{Seek, Write},
    path::Path,
    sync::Arc,
};

use arrow::array::{StringArray, UInt64Array};
use byteorder::{WriteBytesExt, LE};

use crate::{
    array::{
        column::ArrayColumnBuilder, compression::Compression, datatype::DataType,
        dimensions::Dimensions, statistics::ColumnStatistics, util::Store,
    },
    consts::{self, PRUNING_INDEX_NAME},
    entry::{ArrayCollection, Entry, EntryKey},
    error::BBFError,
    footer::Footer,
    index::pruning::PruningIndexBuilder,
    resolver::Resolver,
    util::super_type_arrow,
};

pub mod async_writer;

pub struct BBFWriter {
    pub file: std::fs::File,
    pub version: String,
    pub schema: indexmap::IndexMap<String, DataType>,
    pub array_columns: indexmap::IndexMap<String, ArrayColumnBuilder>,
    pub pruning_index: Option<PruningIndexBuilder>,

    pub entries_set: indexmap::IndexSet<String>,
    pub entries: Vec<EntryKey>,

    pub compression: Option<Compression>,
    pub group_size: usize,
}

impl BBFWriter {
    pub fn new<P: AsRef<Path>>(
        path: P,
        group_size: usize,
        compression: Option<Compression>,
        pruning_index: bool,
    ) -> Result<Self, BBFError> {
        let mut file = std::fs::File::create(path)
            .map_err(|e| BBFError::Writing(format!("Failed to create file: {}", e).into()))?;
        // Write the magic bytes to the file
        file.write_all(consts::MAGIC_BYTES)
            .map_err(|e| BBFError::Writing(format!("Failed to write magic bytes: {}", e).into()))?;

        Ok(Self {
            file,
            version: env!("CARGO_PKG_VERSION").to_string(),
            schema: indexmap::IndexMap::new(),
            array_columns: indexmap::IndexMap::new(),
            entries_set: indexmap::IndexSet::new(),
            entries: Vec::new(),
            pruning_index: if pruning_index {
                Some(PruningIndexBuilder::new())
            } else {
                None
            },
            compression,
            group_size,
        })
    }

    pub fn append<S: Into<String>>(&mut self, entry: Entry, entry_name: S) {
        let entry_name = entry_name.into();
        if self.entries_set.contains(&entry_name) {
            return;
        }
        self.entries_set.insert(entry_name.clone());

        match entry {
            Entry::Default(array) => {
                let entry_key = EntryKey::new(entry_name.clone(), None);
                self.append_default(array, entry_key);
            }
            Entry::Chunked { chunked } => {
                self.append_chunked(&mut chunked.into_iter(), entry_name);
            }
        }
    }

    fn append_default(&mut self, array: ArrayCollection, entry_key: EntryKey) {
        let mut pruning_statistics = indexmap::IndexMap::new();
        let mut missing_file_columns = self.schema.keys().cloned().collect::<HashSet<_>>();

        self.entries.push(entry_key.clone());

        // Also append the entry as a column
        if self.schema.get(Entry::FIELD_NAME).is_none() {
            self.schema.insert(
                Entry::FIELD_NAME.to_string(),
                DataType::from_arrow(arrow::datatypes::DataType::Utf8),
            );
        }

        let entry_name_array_column = self
            .array_columns
            .entry(Entry::FIELD_NAME.to_string())
            .or_insert_with(|| ArrayColumnBuilder::new(self.group_size, self.compression.clone()));
        entry_name_array_column.append(
            &mut self.file,
            self.entries.len() - 1,
            std::sync::Arc::new(arrow::array::StringArray::from_iter_values(vec![
                &entry_key.name,
            ])),
            Dimensions::Scalar,
        );

        if self.pruning_index.is_some() {
            let statistics = ColumnStatistics::new(
                0,
                1,
                Some(Arc::new(StringArray::from(vec![entry_key.name.clone()]))),
                Some(Arc::new(StringArray::from(vec![entry_key.name.clone()]))),
            );

            pruning_statistics.insert(Entry::FIELD_NAME.to_string(), statistics);
        }

        missing_file_columns.remove(Entry::FIELD_NAME);

        // Also append the chunk index as a column if it is chunked
        if let Some(chunk_index) = entry_key.chunk_index {
            if self.schema.get(Entry::FIELD_CHUNK_INDEX).is_none() {
                self.schema.insert(
                    Entry::FIELD_CHUNK_INDEX.to_string(),
                    DataType::from_arrow(arrow::datatypes::DataType::UInt64),
                );
            }
            let entry_chunk_index_array_column = self
                .array_columns
                .entry(Entry::FIELD_CHUNK_INDEX.to_string())
                .or_insert_with(|| {
                    ArrayColumnBuilder::new(self.group_size, self.compression.clone())
                });
            entry_chunk_index_array_column.append(
                &mut self.file,
                self.entries.len() - 1,
                std::sync::Arc::new(arrow::array::UInt64Array::from_iter_values(vec![
                    chunk_index as u64,
                ])),
                Dimensions::Scalar,
            );

            missing_file_columns.remove(Entry::FIELD_CHUNK_INDEX);

            if self.pruning_index.is_some() {
                let statistics = ColumnStatistics::new(
                    0,
                    1,
                    Some(Arc::new(UInt64Array::from(vec![chunk_index as u64]))),
                    Some(Arc::new(UInt64Array::from(vec![chunk_index as u64]))),
                );

                pruning_statistics.insert(Entry::FIELD_CHUNK_INDEX.to_string(), statistics);
            }
        }

        for column in array.columns {
            missing_file_columns.remove(&column.name);
            // Ensure the column exists in the schema
            if !self.schema.contains_key(&column.name) {
                self.schema.insert(column.name.clone(), column.data_type);
            } else {
                // If the data type differs then super type the data type.
                if self.schema[&column.name] != column.data_type {
                    let base_type = self.schema[&column.name].to_arrow();
                    let new_type = column.data_type.to_arrow();
                    let super_type = super_type_arrow(&base_type, &new_type).unwrap();
                    self.schema
                        .insert(column.name.clone(), DataType::from_arrow(super_type));
                }
            }

            // Ensure the column exists in the map
            let array_column = self
                .array_columns
                .entry(column.name.clone())
                .or_insert_with(|| {
                    ArrayColumnBuilder::new(self.group_size, self.compression.clone())
                });

            // If pruning index is enabled, collect statistics for pruning
            if self.pruning_index.is_some() {
                // Collect statistics for pruning
                let statistics = ColumnStatistics::from_column(&column);
                pruning_statistics.insert(column.name.clone(), statistics);
            }

            array_column.append(
                &mut self.file,
                self.entries.len() - 1,
                column.array,
                column.dimensions,
            );
        }

        for missing_column in missing_file_columns {
            if let Some(array_column) = self.array_columns.get_mut(&missing_column) {
                array_column.append_null(&mut self.file, self.entries.len() - 1);
            }
        }

        // If pruning index is enabled, append the statistics
        if let Some(pruning_index) = &mut self.pruning_index {
            pruning_index.append_statistics(pruning_statistics);
        }
    }

    fn append_chunked(
        &mut self,
        chunked: &mut dyn Iterator<Item = ArrayCollection>,
        entry_name: String,
    ) {
        for (idx, array) in chunked.enumerate() {
            self.append_default(array, EntryKey::new(entry_name.clone(), Some(idx)));
        }
    }

    pub fn finish(mut self) -> Result<(), BBFError> {
        let mut array_groups = indexmap::IndexMap::new();
        for (k, v) in self.array_columns {
            let array_column = v.finish(&mut self.file);
            let stored = Store::new(array_column, self.compression.clone());
            let resolver = Resolver::create_resolver(&mut self.file, stored);
            array_groups.insert(k, resolver);
        }

        let mut indexes = indexmap::IndexMap::new();

        if let Some(pruning_index) = self.pruning_index {
            let pruning_index = crate::index::Index::PruningIndex(
                pruning_index.build(&mut self.file, self.compression.clone()),
            );
            let pruning_resolver = Resolver::create_resolver(
                &mut self.file,
                Store::new(pruning_index, self.compression.clone()),
            );
            indexes.insert(PRUNING_INDEX_NAME.to_string(), pruning_resolver);
        }

        let footer = Footer {
            version: self.version.clone(),
            schema: self.schema,
            indexes,
            array_columns: array_groups,
            entries_deleted: vec![false; self.entries.len()],
            entries: self.entries,
        };

        let footer_store = Store::new(footer, self.compression.clone());

        // Serialize the footer and write it to the file
        let footer_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&footer_store)
            .map_err(|e| BBFError::Writing(format!("Failed to serialize footer: {}", e).into()))?;
        let footer_len = footer_bytes.len() as u64;

        self.file.seek(std::io::SeekFrom::End(0))?;
        self.file.write_all(&footer_bytes)?;
        self.file.write_u64::<LE>(footer_len)?;

        // Write the magic bytes
        self.file.write_all(consts::MAGIC_BYTES)?;

        Ok(())
    }
}
