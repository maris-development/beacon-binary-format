use std::{
    collections::HashMap,
    io::{Seek, Write},
    sync::Arc,
};

use arrow::{
    array::{
        new_null_array, Array, ArrayBuilder, ArrayRef, AsArray, BinaryRunBuilder, BooleanBuilder,
        NullBuilder, PrimitiveRunBuilder, RunArray, StringRunBuilder, UInt32Array,
    },
    datatypes::{
        Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, RunEndIndexType,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
};

use crate::{
    array::{
        arrow::to_arrow_array,
        compression::Compression,
        datatype::DataType,
        statistics::ColumnStatistics,
        util::{Store, ZeroAccessor},
        ArrayBuffer,
    },
    reader::async_reader::AsyncRangeRead,
};
use crate::{error::BBFError, reader::async_reader::AsyncChunkReader};
use crate::{resolver::Resolver, util::super_type_arrow};

#[derive(Clone, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct PruningIndex {
    pub num_containers: usize,
    pub columns: indexmap::IndexMap<String, Resolver<CombinedColumnStatistics>>,
}

pub struct PruningIndexReader<'a> {
    index: &'a ArchivedPruningIndex,
    reader: AsyncChunkReader,
    cache_statistics: parking_lot::Mutex<HashMap<String, ZeroAccessor<CombinedColumnStatistics>>>,
}

impl<'a> PruningIndexReader<'a> {
    pub async fn new(
        index: &'a ArchivedPruningIndex,
        reader: Arc<dyn AsyncRangeRead>,
    ) -> Result<Self, String> {
        let chunk_reader = AsyncChunkReader::new(reader.clone(), 0, reader.size().await.unwrap());
        Ok(Self {
            index,
            reader: chunk_reader,
            cache_statistics: parking_lot::Mutex::new(HashMap::new()),
        })
    }

    pub async fn fetch_column_statistics(
        &self,
        column_name: &str,
    ) -> Result<Option<ZeroAccessor<CombinedColumnStatistics>>, BBFError> {
        if let Some(cached) = self.cache_statistics.lock().get(column_name) {
            return Ok(Some(cached.clone()));
        }

        let resolver = if let Some(resolver) = self.index.columns.get(column_name) {
            resolver
        } else {
            return Ok(None);
        };

        let statistics = resolver.resolve(&self.reader).await.map_err(|e| {
            BBFError::Reading(
                format!(
                    "Failed to read column statistics for '{}': {}",
                    column_name, e
                )
                .into(),
            )
        })?;
        self.cache_statistics
            .lock()
            .insert(column_name.to_string(), statistics.clone());
        Ok(Some(statistics))
    }
}

#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct CombinedColumnStatistics {
    null_count: ArrayBuffer,
    row_count: ArrayBuffer,
    min_value: ArrayBuffer,
    max_value: ArrayBuffer,
    data_type: DataType,
}

impl ArchivedCombinedColumnStatistics {
    pub fn null_count(&self) -> ArrayRef {
        to_arrow_array(&self.null_count)
    }
    pub fn row_count(&self) -> ArrayRef {
        to_arrow_array(&self.row_count)
    }
    pub fn min_value(&self) -> ArrayRef {
        to_arrow_array(&self.min_value)
    }
    pub fn max_value(&self) -> ArrayRef {
        to_arrow_array(&self.max_value)
    }
    pub fn arrow_data_type(&self) -> arrow::datatypes::DataType {
        self.data_type.to_arrow()
    }
}

pub struct CombinedColumnStatisticsBuilder {
    num_entries: usize,
    null_count: PrimitiveRunBuilder<Int32Type, UInt64Type>,
    row_count: PrimitiveRunBuilder<Int32Type, UInt64Type>,
    min_value: PruningArrayBuilder<RunPruningArrayBuilder>,
    max_value: PruningArrayBuilder<RunPruningArrayBuilder>,
    data_type: arrow::datatypes::DataType,
}

impl CombinedColumnStatisticsBuilder {
    pub fn new(pre_len: Option<usize>) -> Self {
        let mut null_count_builder = PrimitiveRunBuilder::<Int32Type, UInt64Type>::new();
        let mut row_count_builder = PrimitiveRunBuilder::<Int32Type, UInt64Type>::new();
        let min_value_builder = PruningArrayBuilder::new(pre_len);
        let max_value_builder = PruningArrayBuilder::new(pre_len);

        for _ in 0..pre_len.unwrap_or_default() {
            null_count_builder.append_value(1);
            row_count_builder.append_value(1);
        }

        Self {
            num_entries: pre_len.unwrap_or_default(),
            null_count: null_count_builder,
            row_count: row_count_builder,
            min_value: min_value_builder,
            max_value: max_value_builder,
            data_type: arrow::datatypes::DataType::Null,
        }
    }

    pub fn append_null(&mut self) {
        // Append null count == row count == 1, min and max as null
        // This indicated that this entry only has nulls
        self.null_count.append_value(1);
        self.row_count.append_value(1);
        self.min_value.append_null();
        self.max_value.append_null();
        self.num_entries += 1;
    }

    pub fn append(&mut self, statistics: &mut ColumnStatistics) {
        self.row_count.append_value(statistics.row_count as u64);
        self.null_count.append_value(statistics.null_count as u64);
        match statistics.max_value() {
            Some(max_value) => {
                self.max_value.append(max_value.clone()).unwrap();
            }
            None => {
                self.max_value.append_null();
            }
        }
        match statistics.min_value() {
            Some(min_value) => {
                self.min_value.append(min_value.clone()).unwrap();
            }
            None => {
                self.min_value.append_null();
            }
        }
        self.num_entries += 1;
    }

    pub fn build(mut self) -> CombinedColumnStatistics {
        // Validate logical lengths
        assert_eq!(self.null_count.len(), self.num_entries);
        assert_eq!(self.row_count.len(), self.num_entries);

        let null_count_full = flatten_run_array(&self.null_count.finish_cloned());
        let row_count_full = flatten_run_array(&self.row_count.finish_cloned());
        let min_value_full = self.min_value.finish();
        let max_value_full = self.max_value.finish();

        // Assert they all have the same length
        assert_eq!(null_count_full.len(), self.num_entries);
        assert_eq!(row_count_full.len(), self.num_entries);
        assert_eq!(min_value_full.len(), self.num_entries);
        assert_eq!(max_value_full.len(), self.num_entries);

        CombinedColumnStatistics {
            null_count: ArrayBuffer::try_from_arrow(null_count_full).unwrap(),
            row_count: ArrayBuffer::try_from_arrow(row_count_full).unwrap(),
            min_value: ArrayBuffer::try_from_arrow(min_value_full).unwrap(),
            max_value: ArrayBuffer::try_from_arrow(max_value_full).unwrap(),
            data_type: DataType::from_arrow(self.data_type.clone()),
        }
    }
}

fn flatten_run_array<R: RunEndIndexType>(run: &RunArray<R>) -> ArrayRef {
    // 1) logical indices [0, 1, 2, ..., len-1]
    let len = run.len();
    let logical: Vec<u32> = (0..(len as u32)).collect();

    // 2) map logical -> physical indices inside the values array
    let phys = run.get_physical_indices(&logical).expect("indices mapping");

    // 3) gather those physical indices from the values() array
    let idx = UInt32Array::from_iter_values(phys.into_iter().map(|i| i as u32));
    arrow::compute::take(run.values().as_ref(), &idx, None).expect("gather")
}

pub struct PruningIndexBuilder {
    num_containers: usize,
    columns: indexmap::IndexMap<String, CombinedColumnStatisticsBuilder>,
}

impl PruningIndexBuilder {
    pub fn new() -> Self {
        Self {
            num_containers: 0,
            columns: indexmap::IndexMap::new(),
        }
    }

    pub fn append_statistics(&mut self, statistics: indexmap::IndexMap<String, ColumnStatistics>) {
        let null_appendable_columns: Vec<_> = self
            .columns
            .keys()
            .filter(|name| !statistics.contains_key(*name))
            .cloned()
            .collect();

        for (name, mut stats) in statistics {
            let entry = self
                .columns
                .entry(name.clone())
                .or_insert_with(|| CombinedColumnStatisticsBuilder::new(Some(self.num_containers)));

            entry.append(&mut stats);
        }

        for name in null_appendable_columns {
            self.columns.get_mut(&name).unwrap().append_null();
        }

        self.num_containers += 1;
    }

    pub fn build<W: Write + Seek>(
        self,
        writer: &mut W,
        compression: Option<Compression>,
    ) -> PruningIndex {
        let mut columns = indexmap::IndexMap::new();

        for (name, builder) in self.columns {
            columns.insert(
                name,
                Resolver::create_resolver(writer, Store::new(builder.build(), compression.clone())),
            );
        }

        PruningIndex {
            num_containers: self.num_containers,
            columns,
        }
    }
}

impl Default for PruningIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub trait PruningArrayBuilderImpl {
    fn new(pre_len: Option<usize>) -> Self
    where
        Self: Sized;
    fn builder_data_type(&self) -> &arrow::datatypes::DataType;
    fn cast(&mut self, data_type: &arrow::datatypes::DataType);
    fn append(&mut self, array: ArrayRef) -> Result<(), BBFError>;
    fn append_null(&mut self);
    fn finish(&mut self) -> ArrayRef;
}

pub struct PruningArrayBuilder<T: PruningArrayBuilderImpl> {
    array_builder: T,
}

impl<T: PruningArrayBuilderImpl> PruningArrayBuilder<T> {
    pub fn new(pre_len: Option<usize>) -> Self {
        Self {
            array_builder: T::new(pre_len),
        }
    }

    pub fn append(&mut self, mut array: ArrayRef) -> Result<(), BBFError> {
        if self.array_builder.builder_data_type() != array.data_type() {
            let super_type =
                super_type_arrow(self.array_builder.builder_data_type(), array.data_type())
                    .ok_or(BBFError::Writing("Incompatible data types".into()))?;

            if self.array_builder.builder_data_type() != &super_type {
                self.array_builder.cast(&super_type);
            }
            if array.data_type() != &super_type {
                // cast the array to the super type
                array = arrow::compute::cast(&array, &super_type).map_err(|e| {
                    BBFError::Writing(format!("Failed to cast array: {}", e).into())
                })?;
            }
        }
        self.array_builder.append(array)?;
        Ok(())
    }

    pub fn append_null(&mut self) {
        self.array_builder.append_null();
    }

    pub fn finish(&mut self) -> ArrayRef {
        self.array_builder.finish()
    }
}

pub struct RunPruningArrayBuilder {
    data_type: arrow::datatypes::DataType,
    builder: Box<dyn ArrayBuilder>,
}

impl RunPruningArrayBuilder {
    pub fn new(pre_len: Option<usize>) -> Self {
        let data_type = arrow::datatypes::DataType::Null;
        let mut builder = Box::new(NullBuilder::new());

        if let Some(len) = pre_len {
            builder.append_nulls(len);
        }

        Self { data_type, builder }
    }

    fn make_builder(
        data_type: &arrow::datatypes::DataType,
    ) -> Result<Box<dyn ArrayBuilder>, BBFError> {
        match data_type {
            arrow::datatypes::DataType::Null => {
                Ok(Box::new(NullBuilder::new()) as Box<dyn ArrayBuilder>)
            }
            arrow::datatypes::DataType::Boolean => Ok(Box::new(BooleanBuilder::new())),
            arrow::datatypes::DataType::Int8 => {
                Ok(Box::new(PrimitiveRunBuilder::<Int32Type, Int8Type>::new()))
            }
            arrow::datatypes::DataType::Int16 => {
                Ok(Box::new(PrimitiveRunBuilder::<Int32Type, Int16Type>::new()))
            }
            arrow::datatypes::DataType::Int32 => {
                Ok(Box::new(PrimitiveRunBuilder::<Int32Type, Int32Type>::new()))
            }

            arrow::datatypes::DataType::Int64 => {
                Ok(Box::new(PrimitiveRunBuilder::<Int32Type, Int64Type>::new()))
            }

            arrow::datatypes::DataType::UInt8 => {
                Ok(Box::new(PrimitiveRunBuilder::<Int32Type, UInt8Type>::new()))
            }

            arrow::datatypes::DataType::UInt16 => {
                Ok(Box::new(PrimitiveRunBuilder::<Int32Type, UInt16Type>::new()))
            }

            arrow::datatypes::DataType::UInt32 => {
                Ok(Box::new(PrimitiveRunBuilder::<Int32Type, UInt32Type>::new()))
            }

            arrow::datatypes::DataType::UInt64 => {
                Ok(Box::new(PrimitiveRunBuilder::<Int32Type, UInt64Type>::new()))
            }

            arrow::datatypes::DataType::Float32 => Ok(Box::new(PrimitiveRunBuilder::<
                Int32Type,
                Float32Type,
            >::new())),

            arrow::datatypes::DataType::Float64 => Ok(Box::new(PrimitiveRunBuilder::<
                Int32Type,
                Float64Type,
            >::new())),

            arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Second, _) => {
                Ok(Box::new(PrimitiveRunBuilder::<
                    Int32Type,
                    TimestampSecondType,
                >::new()))
            }

            arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
                Ok(Box::new(PrimitiveRunBuilder::<
                    Int32Type,
                    TimestampMillisecondType,
                >::new()))
            }

            arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
                Ok(Box::new(PrimitiveRunBuilder::<
                    Int32Type,
                    TimestampMicrosecondType,
                >::new()))
            }

            arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
                Ok(Box::new(PrimitiveRunBuilder::<
                    Int32Type,
                    TimestampNanosecondType,
                >::new()))
            }

            arrow::datatypes::DataType::Utf8 => Ok(Box::new(StringRunBuilder::<Int32Type>::new())),

            arrow::datatypes::DataType::Binary => {
                Ok(Box::new(BinaryRunBuilder::<Int32Type>::new()))
            }
            _ => Err(BBFError::Writing(
                format!(
                    "Unsupported data type for array builder pruning index: {:?}",
                    data_type
                )
                .into(),
            )),
        }
    }
}

impl PruningArrayBuilderImpl for RunPruningArrayBuilder {
    fn new(pre_len: Option<usize>) -> Self
    where
        Self: Sized,
    {
        Self::new(pre_len)
    }

    fn builder_data_type(&self) -> &arrow::datatypes::DataType {
        &self.data_type
    }

    fn cast(&mut self, data_type: &arrow::datatypes::DataType) {
        let new_builder = Self::make_builder(data_type).unwrap();
        let old_array = self.builder.finish_cloned();
        let flat_array = match old_array.as_run_opt::<Int32Type>() {
            Some(run_array) => {
                // Flatten the run array to a normal array
                flatten_run_array(run_array)
            }
            None => {
                // not a run array, just cast the whole array
                old_array
            }
        };
        let casted_array = arrow::compute::cast(&flat_array, data_type).unwrap();
        self.builder = new_builder;
        self.data_type = data_type.clone();
        self.append(casted_array).unwrap();
    }

    fn append(&mut self, array: ArrayRef) -> Result<(), BBFError> {
        match &self.data_type {
            arrow::datatypes::DataType::Null => {
                self.builder
                    .as_any_mut()
                    .downcast_mut::<NullBuilder>()
                    .unwrap()
                    .append_null();
            }
            arrow::datatypes::DataType::Boolean => {
                let boolean_builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<BooleanBuilder>()
                    .unwrap();
                let boolean_array = array.as_boolean();
                boolean_array.iter().for_each(|v| {
                    boolean_builder.append_option(v);
                });
            }
            arrow::datatypes::DataType::Int8 => {
                let int_builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveRunBuilder<Int32Type, Int8Type>>()
                    .unwrap();
                let int_array = array.as_primitive::<Int8Type>();
                int_array.iter().for_each(|v| {
                    int_builder.append_option(v);
                });
            }
            arrow::datatypes::DataType::Int16 => {
                let int_builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveRunBuilder<Int32Type, Int16Type>>()
                    .unwrap();
                let int_array = array.as_primitive::<Int16Type>();
                int_array.iter().for_each(|v| {
                    int_builder.append_option(v);
                });
            }
            arrow::datatypes::DataType::Int32 => {
                let int_builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveRunBuilder<Int32Type, Int32Type>>()
                    .unwrap();
                let int_array = array.as_primitive::<Int32Type>();
                int_array.iter().for_each(|v| {
                    int_builder.append_option(v);
                });
            }

            arrow::datatypes::DataType::Int64 => {
                let int_builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveRunBuilder<Int32Type, Int64Type>>()
                    .unwrap();
                let int_array = array.as_primitive::<Int64Type>();
                int_array.iter().for_each(|v| {
                    int_builder.append_option(v);
                });
            }

            arrow::datatypes::DataType::UInt8 => {
                let uint_builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveRunBuilder<Int32Type, UInt8Type>>()
                    .unwrap();
                let uint_array = array.as_primitive::<UInt8Type>();
                uint_array.iter().for_each(|v| {
                    uint_builder.append_option(v);
                });
            }
            arrow::datatypes::DataType::UInt16 => {
                let uint_builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveRunBuilder<Int32Type, UInt16Type>>()
                    .unwrap();
                let uint_array = array.as_primitive::<UInt16Type>();
                uint_array.iter().for_each(|v| {
                    uint_builder.append_option(v);
                });
            }

            arrow::datatypes::DataType::UInt32 => {
                let uint_builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveRunBuilder<Int32Type, UInt32Type>>()
                    .unwrap();
                let uint_array = array.as_primitive::<UInt32Type>();
                uint_array.iter().for_each(|v| {
                    uint_builder.append_option(v);
                });
            }

            arrow::datatypes::DataType::UInt64 => {
                let uint_builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveRunBuilder<Int32Type, UInt64Type>>()
                    .unwrap();
                let uint_array = array.as_primitive::<UInt64Type>();
                uint_array.iter().for_each(|v| {
                    uint_builder.append_option(v);
                });
            }

            arrow::datatypes::DataType::Float32 => {
                let float_builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveRunBuilder<Int32Type, Float32Type>>()
                    .unwrap();
                let float_array = array.as_primitive::<Float32Type>();
                float_array.iter().for_each(|v| {
                    float_builder.append_option(v);
                });
            }

            arrow::datatypes::DataType::Float64 => {
                let float_builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveRunBuilder<Int32Type, Float64Type>>()
                    .unwrap();
                let float_array = array.as_primitive::<Float64Type>();
                float_array.iter().for_each(|v| {
                    float_builder.append_option(v);
                });
            }

            arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Second, _) => {
                let timestamp_builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveRunBuilder<Int32Type, TimestampSecondType>>()
                    .unwrap();
                let timestamp_array = array.as_primitive::<TimestampSecondType>();
                timestamp_array.iter().for_each(|v| {
                    timestamp_builder.append_option(v);
                });
            }

            arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
                let timestamp_builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveRunBuilder<Int32Type, TimestampMillisecondType>>()
                    .unwrap();
                let timestamp_array = array.as_primitive::<TimestampMillisecondType>();
                timestamp_array.iter().for_each(|v| {
                    timestamp_builder.append_option(v);
                });
            }

            arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
                let timestamp_builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveRunBuilder<Int32Type, TimestampMicrosecondType>>()
                    .unwrap();
                let timestamp_array = array.as_primitive::<TimestampMicrosecondType>();
                timestamp_array.iter().for_each(|v| {
                    timestamp_builder.append_option(v);
                });
            }

            arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
                let timestamp_builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveRunBuilder<Int32Type, TimestampNanosecondType>>()
                    .unwrap();
                let timestamp_array = array.as_primitive::<TimestampNanosecondType>();
                timestamp_array.iter().for_each(|v| {
                    timestamp_builder.append_option(v);
                });
            }

            arrow::datatypes::DataType::Utf8 => {
                let utf8_builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<StringRunBuilder<Int32Type>>()
                    .unwrap();
                let string_array = array.as_string::<i32>();
                string_array.iter().for_each(|v| {
                    utf8_builder.append_option(v);
                });
            }

            arrow::datatypes::DataType::Binary => {
                let binary_builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<BinaryRunBuilder<Int32Type>>()
                    .unwrap();
                let binary_array = array.as_binary::<i32>();
                binary_array.iter().for_each(|v| {
                    binary_builder.append_option(v);
                });
            }

            data_type => {
                return Err(BBFError::Writing(
                    format!(
                        "Unsupported data type for RunPruningArrayBuilder: {:?}",
                        data_type
                    )
                    .into(),
                ))
            }
        }
        Ok(())
    }

    fn append_null(&mut self) {
        let null_array = new_null_array(&self.data_type, 1);
        self.append(null_array).unwrap();
    }

    fn finish(&mut self) -> ArrayRef {
        let finished = self.builder.finish();
        match finished.as_run_opt::<Int32Type>() {
            Some(run_array) => flatten_run_array(run_array),
            None => finished,
        }
    }
}
