use std::ops::RangeInclusive;

use arrow::array::ArrayRef;
use nd_arrow_array::NdArrowArray;
use rkyv::{option::ArchivedOption, validation::archive};

use crate::{
    array::{self, arrow::to_arrow_array, column::ArrayColumn, util::ZeroAccessor},
    error::BBFError,
    reader::async_reader::{
        cache::{SharedCache, SharedCacheKey},
        AsyncChunkReader,
    },
};

pub struct AsyncArrayReader {
    column_name: String,
    io_reader: AsyncChunkReader,
    column: ZeroAccessor<ArrayColumn>,
    ranges: Vec<RangeInclusive<usize>>,
    shared_cache: SharedCache,
}

impl AsyncArrayReader {
    pub fn new<S: Into<String>>(
        column_name: S,
        column: ZeroAccessor<ArrayColumn>,
        shared_cache: SharedCache,
        io_reader: AsyncChunkReader,
    ) -> Self {
        let ranges = column
            .as_ref()
            .groups
            .iter()
            .map(|g| g.range.start.to_native() as usize..=g.range.end.to_native() as usize)
            .collect::<Vec<_>>();

        Self {
            io_reader,
            column,
            ranges,
            shared_cache,
            column_name: column_name.into(),
        }
    }

    pub async fn read_array(&self, entry_index: usize) -> Result<Option<NdArrowArray>, BBFError> {
        let batch_index = if let Some(index) = self.find_group_index(entry_index) {
            index
        } else {
            return Ok(None);
        };
        let array_offset = self.find_array_offset(batch_index, entry_index);

        let key = (self.column_name.clone(), batch_index);

        let cloned_column = self.column.clone();
        let chunk_reader = self.io_reader.clone();
        // let group = &cloned_column.as_ref().groups[batch_index];
        // let out = group.data.resolve(&chunk_reader).await.unwrap();

        let out = self
            .shared_cache
            .get_or_insert_with(key.clone(), move |_k: &SharedCacheKey| async move {
                let group = &cloned_column.as_ref().groups[batch_index];
                let data = group.data.resolve(&chunk_reader).await.unwrap();
                data
            })
            .await;

        let array = out.as_ref().arrays.get(array_offset).unwrap();
        let dimensions = out.as_ref().array_dimensions.get(array_offset).unwrap();

        match (array, dimensions) {
            (ArchivedOption::Some(archived_array), ArchivedOption::Some(dimensions)) => {
                let array = to_arrow_array(archived_array);
                let dimensions = dimensions.into();
                Ok(NdArrowArray::new(array, dimensions).ok())
            }
            (ArchivedOption::Some(archived_array), _) => {
                let array = to_arrow_array(archived_array);
                let dimensions = nd_arrow_array::dimensions::Dimensions::Scalar;
                Ok(NdArrowArray::new(array, dimensions).ok())
            }
            _ => Ok(None),
        }
    }

    fn find_array_offset(&self, batch_index: usize, array_index: usize) -> usize {
        // Find the start of the array in the batch
        let range = &self.column.as_ref().groups[batch_index].range;
        if array_index < range.start.to_native() as usize
            || array_index > range.end.to_native() as usize
        {
            panic!("Array index out of bounds for batch");
        }
        (array_index - range.start.to_native() as usize) as usize
    }

    fn find_group_index(&self, array_index: usize) -> Option<usize> {
        // Use binary search to find the batch index for the given array index
        Self::find_group_index_impl(&self.ranges, array_index)
    }

    fn find_group_index_impl(
        group_ranges: &[RangeInclusive<usize>],
        array_index: usize,
    ) -> Option<usize> {
        group_ranges
            .binary_search_by(|r| {
                // if this range ends before x, it’s “less” than the target
                if *r.end() < array_index {
                    std::cmp::Ordering::Less
                }
                // if this range starts after x, it’s “greater”
                else if *r.start() > array_index {
                    std::cmp::Ordering::Greater
                }
                // otherwise x is inside [start..=end], so it’s equal
                else {
                    std::cmp::Ordering::Equal
                }
            })
            .ok()
    }
}
