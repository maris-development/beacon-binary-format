use std::io::{Seek, Write};
use std::sync::Arc;

use crate::array::ArrayBuffer;
use crate::array::{compression::Compression, dimensions::Dimensions, util::Store, Array};
use crate::resolver::Resolver;

#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct EncodedArrayGroup {
    pub range: std::ops::RangeInclusive<usize>,
    pub data: Resolver<ArrayGroupData>,
}

#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ArrayGroupData {
    pub arrays: Vec<Option<ArrayBuffer>>,
    pub array_dimensions: Vec<Option<Dimensions>>,
}

pub struct ArrayGroupBuilder {
    rle_nulls: Vec<(usize, usize)>, // (start_index, length)
    entries: usize,
    pub arrays: Vec<ArrayBuffer>,
    pub array_dimensions: Vec<Dimensions>,
    pub size: usize,
    current_range: Option<std::ops::RangeInclusive<usize>>,
}

impl ArrayGroupBuilder {
    pub fn new() -> Self {
        Self {
            rle_nulls: Vec::new(),
            entries: 0,
            arrays: Vec::new(),
            array_dimensions: Vec::new(),
            size: 0,
            current_range: None,
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn append_null(&mut self, entry_id: usize) {
        if let Some((start, length)) = self.rle_nulls.last_mut() {
            if *start + *length == self.entries {
                *length += 1;
            } else {
                self.rle_nulls.push((self.entries, 1));
            }
        } else {
            self.rle_nulls.push((self.entries, 1));
        }

        self.size += std::mem::size_of::<Option<ArrayBuffer>>();
        if self.current_range.is_none() {
            self.current_range = Some(entry_id..=entry_id);
        } else {
            let range = self.current_range.as_mut().unwrap();
            *range = *range.start()..=entry_id;
        }
        self.entries += 1;
    }

    pub fn append(
        &mut self,
        array: Arc<dyn arrow::array::Array>,
        dimensions: Dimensions,
        entry_id: usize,
    ) {
        // Verify that the dimensions match the array length
        assert_eq!(
            dimensions.len(),
            array.len(),
            "Array length does not match dimensions length"
        );

        let array_buffer = ArrayBuffer::try_from_arrow(array.clone()).unwrap();

        self.size += array.get_array_memory_size();
        self.arrays.push(array_buffer);
        self.array_dimensions.push(dimensions);

        if self.current_range.is_none() {
            self.current_range = Some(entry_id..=entry_id);
        } else {
            let range = self.current_range.as_mut().unwrap();
            *range = *range.start()..=entry_id;
        }
        self.entries += 1;
    }

    pub fn finish<W: Write + Seek>(
        self,
        writer: &mut W,
        compression: Option<Compression>,
    ) -> Option<EncodedArrayGroup> {
        if self.size == 0 {
            return None; // No data to encode
        }

        // Interleave arrays and dimensions with nulls using iterators and without cloning
        let mut arrays = Vec::with_capacity(self.entries);
        let mut array_dimensions = Vec::with_capacity(self.entries);
        let mut arrays_iter = self.arrays.into_iter();
        let mut dims_iter = self.array_dimensions.into_iter();
        let mut current_index = 0;
        for (start, length) in self.rle_nulls {
            // Fill in non-null entries before the null run
            let non_null_count = start.saturating_sub(current_index);
            for _ in 0..non_null_count {
                arrays.push(arrays_iter.next());
                array_dimensions.push(dims_iter.next());
            }
            current_index += non_null_count;
            // Fill in null entries
            for _ in 0..length {
                arrays.push(None);
                array_dimensions.push(None);
            }
            current_index += length;
        }
        // Fill in any remaining non-null entries
        for (array, dim) in arrays_iter.zip(dims_iter) {
            arrays.push(Some(array));
            array_dimensions.push(Some(dim));
        }

        assert!(arrays.len() == self.entries);
        assert!(array_dimensions.len() == self.entries);

        let data = ArrayGroupData {
            arrays,
            array_dimensions,
        };

        // The range should never be empty here as the size is greater than zero
        Some(EncodedArrayGroup {
            range: self.current_range.unwrap(),
            data: Resolver::create_resolver(writer, Store::new(data, compression)),
        })
    }
}
