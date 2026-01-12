use std::{
    io::{Seek, Write},
    sync::Arc,
};

use crate::array::{
    compression::Compression,
    dimensions::Dimensions,
    group::{ArrayGroupBuilder, EncodedArrayGroup},
};

#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ArrayColumn {
    pub groups: Vec<EncodedArrayGroup>,
    pub column_size: usize,
}

pub struct ArrayColumnBuilder {
    pub groups: Vec<EncodedArrayGroup>,
    pub current_builder: ArrayGroupBuilder,
    pub max_group_size: usize,
    pub compression: Option<Compression>,
}

impl ArrayColumnBuilder {
    pub fn new(max_group_size: usize, compression: Option<Compression>) -> Self {
        Self {
            groups: Vec::new(),
            current_builder: ArrayGroupBuilder::new(),
            max_group_size,
            compression,
        }
    }

    fn finish_group<W: Write + Seek>(&mut self, writer: &mut W) {
        let group = std::mem::replace(&mut self.current_builder, ArrayGroupBuilder::new())
            .finish(writer, self.compression.clone());
        if let Some(group) = group {
            self.groups.push(group);
        }
    }

    pub fn append<W: Write + Seek>(
        &mut self,
        writer: &mut W,
        entry_id: usize,
        array: Arc<dyn arrow::array::Array>,
        dimensions: Dimensions,
    ) {
        self.current_builder.append(array, dimensions, entry_id);

        if self.current_builder.size() >= self.max_group_size {
            self.finish_group(writer);
        }
    }

    pub fn append_null<W: Write + Seek>(&mut self, writer: &mut W, entry_id: usize) {
        self.current_builder.append_null(entry_id);

        if self.current_builder.size() >= self.max_group_size {
            self.finish_group(writer);
        }
    }

    pub fn finish<W: Write + Seek>(mut self, writer: &mut W) -> ArrayColumn {
        // Finalize the last group if it has any entries
        if self.current_builder.size() > 0 {
            self.finish_group(writer);
        }

        ArrayColumn {
            column_size: self.groups.len(),
            groups: self.groups,
        }
    }
}
