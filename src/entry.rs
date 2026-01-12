use arrow::array::ArrayRef;
use nd_arrow_array::NdArrowArray;

use crate::array::{datatype::DataType, dimensions::Dimensions};

pub enum Entry {
    Chunked {
        chunked: Box<dyn Iterator<Item = ArrayCollection>>,
    },
    Default(ArrayCollection),
}

impl Entry {
    pub const FIELD_NAME: &'static str = "__entry_key";
    pub const FIELD_CHUNK_INDEX: &'static str = "__chunk_index";

    pub fn new(collection: ArrayCollection) -> Self {
        Entry::Default(collection)
    }

    pub fn new_chunked(chunked: Box<dyn Iterator<Item = ArrayCollection>>) -> Self {
        Entry::Chunked { chunked }
    }
}

pub struct ArrayCollection {
    pub name: String,
    pub columns: Box<dyn Iterator<Item = Column>>,
}

impl ArrayCollection {
    pub fn new<S: Into<String>>(name: S, columns: Box<dyn Iterator<Item = Column>>) -> Self {
        Self {
            name: name.into(),
            columns,
        }
    }
}

pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub array: ArrayRef,
    pub dimensions: Dimensions,
}

impl Column {
    pub fn new<S: Into<String>>(name: S, array: ArrayRef, dimensions: Dimensions) -> Self {
        Self {
            name: name.into(),
            data_type: DataType::from_arrow(array.data_type().clone()),
            array,
            dimensions,
        }
    }

    pub fn array(&self) -> &ArrayRef {
        &self.array
    }

    pub fn try_from_arrow<S: Into<String>>(
        name: S,
        arrow_array: arrow::array::ArrayRef,
    ) -> Result<Self, String> {
        let name = name.into();
        let dimensions = Dimensions::Multi(vec![("arrow1", arrow_array.len()).into()]);
        let data_type = DataType::from_arrow(arrow_array.data_type().clone());
        Ok(Self {
            name,
            data_type,
            array: arrow_array,
            dimensions,
        })
    }

    pub fn from_nd_arrow<S: Into<String>>(name: S, array: NdArrowArray) -> Self {
        let name = name.into();
        let dimensions = array.dimensions().into();
        let arrow_array = array.as_arrow_array().clone();
        let data_type = DataType::from_arrow(arrow_array.data_type().clone());
        Self {
            name,
            data_type,
            array: arrow_array,
            dimensions,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct EntryKey {
    pub name: String,
    pub chunk_index: Option<usize>,
}

impl EntryKey {
    pub fn new(name: String, chunk_index: Option<usize>) -> Self {
        Self { name, chunk_index }
    }
}
