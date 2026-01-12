use crate::array::datatype::DataType;

#[derive(Debug, Clone, PartialEq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum Element {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    String(String),
    TimestampSeconds(i64),
    TimestampMilliseconds(i64),
    TimestampMicroseconds(i64),
    TimestampNanoseconds(i64),
    Binary(Vec<u8>),
}

impl Element {
    pub fn data_type(&self) -> DataType {
        match self {
            Element::Null => DataType::Null,
            Element::Boolean(_) => DataType::Boolean,
            Element::Int8(_) => DataType::Int8,
            Element::Int16(_) => DataType::Int16,
            Element::Int32(_) => DataType::Int32,
            Element::Int64(_) => DataType::Int64,
            Element::UInt8(_) => DataType::UInt8,
            Element::UInt16(_) => DataType::UInt16,
            Element::UInt32(_) => DataType::UInt32,
            Element::UInt64(_) => DataType::UInt64,
            Element::Float32(_) => DataType::Float32,
            Element::Float64(_) => DataType::Float64,
            Element::String(_) => DataType::String,
            Element::TimestampSeconds(_) => DataType::TimestampSeconds,
            Element::TimestampMilliseconds(_) => DataType::TimestampMilliseconds,
            Element::TimestampMicroseconds(_) => DataType::TimestampMicroseconds,
            Element::TimestampNanoseconds(_) => DataType::TimestampNanoseconds,
            Element::Binary(_) => DataType::Binary,
        }
    }

    pub fn arrow_data_type(&self) -> arrow::datatypes::DataType {
        self.data_type().to_arrow()
    }

    pub fn as_null(&self) -> Option<()> {
        match self {
            Element::Null => Some(()),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Element::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i8(&self) -> Option<i8> {
        match self {
            Element::Int8(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i16(&self) -> Option<i16> {
        match self {
            Element::Int16(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Element::Int32(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Element::Int64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u8(&self) -> Option<u8> {
        match self {
            Element::UInt8(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u16(&self) -> Option<u16> {
        match self {
            Element::UInt16(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u32(&self) -> Option<u32> {
        match self {
            Element::UInt32(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Element::UInt64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Element::Float32(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Element::Float64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&str> {
        match self {
            Element::String(ref v) => Some(v),
            _ => None,
        }
    }

    pub fn as_timestamp_seconds(&self) -> Option<i64> {
        match self {
            Element::TimestampSeconds(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_timestamp_milliseconds(&self) -> Option<i64> {
        match self {
            Element::TimestampMilliseconds(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_timestamp_microseconds(&self) -> Option<i64> {
        match self {
            Element::TimestampMicroseconds(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_timestamp_nanoseconds(&self) -> Option<i64> {
        match self {
            Element::TimestampNanoseconds(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_binary(&self) -> Option<&[u8]> {
        match self {
            Element::Binary(ref v) => Some(v),
            _ => None,
        }
    }
}
