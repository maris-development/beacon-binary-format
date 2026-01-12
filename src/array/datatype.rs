#[derive(Debug, Copy, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum DataType {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    TimestampSeconds,
    TimestampMilliseconds,
    TimestampMicroseconds,
    TimestampNanoseconds,
    String,
    Binary,
}

impl From<&str> for DataType {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "null" => DataType::Null,
            "boolean" => DataType::Boolean,
            "int8" => DataType::Int8,
            "int16" => DataType::Int16,
            "int32" => DataType::Int32,
            "int64" => DataType::Int64,
            "uint8" => DataType::UInt8,
            "uint16" => DataType::UInt16,
            "uint32" => DataType::UInt32,
            "uint64" => DataType::UInt64,
            "float32" => DataType::Float32,
            "float64" => DataType::Float64,
            "timestamp(seconds)" => DataType::TimestampSeconds,
            "timestamp(milliseconds)" => DataType::TimestampMilliseconds,
            "timestamp(microseconds)" => DataType::TimestampMicroseconds,
            "timestamp(nanoseconds)" => DataType::TimestampNanoseconds,
            "string" => DataType::String,
            "binary" => DataType::Binary,
            _ => panic!("Unsupported data type string: {}", s),
        }
    }
}

impl DataType {
    pub fn from_arrow(data_type: arrow::datatypes::DataType) -> Self {
        match data_type {
            arrow::datatypes::DataType::Null => DataType::Null,
            arrow::datatypes::DataType::Boolean => DataType::Boolean,
            arrow::datatypes::DataType::Int8 => DataType::Int8,
            arrow::datatypes::DataType::Int16 => DataType::Int16,
            arrow::datatypes::DataType::Int32 => DataType::Int32,
            arrow::datatypes::DataType::Int64 => DataType::Int64,
            arrow::datatypes::DataType::UInt8 => DataType::UInt8,
            arrow::datatypes::DataType::UInt16 => DataType::UInt16,
            arrow::datatypes::DataType::UInt32 => DataType::UInt32,
            arrow::datatypes::DataType::UInt64 => DataType::UInt64,
            arrow::datatypes::DataType::Float32 => DataType::Float32,
            arrow::datatypes::DataType::Float64 => DataType::Float64,
            arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Second, None) => {
                DataType::TimestampSeconds
            }
            arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None) => {
                DataType::TimestampMilliseconds
            }
            arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None) => {
                DataType::TimestampMicroseconds
            }
            arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None) => {
                DataType::TimestampNanoseconds
            }
            arrow::datatypes::DataType::Utf8 => DataType::String,
            arrow::datatypes::DataType::Binary => DataType::Binary,
            arrow::datatypes::DataType::RunEndEncoded(_, values_field) => {
                Self::from_arrow(values_field.data_type().clone())
            }
            _ => panic!("Unsupported arrow data type: {:?}", data_type),
        }
    }

    pub fn to_arrow(&self) -> arrow::datatypes::DataType {
        match self {
            DataType::Null => arrow::datatypes::DataType::Null,
            DataType::Boolean => arrow::datatypes::DataType::Boolean,
            DataType::Int8 => arrow::datatypes::DataType::Int8,
            DataType::Int16 => arrow::datatypes::DataType::Int16,
            DataType::Int32 => arrow::datatypes::DataType::Int32,
            DataType::Int64 => arrow::datatypes::DataType::Int64,
            DataType::UInt8 => arrow::datatypes::DataType::UInt8,
            DataType::UInt16 => arrow::datatypes::DataType::UInt16,
            DataType::UInt32 => arrow::datatypes::DataType::UInt32,
            DataType::UInt64 => arrow::datatypes::DataType::UInt64,
            DataType::Float32 => arrow::datatypes::DataType::Float32,
            DataType::Float64 => arrow::datatypes::DataType::Float64,
            DataType::TimestampSeconds => {
                arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Second, None)
            }
            DataType::TimestampMilliseconds => {
                arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None)
            }
            DataType::TimestampMicroseconds => {
                arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
            }
            DataType::TimestampNanoseconds => {
                arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
            }
            DataType::String => arrow::datatypes::DataType::Utf8,
            DataType::Binary => arrow::datatypes::DataType::Binary,
        }
    }
}

impl ArchivedDataType {
    pub fn to_arrow(&self) -> arrow::datatypes::DataType {
        match self {
            ArchivedDataType::Null => arrow::datatypes::DataType::Null,
            ArchivedDataType::Boolean => arrow::datatypes::DataType::Boolean,
            ArchivedDataType::Int8 => arrow::datatypes::DataType::Int8,
            ArchivedDataType::Int16 => arrow::datatypes::DataType::Int16,
            ArchivedDataType::Int32 => arrow::datatypes::DataType::Int32,
            ArchivedDataType::Int64 => arrow::datatypes::DataType::Int64,
            ArchivedDataType::UInt8 => arrow::datatypes::DataType::UInt8,
            ArchivedDataType::UInt16 => arrow::datatypes::DataType::UInt16,
            ArchivedDataType::UInt32 => arrow::datatypes::DataType::UInt32,
            ArchivedDataType::UInt64 => arrow::datatypes::DataType::UInt64,
            ArchivedDataType::Float32 => arrow::datatypes::DataType::Float32,
            ArchivedDataType::Float64 => arrow::datatypes::DataType::Float64,
            ArchivedDataType::TimestampSeconds => {
                arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Second, None)
            }
            ArchivedDataType::TimestampMilliseconds => {
                arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None)
            }

            ArchivedDataType::TimestampMicroseconds => {
                arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
            }

            ArchivedDataType::TimestampNanoseconds => {
                arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
            }

            ArchivedDataType::String => arrow::datatypes::DataType::Utf8,
            ArchivedDataType::Binary => arrow::datatypes::DataType::Binary,
        }
    }
}
