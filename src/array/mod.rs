use ::arrow::{
    array::{Array, AsArray},
    datatypes::{
        Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
};

pub mod arrow;
pub mod column;
pub mod compression;
pub mod datatype;
pub mod dimensions;
pub mod element;
pub mod group;
pub mod statistics;
pub mod util;

#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(serialize_bounds(
    __S: rkyv::ser::Writer + rkyv::ser::Allocator,
    __S::Error: rkyv::rancor::Source,
))]
#[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
#[rkyv(bytecheck(
    bounds(
        __C: rkyv::validation::ArchiveContext,
    )
))]
pub enum ArrayBuffer {
    RleEncoded {
        rl_buffer: Vec<u8>,
        rl_length: usize,
        #[rkyv(omit_bounds)]
        inner_array: Box<ArrayBuffer>,
    },
    Null(usize),
    Boolean {
        buffer: Vec<u8>,
        length: usize,
        null_buffer: Option<Vec<u8>>,
    },
    Int8 {
        buffer: Vec<u8>,
        length: usize,
        null_buffer: Option<Vec<u8>>,
    },
    Int16 {
        buffer: Vec<u8>,
        length: usize,
        null_buffer: Option<Vec<u8>>,
    },
    Int32 {
        buffer: Vec<u8>,
        length: usize,
        null_buffer: Option<Vec<u8>>,
    },
    Int64 {
        buffer: Vec<u8>,
        length: usize,
        null_buffer: Option<Vec<u8>>,
    },
    UInt8 {
        buffer: Vec<u8>,
        length: usize,
        null_buffer: Option<Vec<u8>>,
    },
    UInt16 {
        buffer: Vec<u8>,
        length: usize,
        null_buffer: Option<Vec<u8>>,
    },
    UInt32 {
        buffer: Vec<u8>,
        length: usize,
        null_buffer: Option<Vec<u8>>,
    },
    UInt64 {
        buffer: Vec<u8>,
        length: usize,
        null_buffer: Option<Vec<u8>>,
    },
    Float32 {
        buffer: Vec<u8>,
        length: usize,
        null_buffer: Option<Vec<u8>>,
    },
    Float64 {
        buffer: Vec<u8>,
        length: usize,
        null_buffer: Option<Vec<u8>>,
    },
    TimestampSeconds {
        buffer: Vec<u8>,
        length: usize,
        null_buffer: Option<Vec<u8>>,
    },
    TimestampMilliseconds {
        buffer: Vec<u8>,
        length: usize,
        null_buffer: Option<Vec<u8>>,
    },
    TimestampMicroseconds {
        buffer: Vec<u8>,
        length: usize,
        null_buffer: Option<Vec<u8>>,
    },
    TimestampNanoseconds {
        buffer: Vec<u8>,
        length: usize,
        null_buffer: Option<Vec<u8>>,
    },
    String {
        buffer: Vec<u8>,
        offsets_buffer: Vec<u8>,
        length: usize,
        null_buffer: Option<Vec<u8>>,
    },
    Binary {
        buffer: Vec<u8>,
        length: usize,
        null_buffer: Option<Vec<u8>>,
    },
}

impl ArrayBuffer {
    pub fn try_from_arrow(array: ::arrow::array::ArrayRef) -> Result<Self, String> {
        match array.data_type() {
            ::arrow::datatypes::DataType::Null => Ok(ArrayBuffer::Null(array.len())),
            ::arrow::datatypes::DataType::Boolean => {
                let boolean_array = array.as_boolean();
                let buffer = boolean_array.values().inner().to_vec();
                let length = boolean_array.len();
                let null_buffer = boolean_array.nulls().map(|b| b.inner().inner().to_vec());
                Ok(ArrayBuffer::Boolean {
                    buffer,
                    length,
                    null_buffer,
                })
            }
            ::arrow::datatypes::DataType::Int8 => {
                let int8_array = array.as_primitive::<Int8Type>();
                let buffer = int8_array.values().inner().to_vec();
                let length = int8_array.len();
                let null_buffer = int8_array.nulls().map(|b| b.inner().inner().to_vec());
                Ok(ArrayBuffer::Int8 {
                    buffer,
                    length,
                    null_buffer,
                })
            }
            ::arrow::datatypes::DataType::Int16 => {
                let int16_array = array.as_primitive::<Int16Type>();
                let buffer = int16_array.values().inner().to_vec();
                let length = int16_array.len();
                let null_buffer = int16_array.nulls().map(|b| b.inner().inner().to_vec());
                Ok(ArrayBuffer::Int16 {
                    buffer,
                    length,
                    null_buffer,
                })
            }
            ::arrow::datatypes::DataType::Int32 => {
                let int32_array = array.as_primitive::<Int32Type>();
                let buffer = int32_array.values().inner().to_vec();
                let length = int32_array.len();
                let null_buffer = int32_array.nulls().map(|b| b.inner().inner().to_vec());
                Ok(ArrayBuffer::Int32 {
                    buffer,
                    length,
                    null_buffer,
                })
            }
            ::arrow::datatypes::DataType::Int64 => {
                let int64_array = array.as_primitive::<Int64Type>();
                let buffer = int64_array.values().inner().to_vec();
                let length = int64_array.len();
                let null_buffer = int64_array.nulls().map(|b| b.inner().inner().to_vec());
                Ok(ArrayBuffer::Int64 {
                    buffer,
                    length,
                    null_buffer,
                })
            }
            ::arrow::datatypes::DataType::UInt8 => {
                let uint8_array = array.as_primitive::<UInt8Type>();
                let buffer = uint8_array.values().inner().to_vec();
                let length = uint8_array.len();
                let null_buffer = uint8_array.nulls().map(|b| b.inner().inner().to_vec());
                Ok(ArrayBuffer::UInt8 {
                    buffer,
                    length,
                    null_buffer,
                })
            }
            ::arrow::datatypes::DataType::UInt16 => {
                let uint16_array = array.as_primitive::<UInt16Type>();
                let buffer = uint16_array.values().inner().to_vec();
                let length = uint16_array.len();
                let null_buffer = uint16_array.nulls().map(|b| b.inner().inner().to_vec());
                Ok(ArrayBuffer::UInt16 {
                    buffer,
                    length,
                    null_buffer,
                })
            }
            ::arrow::datatypes::DataType::UInt32 => {
                let uint32_array = array.as_primitive::<UInt32Type>();
                let buffer = uint32_array.values().inner().to_vec();
                let length = uint32_array.len();
                let null_buffer = uint32_array.nulls().map(|b| b.inner().inner().to_vec());
                Ok(ArrayBuffer::UInt32 {
                    buffer,
                    length,
                    null_buffer,
                })
            }
            ::arrow::datatypes::DataType::UInt64 => {
                let uint64_array = array.as_primitive::<UInt64Type>();
                let buffer = uint64_array.values().inner().to_vec();
                let length = uint64_array.len();
                let null_buffer = uint64_array.nulls().map(|b| b.inner().inner().to_vec());
                Ok(ArrayBuffer::UInt64 {
                    buffer,
                    length,
                    null_buffer,
                })
            }
            ::arrow::datatypes::DataType::Float32 => {
                let float32_array = array.as_primitive::<Float32Type>();
                let buffer = float32_array.values().inner().to_vec();
                let length = float32_array.len();
                let null_buffer = float32_array.nulls().map(|b| b.inner().inner().to_vec());
                Ok(ArrayBuffer::Float32 {
                    buffer,
                    length,
                    null_buffer,
                })
            }
            ::arrow::datatypes::DataType::Float64 => {
                let float64_array = array.as_primitive::<Float64Type>();
                let buffer = float64_array.values().inner().to_vec();
                let length = float64_array.len();
                let null_buffer = float64_array.nulls().map(|b| b.inner().inner().to_vec());
                Ok(ArrayBuffer::Float64 {
                    buffer,
                    length,
                    null_buffer,
                })
            }

            ::arrow::datatypes::DataType::Timestamp(unit, _) => match unit {
                ::arrow::datatypes::TimeUnit::Second => {
                    let timestamp_second_array = array.as_primitive::<TimestampSecondType>();
                    let buffer = timestamp_second_array.values().inner().to_vec();
                    let length = timestamp_second_array.len();
                    let null_buffer = timestamp_second_array
                        .nulls()
                        .map(|b| b.inner().inner().to_vec());
                    Ok(ArrayBuffer::TimestampSeconds {
                        buffer,
                        length,
                        null_buffer,
                    })
                }
                ::arrow::datatypes::TimeUnit::Millisecond => {
                    let timestamp_millisecond_array =
                        array.as_primitive::<TimestampMillisecondType>();
                    let buffer = timestamp_millisecond_array.values().inner().to_vec();
                    let length = timestamp_millisecond_array.len();
                    let null_buffer = timestamp_millisecond_array
                        .nulls()
                        .map(|b| b.inner().inner().to_vec());
                    Ok(ArrayBuffer::TimestampMilliseconds {
                        buffer,
                        length,
                        null_buffer,
                    })
                }
                ::arrow::datatypes::TimeUnit::Microsecond => {
                    let timestamp_microsecond_array =
                        array.as_primitive::<TimestampMicrosecondType>();
                    let buffer = timestamp_microsecond_array.values().inner().to_vec();
                    let length = timestamp_microsecond_array.len();
                    let null_buffer = timestamp_microsecond_array
                        .nulls()
                        .map(|b| b.inner().inner().to_vec());
                    Ok(ArrayBuffer::TimestampMicroseconds {
                        buffer,
                        length,
                        null_buffer,
                    })
                }
                ::arrow::datatypes::TimeUnit::Nanosecond => {
                    let timestamp_nanosecond_array =
                        array.as_primitive::<TimestampNanosecondType>();
                    let buffer = timestamp_nanosecond_array.values().inner().to_vec();
                    let length = timestamp_nanosecond_array.len();
                    let null_buffer = timestamp_nanosecond_array
                        .nulls()
                        .map(|b| b.inner().inner().to_vec());
                    Ok(ArrayBuffer::TimestampNanoseconds {
                        buffer,
                        length,
                        null_buffer,
                    })
                }
            },
            ::arrow::datatypes::DataType::Utf8 => {
                let utf8_array = array.as_string::<i32>();
                let buffer = utf8_array.values().as_slice().to_vec();
                let offset_buffer = utf8_array.offsets().inner().inner().to_vec();
                let length = utf8_array.len();
                let null_buffer = utf8_array.nulls().map(|b| b.inner().inner().to_vec());
                Ok(ArrayBuffer::String {
                    buffer,
                    length,
                    offsets_buffer: offset_buffer,
                    null_buffer,
                })
            }
            ::arrow::datatypes::DataType::Binary => {
                let bin_array = array.as_binary::<i32>();
                let buffer = bin_array.values().as_slice().to_vec();
                let length = bin_array.len();
                let null_buffer = bin_array.nulls().map(|b| b.inner().inner().to_vec());
                Ok(ArrayBuffer::Binary {
                    buffer,
                    length,
                    null_buffer,
                })
            }
            ::arrow::datatypes::DataType::RunEndEncoded(run_field, value_field)
                if run_field.data_type() == &::arrow::datatypes::DataType::Int32 =>
            {
                let run_array = array.as_run::<Int32Type>();
                let rl_array = run_array.run_ends();

                let value_array = run_array.values().clone();
                let array_buffer = Self::try_from_arrow(value_array)?;

                Ok(ArrayBuffer::RleEncoded {
                    rl_buffer: rl_array.inner().inner().to_vec(),
                    rl_length: rl_array.inner().len(),
                    inner_array: Box::new(array_buffer),
                })
            }

            _ => Err("Unsupported arrow array type".into()),
        }
    }
}
