use std::sync::Arc;

use arrow::array::{
    ArrayRef, AsArray, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, Scalar, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};

use crate::entry::Column;

#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub null_count: usize,
    pub row_count: usize,
    pub min_value: Option<ArrayRef>,
    pub max_value: Option<ArrayRef>,
}

impl ColumnStatistics {
    pub(crate) fn new(
        null_count: usize,
        row_count: usize,
        min_value: Option<ArrayRef>,
        max_value: Option<ArrayRef>,
    ) -> Self {
        Self {
            null_count,
            row_count,
            min_value,
            max_value,
        }
    }

    pub fn from_column(column: &Column) -> Self {
        let array = column.array();

        Self::calculate_from(Some(array.clone()))
    }

    pub fn calculate_from(array: Option<ArrayRef>) -> Self {
        if let Some(array) = array {
            let null_count = array.null_count();
            let row_count = array.len();
            let min_value = Self::calculate_min(&array).map(|scalar| scalar.into_inner());
            let max_value = Self::calculate_max(&array).map(|scalar| scalar.into_inner());

            Self {
                null_count,
                row_count,
                min_value,
                max_value,
            }
        } else {
            Self {
                null_count: 0,
                row_count: 0,
                min_value: None,
                max_value: None,
            }
        }
    }

    fn calculate_max(array: &ArrayRef) -> Option<Scalar<ArrayRef>> {
        let mut max_value: Option<Scalar<ArrayRef>> = None;

        match array.data_type() {
            arrow::datatypes::DataType::Null => {}
            arrow::datatypes::DataType::Boolean => {
                let max = arrow::compute::max_boolean(array.as_boolean());
                max_value = match max {
                    Some(value) => Some(Scalar::new(Arc::new(BooleanArray::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Int8 => {
                let max = arrow::compute::max(array.as_primitive::<arrow::datatypes::Int8Type>());
                max_value = match max {
                    Some(value) => Some(Scalar::new(Arc::new(Int8Array::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Int16 => {
                let max = arrow::compute::max(array.as_primitive::<arrow::datatypes::Int16Type>());
                max_value = match max {
                    Some(value) => Some(Scalar::new(Arc::new(Int16Array::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Int32 => {
                let max = arrow::compute::max(array.as_primitive::<arrow::datatypes::Int32Type>());
                max_value = match max {
                    Some(value) => Some(Scalar::new(Arc::new(Int32Array::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Int64 => {
                let max = arrow::compute::max(array.as_primitive::<arrow::datatypes::Int64Type>());
                max_value = match max {
                    Some(value) => Some(Scalar::new(Arc::new(Int64Array::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::UInt8 => {
                let max = arrow::compute::max(array.as_primitive::<arrow::datatypes::UInt8Type>());
                max_value = match max {
                    Some(value) => Some(Scalar::new(Arc::new(UInt8Array::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::UInt16 => {
                let max = arrow::compute::max(array.as_primitive::<arrow::datatypes::UInt16Type>());
                max_value = match max {
                    Some(value) => Some(Scalar::new(Arc::new(UInt16Array::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::UInt32 => {
                let max = arrow::compute::max(array.as_primitive::<arrow::datatypes::UInt32Type>());
                max_value = match max {
                    Some(value) => Some(Scalar::new(Arc::new(UInt32Array::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::UInt64 => {
                let max = arrow::compute::max(array.as_primitive::<arrow::datatypes::UInt64Type>());
                max_value = match max {
                    Some(value) => Some(Scalar::new(Arc::new(UInt64Array::from(vec![value])))),
                    None => None,
                };
            }

            arrow::datatypes::DataType::Float32 => {
                let max =
                    arrow::compute::max(array.as_primitive::<arrow::datatypes::Float32Type>());
                max_value = match max {
                    Some(value) => Some(Scalar::new(Arc::new(Float32Array::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Float64 => {
                let max =
                    arrow::compute::max(array.as_primitive::<arrow::datatypes::Float64Type>());
                max_value = match max {
                    Some(value) => Some(Scalar::new(Arc::new(Float64Array::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Second, None) => {
                let max = arrow::compute::max(
                    array.as_primitive::<arrow::datatypes::TimestampSecondType>(),
                );
                max_value = match max {
                    Some(value) => Some(Scalar::new(Arc::new(TimestampSecondArray::from(vec![
                        value,
                    ])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None) => {
                let max = arrow::compute::max(
                    array.as_primitive::<arrow::datatypes::TimestampMillisecondType>(),
                );
                max_value = match max {
                    Some(value) => Some(Scalar::new(Arc::new(TimestampMillisecondArray::from(
                        vec![value],
                    )))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None) => {
                let max = arrow::compute::max(
                    array.as_primitive::<arrow::datatypes::TimestampMicrosecondType>(),
                );
                max_value = match max {
                    Some(value) => Some(Scalar::new(Arc::new(TimestampMicrosecondArray::from(
                        vec![value],
                    )))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None) => {
                let max = arrow::compute::max(
                    array.as_primitive::<arrow::datatypes::TimestampNanosecondType>(),
                );
                max_value = match max {
                    Some(value) => {
                        Some(Scalar::new(Arc::new(TimestampNanosecondArray::from(vec![
                            value,
                        ]))))
                    }
                    None => None,
                };
            }
            arrow::datatypes::DataType::Utf8 => {
                let max = arrow::compute::max_string(array.as_string::<i32>());
                max_value = match max {
                    Some(value) => Some(Scalar::new(Arc::new(StringArray::from(vec![
                        value.to_string()
                    ])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Binary => {
                let max = arrow::compute::max_binary(array.as_binary::<i32>());
                max_value = match max {
                    Some(value) => Some(Scalar::new(Arc::new(
                        BinaryArray::new_scalar(value.to_vec()).into_inner(),
                    ))),
                    None => None,
                };
            }
            _ => { /* Other types not implemented yet */ }
        }

        max_value
    }

    fn calculate_min(array: &ArrayRef) -> Option<Scalar<ArrayRef>> {
        let mut min_value: Option<Scalar<ArrayRef>> = None;

        match array.data_type() {
            arrow::datatypes::DataType::Null => {}
            arrow::datatypes::DataType::Boolean => {
                let min = arrow::compute::min_boolean(array.as_boolean());
                min_value = match min {
                    Some(value) => Some(Scalar::new(Arc::new(BooleanArray::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Int8 => {
                let min = arrow::compute::min(array.as_primitive::<arrow::datatypes::Int8Type>());
                min_value = match min {
                    Some(value) => Some(Scalar::new(Arc::new(Int8Array::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Int16 => {
                let min = arrow::compute::min(array.as_primitive::<arrow::datatypes::Int16Type>());
                min_value = match min {
                    Some(value) => Some(Scalar::new(Arc::new(Int16Array::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Int32 => {
                let min = arrow::compute::min(array.as_primitive::<arrow::datatypes::Int32Type>());
                min_value = match min {
                    Some(value) => Some(Scalar::new(Arc::new(Int32Array::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Int64 => {
                let min = arrow::compute::min(array.as_primitive::<arrow::datatypes::Int64Type>());
                min_value = match min {
                    Some(value) => Some(Scalar::new(Arc::new(Int64Array::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::UInt8 => {
                let min = arrow::compute::min(array.as_primitive::<arrow::datatypes::UInt8Type>());
                min_value = match min {
                    Some(value) => Some(Scalar::new(Arc::new(UInt8Array::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::UInt16 => {
                let min = arrow::compute::min(array.as_primitive::<arrow::datatypes::UInt16Type>());
                min_value = match min {
                    Some(value) => Some(Scalar::new(Arc::new(UInt16Array::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::UInt32 => {
                let min = arrow::compute::min(array.as_primitive::<arrow::datatypes::UInt32Type>());
                min_value = match min {
                    Some(value) => Some(Scalar::new(Arc::new(UInt32Array::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::UInt64 => {
                let min = arrow::compute::min(array.as_primitive::<arrow::datatypes::UInt64Type>());
                min_value = match min {
                    Some(value) => Some(Scalar::new(Arc::new(UInt64Array::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Float32 => {
                let min =
                    arrow::compute::min(array.as_primitive::<arrow::datatypes::Float32Type>());
                min_value = match min {
                    Some(value) => Some(Scalar::new(Arc::new(Float32Array::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Float64 => {
                let min =
                    arrow::compute::min(array.as_primitive::<arrow::datatypes::Float64Type>());
                min_value = match min {
                    Some(value) => Some(Scalar::new(Arc::new(Float64Array::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Second, None) => {
                let min = arrow::compute::min(
                    array.as_primitive::<arrow::datatypes::TimestampSecondType>(),
                );
                min_value = match min {
                    Some(value) => Some(Scalar::new(Arc::new(TimestampSecondArray::from(vec![
                        value,
                    ])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None) => {
                let min = arrow::compute::min(
                    array.as_primitive::<arrow::datatypes::TimestampMillisecondType>(),
                );
                min_value = match min {
                    Some(value) => Some(Scalar::new(Arc::new(TimestampMillisecondArray::from(
                        vec![value],
                    )))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None) => {
                let min = arrow::compute::min(
                    array.as_primitive::<arrow::datatypes::TimestampMicrosecondType>(),
                );
                min_value = match min {
                    Some(value) => Some(Scalar::new(Arc::new(TimestampMicrosecondArray::from(
                        vec![value],
                    )))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None) => {
                let min = arrow::compute::min(
                    array.as_primitive::<arrow::datatypes::TimestampNanosecondType>(),
                );
                min_value = match min {
                    Some(value) => {
                        Some(Scalar::new(Arc::new(TimestampNanosecondArray::from(vec![
                            value,
                        ]))))
                    }
                    None => None,
                };
            }
            arrow::datatypes::DataType::Utf8 => {
                let min = arrow::compute::min_string(array.as_string::<i32>());
                min_value = match min {
                    Some(value) => Some(Scalar::new(Arc::new(StringArray::from(vec![value])))),
                    None => None,
                };
            }
            arrow::datatypes::DataType::Binary => {
                let min = arrow::compute::min_binary(array.as_binary::<i32>());
                min_value = match min {
                    Some(value) => Some(Scalar::new(Arc::new(
                        BinaryArray::new_scalar(value.to_vec()).into_inner(),
                    ))),
                    None => None,
                };
            }
            _ => { /* Other types not implemented yet */ }
        }

        min_value
    }

    pub fn null_count(&self) -> usize {
        self.null_count
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn min_value(&self) -> Option<&ArrayRef> {
        self.min_value.as_ref()
    }

    pub fn max_value(&self) -> Option<&ArrayRef> {
        self.max_value.as_ref()
    }

    pub fn arrow_data_type(&self) -> Option<&arrow::datatypes::DataType> {
        self.min_value.as_ref().map(|v| v.data_type())
    }

    pub fn cast(&mut self, data_type: &arrow::datatypes::DataType) {
        if data_type != self.arrow_data_type().unwrap_or(data_type) {
            // Cast the statistics to the correct type
            let cast_options = arrow::compute::CastOptions::default();
            self.min_value = self.min_value.as_ref().and_then(|array| {
                arrow::compute::cast_with_options(array, data_type, &cast_options).ok()
            });
            self.max_value = self.max_value.as_ref().and_then(|array| {
                arrow::compute::cast_with_options(array, data_type, &cast_options).ok()
            });
        }
    }
}
