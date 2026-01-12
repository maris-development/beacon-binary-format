use std::sync::Arc;

use arrow::{
    array::{Array, BooleanArray, PrimitiveArray, RunArray},
    datatypes::Int32Type,
};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};

use crate::array::{util::flatten_run_array, ArchivedArrayBuffer};

pub fn to_arrow_array(array: &ArchivedArrayBuffer) -> Arc<dyn Array> {
    match array {
        ArchivedArrayBuffer::Null(len) => {
            Arc::new(arrow::array::NullArray::new(len.to_native() as usize))
        }
        ArchivedArrayBuffer::Boolean {
            buffer,
            length,
            null_buffer,
        } => {
            let arrow_buffer = init_bool_buffer(buffer.as_slice(), length.to_native() as usize);
            let null_buffer = null_buffer
                .as_ref()
                .map(|nb| init_null_buffer(nb.as_slice(), length.to_native() as usize));
            Arc::new(BooleanArray::new(arrow_buffer, null_buffer))
        }
        ArchivedArrayBuffer::UInt8 {
            buffer,
            length,
            null_buffer,
        } => {
            let arrow_buffer = arrow::buffer::Buffer::from_slice_ref(buffer.as_slice());
            let scalar_buffer = ScalarBuffer::new(arrow_buffer, 0, length.to_native() as usize);
            let null_buffer = null_buffer
                .as_ref()
                .map(|nb| init_null_buffer(nb.as_slice(), length.to_native() as usize));
            Arc::new(arrow::array::UInt8Array::new(scalar_buffer, null_buffer))
        }
        ArchivedArrayBuffer::UInt16 {
            buffer,
            length,
            null_buffer,
        } => {
            let arrow_buffer = arrow::buffer::Buffer::from_slice_ref(buffer.as_slice());
            let scalar_buffer = ScalarBuffer::new(arrow_buffer, 0, length.to_native() as usize);
            let null_buffer = null_buffer
                .as_ref()
                .map(|nb| init_null_buffer(nb.as_slice(), length.to_native() as usize));
            Arc::new(arrow::array::UInt16Array::new(scalar_buffer, null_buffer))
        }
        ArchivedArrayBuffer::UInt32 {
            buffer,
            length,
            null_buffer,
        } => {
            let arrow_buffer = arrow::buffer::Buffer::from_slice_ref(buffer.as_slice());
            let scalar_buffer = ScalarBuffer::new(arrow_buffer, 0, length.to_native() as usize);
            let null_buffer = null_buffer
                .as_ref()
                .map(|nb| init_null_buffer(nb.as_slice(), length.to_native() as usize));
            Arc::new(arrow::array::UInt32Array::new(scalar_buffer, null_buffer))
        }
        ArchivedArrayBuffer::UInt64 {
            buffer,
            length,
            null_buffer,
        } => {
            let arrow_buffer = arrow::buffer::Buffer::from_slice_ref(buffer.as_slice());
            let scalar_buffer = ScalarBuffer::new(arrow_buffer, 0, length.to_native() as usize);
            let null_buffer = null_buffer
                .as_ref()
                .map(|nb| init_null_buffer(nb.as_slice(), length.to_native() as usize));
            Arc::new(arrow::array::UInt64Array::new(scalar_buffer, null_buffer))
        }
        ArchivedArrayBuffer::Int8 {
            buffer,
            length,
            null_buffer,
        } => {
            let arrow_buffer = arrow::buffer::Buffer::from_slice_ref(buffer.as_slice());
            let scalar_buffer = ScalarBuffer::new(arrow_buffer, 0, length.to_native() as usize);
            let null_buffer = null_buffer
                .as_ref()
                .map(|nb| init_null_buffer(nb.as_slice(), length.to_native() as usize));
            Arc::new(arrow::array::Int8Array::new(scalar_buffer, null_buffer))
        }
        ArchivedArrayBuffer::Int16 {
            buffer,
            length,
            null_buffer,
        } => {
            let arrow_buffer = arrow::buffer::Buffer::from_slice_ref(buffer.as_slice());
            let scalar_buffer = ScalarBuffer::new(arrow_buffer, 0, length.to_native() as usize);
            let null_buffer = null_buffer
                .as_ref()
                .map(|nb| init_null_buffer(nb.as_slice(), length.to_native() as usize));
            Arc::new(arrow::array::Int16Array::new(scalar_buffer, null_buffer))
        }
        ArchivedArrayBuffer::Int32 {
            buffer,
            length,
            null_buffer,
        } => {
            let arrow_buffer = arrow::buffer::Buffer::from_slice_ref(buffer.as_slice());
            let scalar_buffer = ScalarBuffer::new(arrow_buffer, 0, length.to_native() as usize);
            let null_buffer = null_buffer
                .as_ref()
                .map(|nb| init_null_buffer(nb.as_slice(), length.to_native() as usize));
            Arc::new(arrow::array::Int32Array::new(scalar_buffer, null_buffer))
        }
        ArchivedArrayBuffer::Int64 {
            buffer,
            length,
            null_buffer,
        } => {
            let arrow_buffer = arrow::buffer::Buffer::from_slice_ref(buffer.as_slice());
            let scalar_buffer = ScalarBuffer::new(arrow_buffer, 0, length.to_native() as usize);
            let null_buffer = null_buffer
                .as_ref()
                .map(|nb| init_null_buffer(nb.as_slice(), length.to_native() as usize));
            Arc::new(arrow::array::Int64Array::new(scalar_buffer, null_buffer))
        }
        ArchivedArrayBuffer::Float32 {
            buffer,
            length,
            null_buffer,
        } => {
            let arrow_buffer = arrow::buffer::Buffer::from_slice_ref(buffer.as_slice());
            let scalar_buffer = ScalarBuffer::new(arrow_buffer, 0, length.to_native() as usize);
            let null_buffer = null_buffer
                .as_ref()
                .map(|nb| init_null_buffer(nb.as_slice(), length.to_native() as usize));
            Arc::new(arrow::array::Float32Array::new(scalar_buffer, null_buffer))
        }
        ArchivedArrayBuffer::Float64 {
            buffer,
            length,
            null_buffer,
        } => {
            let arrow_buffer = arrow::buffer::Buffer::from_slice_ref(buffer.as_slice());
            let scalar_buffer = ScalarBuffer::new(arrow_buffer, 0, length.to_native() as usize);
            let null_buffer = null_buffer
                .as_ref()
                .map(|nb| init_null_buffer(nb.as_slice(), length.to_native() as usize));
            Arc::new(arrow::array::Float64Array::new(scalar_buffer, null_buffer))
        }
        ArchivedArrayBuffer::TimestampSeconds {
            buffer,
            length,
            null_buffer,
        } => {
            let arrow_buffer = arrow::buffer::Buffer::from_slice_ref(buffer.as_slice());
            let scalar_buffer = ScalarBuffer::new(arrow_buffer, 0, length.to_native() as usize);
            let null_buffer = null_buffer
                .as_ref()
                .map(|nb| init_null_buffer(nb.as_slice(), length.to_native() as usize));
            Arc::new(arrow::array::TimestampSecondArray::new(
                scalar_buffer,
                null_buffer,
            ))
        }
        ArchivedArrayBuffer::TimestampMilliseconds {
            buffer,
            length,
            null_buffer,
        } => {
            let arrow_buffer = arrow::buffer::Buffer::from_slice_ref(buffer.as_slice());
            let scalar_buffer = ScalarBuffer::new(arrow_buffer, 0, length.to_native() as usize);
            let null_buffer = null_buffer
                .as_ref()
                .map(|nb| init_null_buffer(nb.as_slice(), length.to_native() as usize));
            Arc::new(arrow::array::TimestampMillisecondArray::new(
                scalar_buffer,
                null_buffer,
            ))
        }
        ArchivedArrayBuffer::TimestampMicroseconds {
            buffer,
            length,
            null_buffer,
        } => {
            let arrow_buffer = arrow::buffer::Buffer::from_slice_ref(buffer.as_slice());
            let scalar_buffer = ScalarBuffer::new(arrow_buffer, 0, length.to_native() as usize);
            let null_buffer = null_buffer
                .as_ref()
                .map(|nb| init_null_buffer(nb.as_slice(), length.to_native() as usize));
            Arc::new(arrow::array::TimestampMicrosecondArray::new(
                scalar_buffer,
                null_buffer,
            ))
        }
        ArchivedArrayBuffer::TimestampNanoseconds {
            buffer,
            length,
            null_buffer,
        } => {
            let arrow_buffer = arrow::buffer::Buffer::from_slice_ref(buffer.as_slice());
            let scalar_buffer = ScalarBuffer::new(arrow_buffer, 0, length.to_native() as usize);
            let null_buffer = null_buffer
                .as_ref()
                .map(|nb| init_null_buffer(nb.as_slice(), length.to_native() as usize));
            Arc::new(arrow::array::TimestampNanosecondArray::new(
                scalar_buffer,
                null_buffer,
            ))
        }
        ArchivedArrayBuffer::String {
            buffer,
            length,
            offsets_buffer,
            null_buffer,
        } => {
            let arrow_buffer = arrow::buffer::Buffer::from_slice_ref(buffer.as_slice());
            let arrow_offsets = arrow::buffer::Buffer::from_slice_ref(offsets_buffer.as_slice());
            let offset_buffer = OffsetBuffer::new(ScalarBuffer::new(
                arrow_offsets,
                0,
                length.to_native() as usize + 1,
            ));
            let null_buffer = null_buffer
                .as_ref()
                .map(|nb| init_null_buffer(nb.as_slice(), length.to_native() as usize));
            Arc::new(arrow::array::StringArray::new(
                offset_buffer,
                arrow_buffer,
                null_buffer,
            ))
        }
        ArchivedArrayBuffer::Binary {
            buffer,
            length,
            null_buffer,
        } => {
            let arrow_buffer = arrow::buffer::Buffer::from_slice_ref(buffer.as_slice());
            let arrow_offsets = arrow::buffer::Buffer::from_slice_ref(buffer.as_slice());
            let offset_buffer = OffsetBuffer::new(ScalarBuffer::new(
                arrow_offsets,
                0,
                length.to_native() as usize + 1,
            ));
            let null_buffer = null_buffer
                .as_ref()
                .map(|nb| init_null_buffer(nb.as_slice(), length.to_native() as usize));
            Arc::new(arrow::array::BinaryArray::new(
                offset_buffer,
                arrow_buffer,
                null_buffer,
            ))
        }
        ArchivedArrayBuffer::RleEncoded {
            rl_buffer,
            rl_length,
            inner_array,
        } => {
            let arrow_buffer = arrow::buffer::Buffer::from_slice_ref(rl_buffer.as_slice());
            let scalar_buffer: ScalarBuffer<i32> =
                ScalarBuffer::new(arrow_buffer, 0, rl_length.to_native() as usize);
            let run_ends = PrimitiveArray::new(scalar_buffer, None);

            let values_array = to_arrow_array(inner_array);

            let array: RunArray<Int32Type> = RunArray::try_new(&run_ends, &values_array).unwrap();

            Arc::new(flatten_run_array(&array))
        }
        _ => {
            todo!()
        }
    }
}

fn init_null_buffer(bytes: &[u8], length: usize) -> NullBuffer {
    let bool_buffer = init_bool_buffer(bytes, length);
    NullBuffer::new(bool_buffer)
}

fn init_bool_buffer(bytes: &[u8], length: usize) -> BooleanBuffer {
    let arrow_buffer = arrow::buffer::Buffer::from_slice_ref(bytes);
    BooleanBuffer::new(arrow_buffer, 0, length)
}
