pub mod array;
pub mod consts;
pub mod entry;
pub mod error;
pub mod footer;
pub mod index;
pub mod object_store;
pub mod ops;
pub mod reader;
pub mod resolver;
pub mod util;
pub mod writer;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{AsArray, Int32Array, StringArray},
        util::display::FormatOptions,
    };
    use arrow_schema::DataType;
    use futures::{FutureExt, StreamExt, TryStreamExt};
    use tempfile::NamedTempFile;

    use crate::{
        entry::{ArrayCollection, Column, Entry},
        index::{pruning::PruningIndexReader, ArchivedIndex},
        reader::{
            async_reader::{AsyncChunkReader, AsyncRangeRead},
            RangeRead,
        },
    };

    use super::*;

    // #[test]
    // fn test_s() {
    //     let null_nd = new_null_nd_arrow_array(DataType::Null);

    //     println!("Null NdArrowArray: {:?}", null_nd.to_arrow_array().unwrap());

    //     let casted = null_nd
    //         .cast(arrow::datatypes::DataType::Int32, None)
    //         .expect("Failed to cast null array to Int32");

    //     println!(
    //         "Casted NdArrowArray: {:?}",
    //         casted.to_arrow_array().unwrap()
    //     );
    // }

    #[tokio::test]
    async fn test_name() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let mut writer = writer::BBFWriter::new(
            file.path(),
            1024 * 1024,
            Some(array::compression::Compression::zstd(1)),
            true,
        )
        .unwrap();

        let column = Column::new(
            "test",
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            array::dimensions::Dimensions::Multi(vec![("dim1", 3).into()]),
        );

        let column_collection =
            ArrayCollection::new("test_collection", Box::new(std::iter::once(column)));

        let entry = Entry::new(column_collection);

        writer.append(entry, "test_entry".to_string());

        let column = Column::new(
            "test",
            Arc::new(StringArray::from(vec!["-1", "2", "3", "4"])),
            array::dimensions::Dimensions::Multi(vec![("dim1", 4).into()]),
        );

        let column_2 = Column::new(
            "test2",
            Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
            array::dimensions::Dimensions::Multi(vec![("dim1", 4).into()]),
        );

        let column_collection = ArrayCollection::new(
            "test_collection2",
            Box::new(vec![column, column_2].into_iter()),
        );

        let entry = Entry::new(column_collection);

        writer.append(entry, "test_entry2".to_string());

        writer.finish().unwrap();
        let (file, path) = file.into_parts();

        println!("Size of file: {}", file.metadata().unwrap().len());

        let file = Arc::new(Arc::new(file)) as Arc<dyn AsyncRangeRead>;

        let reader = reader::async_reader::AsyncBBFReader::new(file.clone(), 32)
            .await
            .unwrap();

        println!("Schema: {:?}", reader.arrow_schema());

        let index_reader = reader.pruning_index().await.unwrap();

        let statistics = index_reader.column("test").await.unwrap().unwrap();
        println!("Num Containers: {:?}", index_reader.num_containers());
        println!("Row Count: {:?}", statistics.as_ref().row_count());
        println!("Null Count: {:?}", statistics.as_ref().null_count());
        println!("Min Value: {:?}", statistics.as_ref().min_value());
        println!("Max Value: {:?}", statistics.as_ref().max_value());

        let producer = reader.read(None, None).await.unwrap();

        let stream = producer.stream().await;
        let mut async_stream = Box::pin(stream.into_stream());

        while let Some(item) = async_stream.next().await {
            println!("Received item: {:?}", item);
            println!(
                "{}",
                arrow::util::pretty::pretty_format_batches_with_options(
                    &[item.to_arrow_record_batch().unwrap()],
                    &FormatOptions::new().with_types_info(true)
                )
                .unwrap()
            );
        }

        drop(path);
    }

    // #[cfg(test)]
    // mod tests {
    //     use arrow::{
    //         array::{Array, ListArray, ListBuilder, PrimitiveBuilder},
    //         datatypes::Int32Type,
    //     };
    //     use arrow_schema::Field;

    //     use super::*;

    //     #[test]
    //     fn test_name() {
    //         let encoded_arrow =
    //             new_from_arrow_array(Arc::new(Int32Array::from(vec![1, 2, 3])), vec![("dim1", 3)]);

    //         println!(
    //             "Size of encoded arrow: {}",
    //             encoded_arrow
    //                 .to_arrow_array()
    //                 .unwrap()
    //                 .get_array_memory_size()
    //         );

    //         let mut list_builder = ListBuilder::new(PrimitiveBuilder::<Int32Type>::new());
    //         list_builder.values().append_value(1);
    //         list_builder.values().append_value(2);
    //         list_builder.values().append_value(3);
    //         list_builder.append(true);

    //         let list_array = list_builder.finish();
    //         println!("Size of list array: {}", list_array.get_array_memory_size());
    //     }
    // }
}
