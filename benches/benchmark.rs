use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use ::object_store::local::LocalFileSystem;
use beacon_binary_format::{
    array::compression::Compression::{self, Lz4},
    entry::{ArrayCollection, Column, Entry},
    object_store::ArrowBBFObjectReader,
    reader::async_reader::AsyncBBFReader,
    writer::BBFWriter,
    *,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use futures::{stream, StreamExt};
use rand::Rng;
use tempfile::NamedTempFile;
use tokio::runtime::Runtime;

fn generate_random_entry() -> Entry {
    let num_columns = 6;
    let num_elements = 100_000;

    let name = format!("name_{}", rand::thread_rng().gen::<u64>());
    let mut arrays = vec![];

    for i in 0..num_columns {
        let column_name = format!("column_{}", i);
        let mut array = Vec::with_capacity(num_elements);
        for _ in 0..num_elements {
            array.push(rand::thread_rng().gen_range::<i32, _>(0..100));
        }

        let column = Column::new(
            column_name,
            Arc::new(arrow::array::Int32Array::from(array)),
            array::dimensions::Dimensions::Multi(vec![("dim1", num_elements).into()]),
        );

        arrays.push(column);
    }

    let collection = ArrayCollection::new(name, Box::new(arrays.into_iter()));

    Entry::new(collection)
}

fn build_benchmark_file() -> NamedTempFile {
    let file = NamedTempFile::new_in("./").unwrap();
    let num_entries = 2500;

    let mut writer = BBFWriter::new(
        file.path(),
        4 * 1024 * 1024,
        Some(Compression::zstd(1)),
        false,
    )
    .unwrap();

    for _ in 0..num_entries {
        let arrays = generate_random_entry();
        let key = format!("entry_{}", rand::thread_rng().gen::<u64>());
        writer.append(arrays, key);
    }

    writer.finish().unwrap();

    file
}

async fn benchmark_file(object_reader: ArrowBBFObjectReader) {
    let reader = AsyncBBFReader::new(Arc::new(object_reader), 0)
        .await
        .unwrap();
    let time = std::time::Instant::now();
    let entry_reader = reader
        .read(Some(vec![0]), None)
        .await
        .unwrap()
        .stream()
        .await;

    let mut streams = vec![];
    for _ in 0..12 {
        let stream = entry_reader.clone();
        streams.push(stream);
    }

    let counter = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];
    for stream in streams {
        let counter = Arc::clone(&counter);
        let handle = tokio::spawn(async move {
            let mut stream = Box::pin(stream.stream());
            while let Some(nd_batch) = stream.next().await {
                let arrow_batch = nd_batch.to_arrow_record_batch().unwrap();
                counter.fetch_add(arrow_batch.num_rows(), Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    futures::future::join_all(handles).await;
    println!(
        "Benchmark completed in {:?} with total size: {}",
        time.elapsed(),
        counter.load(Ordering::SeqCst)
    );

    // println!("Total found: {}", counter.load(Ordering::SeqCst));
}

fn criterion_benchmark(c: &mut Criterion) {
    let file = build_benchmark_file();

    let local_store = Arc::new(LocalFileSystem::new());
    let object_path = ::object_store::path::Path::from_filesystem_path(file.path()).unwrap();

    println!("Benchmarking file: {:?}", file.path());
    println!("Object path: {:?}", object_path);
    let mut group = c.benchmark_group("bbf_large");
    group.sample_size(25);
    group.bench_function("bbf large", |b| {
        let mut runtime = tokio::runtime::Builder::new_multi_thread();
        runtime.enable_all();
        let runtime = runtime.build().unwrap();
        b.to_async(runtime).iter(|| {
            let object_reader = ArrowBBFObjectReader::new(object_path.clone(), local_store.clone());
            benchmark_file(object_reader)
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
