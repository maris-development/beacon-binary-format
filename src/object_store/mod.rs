use std::sync::Arc;

use bytes::Bytes;
use futures::{future::BoxFuture, FutureExt, TryFutureExt};
use object_store::ObjectStore;

use crate::{
    error::BBFError, reader::async_reader::AsyncRangeRead, writer::async_writer::AsyncStoreWrite,
};

pub struct ArrowBBFObjectWriter {
    store: Arc<dyn ObjectStore>,
    object_path: object_store::path::Path,
}

impl ArrowBBFObjectWriter {
    pub fn new(path: object_store::path::Path, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            object_path: path,
        }
    }
}

#[async_trait::async_trait]
impl AsyncStoreWrite for ArrowBBFObjectWriter {
    async fn write(&self, data: Bytes) -> Result<(), std::io::Error> {
        self.store
            .put(&self.object_path, data.into())
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl AsyncRangeRead for ArrowBBFObjectWriter {
    async fn size(&self) -> Result<u64, std::io::Error> {
        self.store
            .head(&self.object_path)
            .await
            .map(|meta| meta.size)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    async fn read_range(&self, offset: u64, size: u64) -> Result<bytes::Bytes, std::io::Error> {
        let bytes = self
            .store
            .get_range(&self.object_path, offset..offset + size)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        Ok(bytes)
    }
}

pub struct ArrowBBFObjectReader {
    store: Arc<dyn ObjectStore>,
    object_path: object_store::path::Path,
    runtime: Option<tokio::runtime::Handle>,
}

impl ArrowBBFObjectReader {
    pub fn new(path: object_store::path::Path, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            object_path: path,
            runtime: None,
        }
    }

    pub fn with_runtime(mut self, runtime: tokio::runtime::Handle) -> Self {
        // Set the runtime for async operations if needed
        // This is a placeholder; actual implementation may vary
        self.runtime = Some(runtime);
        self
    }

    /// Inspired from the ParquetObjectReader
    /// Spawns a future that executes the provided function with the object store and path.
    /// Returns a boxed future that resolves to the result of the function.
    /// This allows for asynchronous operations on the object store.
    #[allow(clippy::type_complexity)]
    fn spawn<F, O>(&self, f: F) -> BoxFuture<'_, Result<O, BBFError>>
    where
        F: for<'a> FnOnce(
                &'a Arc<dyn ObjectStore>,
                &'a object_store::path::Path,
            ) -> BoxFuture<'a, Result<O, BBFError>>
            + Send
            + 'static,
        O: Send + 'static,
    {
        match &self.runtime {
            Some(handle) => {
                let path = self.object_path.clone();
                let store = Arc::clone(&self.store);
                handle
                    .spawn(async move { f(&store, &path).await })
                    .map_ok_or_else(
                        |e| match e.try_into_panic() {
                            Err(e) => Err(BBFError::External(Box::new(e))),
                            Ok(p) => std::panic::resume_unwind(p),
                        },
                        |res| res.map_err(|e| e.into()),
                    )
                    .boxed()
            }
            None => f(&self.store, &self.object_path)
                .map_err(|e| e.into())
                .boxed(),
        }
    }
}

#[async_trait::async_trait]
impl AsyncRangeRead for ArrowBBFObjectReader {
    async fn size(&self) -> Result<u64, std::io::Error> {
        self.spawn(|store, path| {
            async move {
                store
                    .head(&path)
                    .await
                    .map(|meta| meta.size)
                    .map_err(|e| BBFError::ObjectStore(e))
            }
            .boxed()
        })
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        .await
    }

    async fn read_range(&self, offset: u64, size: u64) -> Result<bytes::Bytes, std::io::Error> {
        self.spawn(move |store, path| {
            async move {
                store
                    .get_range(&path, offset..offset + size)
                    .await
                    .map_err(|e| BBFError::ObjectStore(e))
            }
            .boxed()
        })
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        .await
    }
}
