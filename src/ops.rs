use std::{
    collections::HashSet,
    io::{Read, Seek, Write},
    sync::Arc,
};

use bytes::Bytes;
use rkyv::rancor;
use tempfile::tempfile;

use crate::{
    array::util::Store, entry::EntryKey, footer::Footer, reader::async_reader::AsyncBBFReader,
    writer::async_writer::AsyncStoreWrite,
};

pub async fn async_delete_entries(
    writer: Arc<dyn AsyncStoreWrite>,
    entries_to_delete: Vec<String>,
) -> Result<(), crate::error::BBFError> {
    let reader = AsyncBBFReader::new(writer.clone(), 32).await?;

    // Create a temp file containing the entire file
    let mut temp_file = tempfile()?;

    let full_size = reader.reader.size().await?;
    let chunk_size = 8 * 1024 * 1024; // 8 MB
    let mut offset = 0;

    while offset < full_size {
        let read_size = std::cmp::min(chunk_size as u64, full_size - offset) as usize;
        let data = reader
            .reader
            .read_range(offset, read_size as u64)
            .await
            .map_err(|e| {
                crate::error::BBFError::Reading(format!("Failed to read data: {}", e).into())
            })?;
        temp_file.write_all(&data).map_err(|e| {
            crate::error::BBFError::Writing(format!("Failed to write to temp file: {}", e).into())
        })?;
        offset += read_size as u64;
    }

    let footer_accessor = reader.footer();
    let footer = rkyv::deserialize::<_, rancor::Error>(footer_accessor.as_ref()).map_err(|e| {
        crate::error::BBFError::Reading(format!("Failed to deserialize footer: {}", e).into())
    })?;

    // Find the indexes of the entries to delete
    let entries_to_delete: HashSet<String> = entries_to_delete.into_iter().collect();
    let mut entries_deleted = footer.entries_deleted.clone();
    for (idx, entry) in footer.entries.iter().enumerate() {
        if entries_to_delete.contains(&entry.name) {
            entries_deleted[idx] = true;
        }
    }

    let new_footer = Footer {
        version: footer.version,
        schema: footer.schema,
        array_columns: footer.array_columns,
        indexes: footer.indexes,
        entries_deleted,
        entries: footer.entries,
    };

    // Serialize the new footer and write it to the file
    let mut footer_bytes = rkyv::to_bytes::<rancor::Error>(&Store::new(
        new_footer,
        Some(crate::array::compression::Compression::ZStd {
            level: 3,
            zdict: None,
        }),
    ))
    .map_err(|e| {
        crate::error::BBFError::Writing(format!("Failed to serialize footer: {}", e).into())
    })?
    .to_vec();

    // Write the length of the footer to the vec itself
    let footer_length = footer_bytes.len() as u64;
    footer_bytes.extend_from_slice(&footer_length.to_le_bytes());
    // Write magic bytes
    footer_bytes.extend_from_slice(crate::consts::MAGIC_BYTES);

    // Convert to Bytes and write
    let bytes = Bytes::from(footer_bytes);
    temp_file.write_all(&bytes).map_err(|e| {
        crate::error::BBFError::Writing(format!("Failed to write footer: {}", e).into())
    })?;

    // Now write the temp file back to the original writer
    temp_file.rewind().map_err(|e| {
        crate::error::BBFError::Reading(format!("Failed to rewind temp file: {}", e).into())
    })?;

    // Copy over the data from the temp file to the original writer
    let mut buffer = vec![];
    temp_file.read_to_end(&mut buffer).map_err(|e| {
        crate::error::BBFError::Reading(format!("Failed to read from temp file: {}", e).into())
    })?;

    writer.write(Bytes::from(buffer)).await.map_err(|e| {
        crate::error::BBFError::Writing(
            format!("Failed to write back to original writer: {}", e).into(),
        )
    })?;

    Ok(())
}
