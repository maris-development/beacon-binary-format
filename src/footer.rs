use std::io::{Read, Seek, Write};

use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use rkyv::rancor;

use crate::{
    array::{
        column::ArrayColumn,
        datatype::DataType,
        util::{ArchivedStore, Store},
    },
    entry::EntryKey,
    index::Index,
    resolver::Resolver,
};

#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct Footer {
    pub version: String,
    pub schema: indexmap::IndexMap<String, DataType>,
    pub array_columns: indexmap::IndexMap<String, Resolver<ArrayColumn>>,
    pub indexes: indexmap::IndexMap<String, Resolver<Index>>,
    pub entries_deleted: Vec<bool>,
    pub entries: Vec<EntryKey>,
}

pub struct FooterUpdater {
    file: std::fs::File,
    footer: Footer,
    footer_position: u64,
}

impl FooterUpdater {
    pub fn new(mut file: std::fs::File) -> Result<Self, crate::error::BBFError> {
        // First read the magic bytes
        let magic_len = crate::consts::MAGIC_BYTES.len() as u64;
        // Read the first bytes to verify magic bytes
        let mut magic_bytes = vec![0u8; magic_len as usize];
        file.read_exact(&mut magic_bytes)?;

        if magic_bytes != crate::consts::MAGIC_BYTES {
            return Err(crate::error::BBFError::Reading(
                "File is not a valid BBF file (invalid magic bytes)".into(),
            ));
        }

        let file_size = file.metadata()?.len();
        let footer_len_start = file_size - magic_len - 8; // 8 bytes for footer length
        file.seek(std::io::SeekFrom::Start(footer_len_start))?;
        let footer_len = file.read_u64::<LE>()?;
        let footer_start = footer_len_start - footer_len;
        file.seek(std::io::SeekFrom::Start(footer_start))?;
        let mut footer_bytes = vec![0u8; footer_len as usize];
        file.read_exact(&mut footer_bytes)?;
        let store: &ArchivedStore<Footer> = unsafe { rkyv::access_unchecked(&footer_bytes) };

        let archived_footer = store.read();
        let footer =
            rkyv::deserialize::<_, rancor::Error>(archived_footer.as_ref()).map_err(|e| {
                crate::error::BBFError::Reading(
                    format!("Failed to deserialize footer: {}", e).into(),
                )
            })?;

        Ok(Self {
            file,
            footer,
            footer_position: footer_start,
        })
    }

    pub fn update_datatype<S: AsRef<str>>(
        &mut self,
        column_name: S,
        data_type: impl Into<DataType>,
    ) -> Result<(), crate::error::BBFError> {
        let column_name = column_name.as_ref();
        if let Some(dt) = self.footer.schema.get_mut(column_name) {
            *dt = data_type.into();
            Ok(())
        } else {
            Err(crate::error::BBFError::Writing(
                format!("Column '{}' not found in schema", column_name).into(),
            ))
        }
    }

    pub fn save(mut self) -> Result<(), crate::error::BBFError> {
        self.file
            .seek(std::io::SeekFrom::Start(self.footer_position))?;
        let footer_bytes = rkyv::to_bytes::<rancor::Error>(&Store::new(
            self.footer,
            Some(crate::array::compression::Compression::ZStd {
                level: 3,
                zdict: None,
            }),
        ))
        .map_err(|e| {
            crate::error::BBFError::Writing(format!("Failed to serialize footer: {}", e).into())
        })?;
        self.file.write_all(&footer_bytes)?;
        // Write footer length
        self.file.write_u64::<LE>(footer_bytes.len() as u64)?;
        // Write magic bytes at the end
        self.file.write_all(crate::consts::MAGIC_BYTES)?;
        self.file.flush()?;
        Ok(())
    }
}
