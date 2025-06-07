//! Storage backend implementations for ROS2 bag files

use crate::error::Result;
use crate::types::{Connection, Message, MessageDefinition};
use std::collections::HashMap;
use std::path::Path;

pub mod sqlite;

#[cfg(feature = "mcap")]
pub mod mcap;

/// Trait for storage backend implementations
pub trait StorageReader {
    /// Open the storage files for reading
    fn open(&mut self) -> Result<()>;

    /// Close the storage files
    fn close(&mut self) -> Result<()>;

    /// Get message definitions from the storage
    fn get_definitions(&self) -> Result<HashMap<String, MessageDefinition>>;

    /// Iterate over messages, optionally filtered by connections, start time, and stop time
    fn messages(
        &self,
        connections: Option<&[Connection]>,
        start: Option<u64>,
        stop: Option<u64>,
    ) -> Result<Box<dyn Iterator<Item = Result<Message>> + '_>>;

    /// Check if the storage is currently open
    fn is_open(&self) -> bool;

    /// Get a reference to the concrete type for downcasting
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Create a storage reader for the given storage identifier
pub fn create_storage_reader(
    storage_id: &str,
    paths: Vec<&Path>,
    connections: Vec<Connection>,
) -> Result<Box<dyn StorageReader>> {
    match storage_id {
        "sqlite3" => Ok(Box::new(sqlite::SqliteReader::new(paths, connections)?)),
        #[cfg(feature = "mcap")]
        "mcap" => Ok(Box::new(mcap::McapStorageReader::new(paths, connections)?)),
        "" => {
            // Auto-detect storage format from file extensions when storage_identifier is empty
            let has_db3 = paths.iter().any(|path| path.extension().map_or(false, |ext| ext == "db3"));
            let has_mcap = paths.iter().any(|path| path.extension().map_or(false, |ext| ext == "mcap"));

            if has_db3 {
                Ok(Box::new(sqlite::SqliteReader::new(paths, connections)?))
            } else if has_mcap {
                #[cfg(feature = "mcap")]
                {
                    Ok(Box::new(mcap::McapStorageReader::new(paths, connections)?))
                }
                #[cfg(not(feature = "mcap"))]
                {
                    Err(crate::error::ReaderError::UnsupportedStorageFormat {
                        format: "mcap (feature not enabled)".to_string(),
                    })
                }
            } else {
                Err(crate::error::ReaderError::UnsupportedStorageFormat {
                    format: "unknown (no .db3 or .mcap files found)".to_string(),
                })
            }
        }
        _ => Err(crate::error::ReaderError::UnsupportedStorageFormat {
            format: storage_id.to_string(),
        }),
    }
}
