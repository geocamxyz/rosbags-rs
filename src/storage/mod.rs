//! Storage backend implementations for ROS2 bag files

use crate::error::Result;
use crate::types::{CompressionMode, Connection, Message, MessageDefinition, StoragePlugin};
use std::collections::HashMap;
use std::path::Path;

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "mcap")]
pub mod mcap;

/// Trait for storage backend implementations (reading)
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

/// Trait for storage backend implementations (writing)
pub trait StorageWriter {
    /// Open the storage files for writing
    fn open(&mut self) -> Result<()>;

    /// Close the storage files and write any remaining data
    fn close(&mut self, version: u32, metadata: &str) -> Result<()>;

    /// Add a message type definition to the storage
    fn add_msgtype(&mut self, connection: &Connection) -> Result<()>;

    /// Add a connection (topic) to the storage
    fn add_connection(&mut self, connection: &Connection, offered_qos_profiles: &str)
        -> Result<()>;

    /// Write a message to the storage
    fn write(&mut self, connection: &Connection, timestamp: u64, data: &[u8]) -> Result<()>;

    /// Check if the storage is currently open
    fn is_open(&self) -> bool;

    /// Get a reference to the concrete type for downcasting
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Create a storage reader for the given storage identifier
pub fn create_storage_reader(
    storage_id: &str,
    paths: Vec<&Path>,
    #[allow(unused_variables)] connections: Vec<Connection>,
) -> Result<Box<dyn StorageReader>> {
    match storage_id {
        #[cfg(feature = "sqlite")]
        "sqlite3" => Ok(Box::new(sqlite::SqliteReader::new(paths, connections)?)),
        #[cfg(not(feature = "sqlite"))]
        "sqlite3" => Err(crate::error::BagError::UnsupportedStorageFormat {
            format: "sqlite3 (feature not enabled)".to_string(),
        }),
        #[cfg(feature = "mcap")]
        "mcap" => Ok(Box::new(mcap::McapStorageReader::new(paths, connections)?)),
        #[cfg(not(feature = "mcap"))]
        "mcap" => Err(crate::error::BagError::UnsupportedStorageFormat {
            format: "mcap (feature not enabled)".to_string(),
        }),
        "" => {
            // Auto-detect storage format from file extensions when storage_identifier is empty
            let has_db3 = paths
                .iter()
                .any(|path| path.extension().is_some_and(|ext| ext == "db3"));
            let has_mcap = paths
                .iter()
                .any(|path| path.extension().is_some_and(|ext| ext == "mcap"));

            if has_db3 {
                #[cfg(feature = "sqlite")]
                {
                    Ok(Box::new(sqlite::SqliteReader::new(paths, connections)?))
                }
                #[cfg(not(feature = "sqlite"))]
                {
                    Err(crate::error::BagError::UnsupportedStorageFormat {
                        format: "sqlite3 (feature not enabled)".to_string(),
                    })
                }
            } else if has_mcap {
                #[cfg(feature = "mcap")]
                {
                    Ok(Box::new(mcap::McapStorageReader::new(paths, connections)?))
                }
                #[cfg(not(feature = "mcap"))]
                {
                    Err(crate::error::BagError::UnsupportedStorageFormat {
                        format: "mcap (feature not enabled)".to_string(),
                    })
                }
            } else {
                Err(crate::error::BagError::UnsupportedStorageFormat {
                    format: "unknown (no .db3 or .mcap files found)".to_string(),
                })
            }
        }
        _ => Err(crate::error::BagError::UnsupportedStorageFormat {
            format: storage_id.to_string(),
        }),
    }
}

/// Create a storage writer for the given storage plugin
pub fn create_storage_writer(
    storage_plugin: StoragePlugin,
    path: &Path,
    compression_mode: CompressionMode,
) -> Result<Box<dyn StorageWriter>> {
    match storage_plugin {
        #[cfg(feature = "sqlite")]
        StoragePlugin::Sqlite3 => Ok(Box::new(sqlite::SqliteWriter::new(path, compression_mode)?)),
        #[cfg(not(feature = "sqlite"))]
        StoragePlugin::Sqlite3 => Err(crate::error::BagError::UnsupportedStorageFormat {
            format: "sqlite3 (feature not enabled)".to_string(),
        }),
        #[cfg(feature = "mcap")]
        StoragePlugin::Mcap => Ok(Box::new(mcap::McapWriter::new(path, compression_mode)?)),
        #[cfg(not(feature = "mcap"))]
        StoragePlugin::Mcap => Err(crate::error::BagError::UnsupportedStorageFormat {
            format: "mcap (feature not enabled)".to_string(),
        }),
    }
}
