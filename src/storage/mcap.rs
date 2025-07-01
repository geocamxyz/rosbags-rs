//! MCAP storage backend for reading ROS2 bag files
//!
//! This module provides support for reading ROS2 bag files stored in MCAP format.
//! MCAP is a modern, efficient container format for multimodal log data.

use crate::error::{ReaderError, Result};
use crate::storage::StorageReader;
use crate::types::{Connection, Message, MessageDefinition};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

#[cfg(feature = "mcap")]
use mcap::MessageStream;

/// MCAP storage reader implementation
pub struct McapStorageReader {
    /// Paths to MCAP files
    mcap_paths: Vec<std::path::PathBuf>,
    /// Topic connections discovered from MCAP files
    topic_connections: Vec<Connection>,
    /// Whether the storage is currently open
    is_open: bool,
    /// Memory-mapped MCAP files
    #[cfg(feature = "mcap")]
    mapped_files: Vec<memmap2::Mmap>,
    #[cfg(not(feature = "mcap"))]
    mapped_files: Vec<()>, // Placeholder when MCAP feature is disabled
}

impl McapStorageReader {
    /// Create a new MCAP storage reader
    pub fn new(paths: Vec<&Path>, connections: Vec<Connection>) -> Result<Self> {
        #[cfg(not(feature = "mcap"))]
        {
            return Err(ReaderError::UnsupportedStorageFormat {
                format: "MCAP support not enabled (compile with --features mcap)".to_string(),
            });
        }

        #[cfg(feature = "mcap")]
        {
            let mcap_paths: Vec<std::path::PathBuf> =
                paths.iter().map(|p| p.to_path_buf()).collect();

            Ok(Self {
                mcap_paths,
                topic_connections: connections,
                is_open: false,
                mapped_files: Vec::new(),
            })
        }
    }

    /// Get all topics and their message counts directly from MCAP files
    #[cfg(feature = "mcap")]
    pub fn get_topics_from_mcap(&self) -> Result<Vec<Connection>> {
        let mut all_connections = Vec::new();
        let mut topic_map: HashMap<String, (String, u64)> = HashMap::new(); // topic_name -> (message_type, count)

        for mapped_file in &self.mapped_files {
            // Create message stream from mapped file
            let message_stream = MessageStream::new(mapped_file).map_err(|e| {
                ReaderError::generic(format!("Failed to create message stream: {e}"))
            })?;

            // Read all messages to count them by topic
            for message_result in message_stream {
                match message_result {
                    Ok(message) => {
                        let topic_name = &message.channel.topic;
                        let message_type = &message.channel.message_encoding;

                        let entry = topic_map
                            .entry(topic_name.clone())
                            .or_insert((message_type.clone(), 0));
                        entry.1 += 1;
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to read MCAP message: {e}");
                    }
                }
            }
        }

        // Convert to connections
        for (idx, (topic_name, (message_type, count))) in topic_map.into_iter().enumerate() {
            let connection = Connection {
                id: (idx + 1) as u32,
                topic: topic_name,
                message_type,
                message_definition: MessageDefinition::default(),
                type_description_hash: String::new(),
                message_count: count,
                serialization_format: "cdr".to_string(),
                offered_qos_profiles: Vec::new(),
            };
            all_connections.push(connection);
        }

        Ok(all_connections)
    }

    #[cfg(not(feature = "mcap"))]
    pub fn get_topics_from_mcap(&self) -> Result<Vec<Connection>> {
        Err(ReaderError::UnsupportedStorageFormat {
            format: "MCAP support not enabled".to_string(),
        })
    }
}

impl StorageReader for McapStorageReader {
    fn open(&mut self) -> Result<()> {
        #[cfg(not(feature = "mcap"))]
        {
            return Err(ReaderError::UnsupportedStorageFormat {
                format: "MCAP support not enabled (compile with --features mcap)".to_string(),
            });
        }

        #[cfg(feature = "mcap")]
        {
            self.mapped_files.clear();

            for path in &self.mcap_paths {
                let file = File::open(path).map_err(|e| {
                    ReaderError::generic(format!(
                        "Failed to open MCAP file {}: {}",
                        path.display(),
                        e
                    ))
                })?;

                let mapped_file = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
                    ReaderError::generic(format!(
                        "Failed to memory-map MCAP file {}: {}",
                        path.display(),
                        e
                    ))
                })?;

                self.mapped_files.push(mapped_file);
            }

            self.is_open = true;
            Ok(())
        }
    }

    fn close(&mut self) -> Result<()> {
        self.mapped_files.clear();
        self.is_open = false;
        Ok(())
    }

    fn get_definitions(&self) -> Result<HashMap<String, MessageDefinition>> {
        // MCAP stores schema information differently than SQLite
        // For now, return empty definitions - this can be enhanced later
        Ok(HashMap::new())
    }

    fn messages(
        &self,
        connections: Option<&[Connection]>,
        start: Option<u64>,
        stop: Option<u64>,
    ) -> Result<Box<dyn Iterator<Item = Result<Message>> + '_>> {
        #[cfg(not(feature = "mcap"))]
        {
            return Err(ReaderError::UnsupportedStorageFormat {
                format: "MCAP support not enabled".to_string(),
            });
        }

        #[cfg(feature = "mcap")]
        {
            // Create a vector to collect all messages from all MCAP files
            let mut all_messages = Vec::new();

            for mapped_file in &self.mapped_files {
                // Create message stream from mapped file
                let message_stream = MessageStream::new(mapped_file).map_err(|e| {
                    ReaderError::generic(format!("Failed to create message stream: {e}"))
                })?;

                for message_result in message_stream {
                    match message_result {
                        Ok(message) => {
                            // Check if this message matches the requested connections
                            if let Some(conns) = connections {
                                let topic_matches =
                                    conns.iter().any(|c| c.topic == message.channel.topic);
                                if !topic_matches {
                                    continue;
                                }
                            }

                            // Check time bounds
                            let timestamp = message.log_time;
                            if let Some(start_time) = start {
                                if timestamp < start_time {
                                    continue;
                                }
                            }
                            if let Some(stop_time) = stop {
                                if timestamp > stop_time {
                                    continue;
                                }
                            }

                            // Find or create a connection for this topic
                            let connection = if let Some(conn) = self
                                .topic_connections
                                .iter()
                                .find(|c| c.topic == message.channel.topic)
                            {
                                conn.clone()
                            } else {
                                // Create a temporary connection
                                Connection {
                                    id: 1, // Use a default ID since MCAP doesn't have connection IDs
                                    topic: message.channel.topic.clone(),
                                    message_type: message.channel.message_encoding.clone(),
                                    message_definition: MessageDefinition::default(),
                                    type_description_hash: String::new(),
                                    message_count: 0,
                                    serialization_format: "cdr".to_string(),
                                    offered_qos_profiles: Vec::new(),
                                }
                            };

                            let msg = Message {
                                connection,
                                topic: message.channel.topic.clone(),
                                timestamp,
                                data: message.data.to_vec(),
                            };

                            all_messages.push(Ok(msg));
                        }
                        Err(e) => {
                            all_messages.push(Err(ReaderError::generic(format!(
                                "Failed to read MCAP message: {e}"
                            ))));
                        }
                    }
                }
            }

            // Sort messages by timestamp
            all_messages.sort_by(|a, b| match (a, b) {
                (Ok(msg_a), Ok(msg_b)) => msg_a.timestamp.cmp(&msg_b.timestamp),
                _ => std::cmp::Ordering::Equal,
            });

            Ok(Box::new(all_messages.into_iter()))
        }
    }

    fn is_open(&self) -> bool {
        self.is_open
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// MCAP storage writer implementation
#[cfg(feature = "mcap")]
pub struct McapWriter {
    /// Path to the MCAP file
    mcap_path: PathBuf,
    /// File writer
    writer: Option<std::fs::File>,
    /// Whether compression is enabled (reserved for future use)
    _compression_mode: crate::types::CompressionMode,
    /// Whether the writer is currently open
    is_open: bool,
    /// Schema definitions written
    schemas: Vec<Schema>,
    /// Channel definitions written
    channels: Vec<Channel>,
    /// Next schema ID
    next_schema_id: u64,
    /// Next channel ID
    next_channel_id: u64,
    /// Channel ID mapping: topic -> MCAP channel_id
    channel_id_map: HashMap<String, u64>,
}

#[cfg(feature = "mcap")]
impl McapWriter {
    /// Create a new MCAP writer
    pub fn new(path: &Path, compression_mode: crate::types::CompressionMode) -> Result<Self> {
        let mcap_path = path.join(format!(
            "{}.mcap",
            path.file_name().unwrap().to_string_lossy()
        ));

        Ok(Self {
            mcap_path,
            writer: None,
            _compression_mode: compression_mode,
            is_open: false,
            schemas: Vec::new(),
            channels: Vec::new(),
            next_schema_id: 1,
            next_channel_id: 1,
            channel_id_map: HashMap::new(),
        })
    }

    /// Write MCAP header
    fn write_header(&mut self) -> Result<()> {
        if let Some(writer) = &mut self.writer {
            // Write MCAP magic + version
            writer.write_all(b"\x89MCAP0\r\n")?;

            // Write header record (placeholder implementation)
            // This would need proper MCAP format implementation
        }
        Ok(())
    }
}

#[cfg(feature = "mcap")]
impl crate::storage::StorageWriter for McapWriter {
    fn open(&mut self) -> Result<()> {
        if self.is_open {
            return Err(crate::error::BagError::BagAlreadyOpen);
        }

        // Create the MCAP file
        let file = std::fs::File::create(&self.mcap_path)?;
        self.writer = Some(file);

        // Write MCAP header
        self.write_header()?;

        self.is_open = true;
        Ok(())
    }

    fn close(&mut self, _version: u32, _metadata: &str) -> Result<()> {
        if !self.is_open {
            return Ok(());
        }

        // Write MCAP footer and close
        // This would need proper MCAP format implementation
        if let Some(writer) = &mut self.writer {
            writer.flush()?;
        }

        self.writer = None;
        self.is_open = false;
        self.schemas.clear();
        self.channels.clear();
        self.channel_id_map.clear();
        self.next_schema_id = 1;
        self.next_channel_id = 1;

        Ok(())
    }

    fn add_msgtype(&mut self, connection: &Connection) -> Result<()> {
        if !self.is_open {
            return Err(crate::error::BagError::BagNotOpen);
        }

        // Create schema record for this message type
        let schema = Schema {
            id: self.next_schema_id,
            name: connection.message_type.clone(),
            _encoding: "ros2msg".to_string(), // Default encoding
            _data: connection.message_definition.data.as_bytes().to_vec(),
        };

        self.schemas.push(schema);
        self.next_schema_id += 1;

        // Write schema to MCAP file (placeholder implementation)
        // This would need proper MCAP format implementation

        Ok(())
    }

    fn add_connection(
        &mut self,
        connection: &Connection,
        _offered_qos_profiles: &str,
    ) -> Result<()> {
        if !self.is_open {
            return Err(crate::error::BagError::BagNotOpen);
        }

        // Find schema for this message type
        let schema_id = self
            .schemas
            .iter()
            .find(|s| s.name == connection.message_type)
            .map(|s| s.id)
            .unwrap_or(0);

        // Create channel record for this topic
        let channel = Channel {
            _id: self.next_channel_id,
            _schema_id: schema_id,
            _topic: connection.topic.clone(),
            _message_encoding: connection.message_type.clone(),
            _metadata: HashMap::new(),
        };

        self.channel_id_map
            .insert(connection.topic.clone(), self.next_channel_id);
        self.channels.push(channel);
        self.next_channel_id += 1;

        // Write channel to MCAP file (placeholder implementation)
        // This would need proper MCAP format implementation

        Ok(())
    }

    fn write(&mut self, connection: &Connection, _timestamp: u64, _data: &[u8]) -> Result<()> {
        if !self.is_open {
            return Err(crate::error::BagError::BagNotOpen);
        }

        let _channel_id = self
            .channel_id_map
            .get(&connection.topic)
            .ok_or_else(|| crate::error::BagError::connection_not_found(&connection.topic))?;

        // Write message to MCAP file (placeholder implementation)
        // This would need proper MCAP format implementation

        // For now, just ensure we have a valid file handle
        if self.writer.is_none() {
            return Err(crate::error::BagError::BagNotOpen);
        }

        Ok(())
    }

    fn is_open(&self) -> bool {
        self.is_open
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// MCAP writer stub for when MCAP feature is disabled
#[cfg(not(feature = "mcap"))]
pub struct McapWriter;

#[cfg(not(feature = "mcap"))]
impl McapWriter {
    pub fn new(_path: &Path, _compression_mode: crate::types::CompressionMode) -> Result<Self> {
        Err(crate::error::BagError::UnsupportedStorageFormat {
            format: "MCAP support not enabled (compile with --features mcap)".to_string(),
        })
    }
}

#[cfg(not(feature = "mcap"))]
impl crate::storage::StorageWriter for McapWriter {
    fn open(&mut self) -> Result<()> {
        Err(crate::error::BagError::UnsupportedStorageFormat {
            format: "MCAP support not enabled".to_string(),
        })
    }

    fn close(&mut self, _version: u32, _metadata: &str) -> Result<()> {
        Ok(())
    }

    fn add_msgtype(&mut self, _connection: &Connection) -> Result<()> {
        Err(crate::error::BagError::UnsupportedStorageFormat {
            format: "MCAP support not enabled".to_string(),
        })
    }

    fn add_connection(
        &mut self,
        _connection: &Connection,
        _offered_qos_profiles: &str,
    ) -> Result<()> {
        Err(crate::error::BagError::UnsupportedStorageFormat {
            format: "MCAP support not enabled".to_string(),
        })
    }

    fn write(&mut self, _connection: &Connection, _timestamp: u64, _data: &[u8]) -> Result<()> {
        Err(crate::error::BagError::UnsupportedStorageFormat {
            format: "MCAP support not enabled".to_string(),
        })
    }

    fn is_open(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// Helper structs for MCAP format (simplified)
#[cfg(feature = "mcap")]
#[derive(Debug, Clone)]
struct Schema {
    id: u64,
    name: String,
    _encoding: String,
    _data: Vec<u8>,
}

#[cfg(feature = "mcap")]
#[derive(Debug, Clone)]
struct Channel {
    _id: u64,
    _schema_id: u64,
    _topic: String,
    _message_encoding: String,
    _metadata: HashMap<String, String>,
}
