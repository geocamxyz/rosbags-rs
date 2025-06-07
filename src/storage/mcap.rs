//! MCAP storage backend for reading ROS2 bag files
//!
//! This module provides support for reading ROS2 bag files stored in MCAP format.
//! MCAP is a modern, efficient container format for multimodal log data.

use crate::error::{ReaderError, Result};
use crate::storage::StorageReader;
use crate::types::{Connection, Message, MessageDefinition};
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

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
            let mcap_paths: Vec<std::path::PathBuf> = paths.iter().map(|p| p.to_path_buf()).collect();

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
            let message_stream = MessageStream::new(mapped_file)
                .map_err(|e| ReaderError::generic(format!("Failed to create message stream: {}", e)))?;

            // Read all messages to count them by topic
            for message_result in message_stream {
                match message_result {
                    Ok(message) => {
                        let topic_name = &message.channel.topic;
                        let message_type = &message.channel.message_encoding;

                        let entry = topic_map.entry(topic_name.clone()).or_insert((message_type.clone(), 0));
                        entry.1 += 1;
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to read MCAP message: {}", e);
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
                let file = File::open(path)
                    .map_err(|e| ReaderError::generic(
                        format!("Failed to open MCAP file {}: {}", path.display(), e)
                    ))?;

                let mapped_file = unsafe { memmap2::Mmap::map(&file) }
                    .map_err(|e| ReaderError::generic(
                        format!("Failed to memory-map MCAP file {}: {}", path.display(), e)
                    ))?;

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
                let message_stream = MessageStream::new(mapped_file)
                    .map_err(|e| ReaderError::generic(format!("Failed to create message stream: {}", e)))?;

                for message_result in message_stream {
                    match message_result {
                        Ok(message) => {
                            // Check if this message matches the requested connections
                            if let Some(conns) = connections {
                                let topic_matches = conns.iter().any(|c| c.topic == message.channel.topic);
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
                            let connection = if let Some(conn) = self.topic_connections.iter().find(|c| c.topic == message.channel.topic) {
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
                            all_messages.push(Err(ReaderError::generic(
                                format!("Failed to read MCAP message: {}", e)
                            )));
                        }
                    }
                }
            }

            // Sort messages by timestamp
            all_messages.sort_by(|a, b| {
                match (a, b) {
                    (Ok(msg_a), Ok(msg_b)) => msg_a.timestamp.cmp(&msg_b.timestamp),
                    _ => std::cmp::Ordering::Equal,
                }
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
