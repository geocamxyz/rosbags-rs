//! Main writer implementation for ROS2 bag files

use crate::error::{BagError, Result};
use crate::metadata::{BagFileInformation, BagMetadata};
use crate::storage::{create_storage_writer, StorageWriter};
use crate::types::{
    CompressionFormat, CompressionMode, Connection, MessageDefinition, QosProfile, StoragePlugin,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Buffered message for batch writing
#[derive(Debug, Clone)]
struct BufferedMessage {
    connection: Connection,
    timestamp: u64,
    data: Vec<u8>,
}

/// Main writer for ROS2 bag files
pub struct Writer {
    /// Path to the bag directory
    bag_path: PathBuf,
    /// Metadata file path
    metadata_path: PathBuf,
    /// Bag format version (8 or 9)
    version: u32,
    /// Storage plugin to use
    storage_plugin: StoragePlugin,
    /// Compression mode
    compression_mode: CompressionMode,
    /// Compression format
    compression_format: CompressionFormat,
    /// Storage backend
    storage: Option<Box<dyn StorageWriter>>,
    /// Connections (topics) in the bag
    connections: Vec<Connection>,
    /// Message counts per connection
    message_counts: HashMap<u32, u64>,
    /// Custom metadata
    custom_data: HashMap<String, String>,
    /// Added message types (to avoid duplicates)
    added_types: std::collections::HashSet<String>,
    /// Minimum timestamp seen
    min_timestamp: u64,
    /// Maximum timestamp seen
    max_timestamp: u64,
    /// Whether the writer is currently open
    is_open: bool,
    /// Message buffer for batch writing
    message_buffer: Vec<BufferedMessage>,
    /// Maximum buffer size in bytes (default: 10MB)
    buffer_size_limit: usize,
    /// Current buffer size in bytes
    current_buffer_size: usize,
    /// Batch write size threshold (number of messages to trigger flush)
    batch_threshold: usize,
}

impl std::fmt::Debug for Writer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Writer")
            .field("bag_path", &self.bag_path)
            .field("metadata_path", &self.metadata_path)
            .field("version", &self.version)
            .field("storage_plugin", &self.storage_plugin)
            .field("compression_mode", &self.compression_mode)
            .field("compression_format", &self.compression_format)
            .field("storage", &"<storage>")
            .field("connections", &self.connections)
            .field("message_counts", &self.message_counts)
            .field("custom_data", &self.custom_data)
            .field("added_types", &self.added_types)
            .field("min_timestamp", &self.min_timestamp)
            .field("max_timestamp", &self.max_timestamp)
            .field("is_open", &self.is_open)
            .field("message_buffer", &self.message_buffer)
            .field("buffer_size_limit", &self.buffer_size_limit)
            .field("current_buffer_size", &self.current_buffer_size)
            .field("batch_threshold", &self.batch_threshold)
            .finish()
    }
}

impl Writer {
    /// Latest supported bag format version
    pub const VERSION_LATEST: u32 = 9;

    /// Create a new writer for the given bag path
    pub fn new<P: AsRef<Path>>(
        bag_path: P,
        version: Option<u32>,
        storage_plugin: Option<StoragePlugin>,
    ) -> Result<Self> {
        let bag_path = bag_path.as_ref().to_path_buf();

        // Check if the bag directory already exists
        if bag_path.exists() {
            return Err(BagError::BagAlreadyExists { path: bag_path });
        }

        let version = version.unwrap_or(Self::VERSION_LATEST);
        let storage_plugin = storage_plugin.unwrap_or(StoragePlugin::Sqlite3);

        let metadata_path = bag_path.join("metadata.yaml");

        Ok(Self {
            bag_path,
            metadata_path,
            version,
            storage_plugin,
            compression_mode: CompressionMode::None,
            compression_format: CompressionFormat::None,
            storage: None,
            connections: Vec::new(),
            message_counts: HashMap::new(),
            custom_data: HashMap::new(),
            added_types: std::collections::HashSet::new(),
            min_timestamp: u64::MAX,
            max_timestamp: 0,
            is_open: false,
            message_buffer: Vec::new(),
            buffer_size_limit: 10 * 1024 * 1024, // 10MB
            current_buffer_size: 0,
            batch_threshold: 100, // 100 messages
        })
    }

    /// Set compression for the bag
    pub fn set_compression(
        &mut self,
        mode: CompressionMode,
        format: CompressionFormat,
    ) -> Result<()> {
        if self.is_open {
            return Err(BagError::BagAlreadyOpen);
        }

        self.compression_mode = mode;
        self.compression_format = format;

        // Note: For message-level compression, we use one-shot compression per message
        // so no persistent compressor is needed

        #[cfg(not(feature = "compression"))]
        if format == CompressionFormat::Zstd {
            return Err(BagError::UnsupportedCompressionFormat {
                format: "zstd (feature not enabled)".to_string(),
            });
        }

        Ok(())
    }

    /// Set custom metadata
    pub fn set_custom_data(&mut self, key: String, value: String) -> Result<()> {
        self.custom_data.insert(key, value);
        Ok(())
    }

    /// Configure message buffer settings for performance optimization
    ///
    /// # Arguments
    /// * `buffer_size_mb` - Maximum buffer size in megabytes (default: 10MB)
    /// * `batch_threshold` - Number of messages to trigger flush (default: 100)
    ///
    /// # Example
    /// ```no_run
    /// # use rosbags_rs::Writer;
    /// # let mut writer = Writer::new("test", None, None).unwrap();
    /// // Use 20MB buffer with 500 message batches for high-throughput scenarios
    /// writer.configure_buffer(20, 500).unwrap();
    /// ```
    pub fn configure_buffer(
        &mut self,
        buffer_size_mb: usize,
        batch_threshold: usize,
    ) -> Result<()> {
        if self.is_open {
            return Err(BagError::BagAlreadyOpen);
        }

        self.buffer_size_limit = buffer_size_mb * 1024 * 1024;
        self.batch_threshold = batch_threshold;
        Ok(())
    }

    /// Flush the message buffer to storage
    ///
    /// This method writes all buffered messages to storage in a batch operation.
    /// It's automatically called when the buffer reaches its limits, but can also
    /// be called manually for explicit control.
    pub fn flush_buffer(&mut self) -> Result<()> {
        if self.message_buffer.is_empty() {
            return Ok(());
        }

        // Convert buffer to format expected by write_batch
        let batch_messages: Vec<(Connection, u64, Vec<u8>)> = self
            .message_buffer
            .iter()
            .map(|msg| (msg.connection.clone(), msg.timestamp, msg.data.clone()))
            .collect();

        let storage = self.storage.as_mut().unwrap();

        // Use batch write for better performance
        storage.write_batch(&batch_messages)?;

        // Clear the buffer
        self.message_buffer.clear();
        self.current_buffer_size = 0;

        Ok(())
    }

    /// Check if buffer should be flushed
    fn should_flush_buffer(&self) -> bool {
        self.message_buffer.len() >= self.batch_threshold
            || self.current_buffer_size >= self.buffer_size_limit
    }

    /// Open the bag for writing
    pub fn open(&mut self) -> Result<()> {
        if self.is_open {
            return Ok(());
        }

        // Create bag directory
        std::fs::create_dir_all(&self.bag_path)?;

        // Create storage writer
        let mut storage =
            create_storage_writer(self.storage_plugin, &self.bag_path, self.compression_mode)?;

        // Open storage
        storage.open()?;

        self.storage = Some(storage);
        self.is_open = true;

        Ok(())
    }

    /// Add a connection (topic) to the bag
    pub fn add_connection(
        &mut self,
        topic: String,
        message_type: String,
        message_definition: Option<MessageDefinition>,
        type_description_hash: Option<String>,
        serialization_format: Option<String>,
        offered_qos_profiles: Option<Vec<QosProfile>>,
    ) -> Result<Connection> {
        if !self.is_open {
            return Err(BagError::BagNotOpen);
        }

        let connection_id = (self.connections.len() + 1) as u32;

        // Use defaults if not provided
        let message_definition = message_definition.unwrap_or_default();
        let type_description_hash = type_description_hash.unwrap_or_default();
        let serialization_format = serialization_format.unwrap_or_else(|| "cdr".to_string());
        let offered_qos_profiles = offered_qos_profiles.unwrap_or_default();

        let connection = Connection {
            id: connection_id,
            topic: topic.clone(),
            message_type: message_type.clone(),
            message_definition: message_definition.clone(),
            type_description_hash: type_description_hash.clone(),
            message_count: 0,
            serialization_format,
            offered_qos_profiles: offered_qos_profiles.clone(),
        };

        // Check for duplicate connections
        for existing_conn in &self.connections {
            if existing_conn.topic == connection.topic
                && existing_conn.message_type == connection.message_type
            {
                return Err(BagError::ConnectionAlreadyExists {
                    topic: connection.topic,
                });
            }
        }

        // Serialize QoS profiles
        let qos_yaml = self.serialize_qos_profiles(&offered_qos_profiles)?;

        let storage = self.storage.as_mut().unwrap();

        // Add message type definition if not already added
        if !self.added_types.contains(&message_type) {
            storage.add_msgtype(&connection)?;
            self.added_types.insert(message_type);
        }

        // Add connection to storage
        storage.add_connection(&connection, &qos_yaml)?;

        // Initialize message count
        self.message_counts.insert(connection_id, 0);

        self.connections.push(connection.clone());

        Ok(connection)
    }

    /// Write a message to the bag
    pub fn write(&mut self, connection: &Connection, timestamp: u64, data: &[u8]) -> Result<()> {
        if !self.is_open {
            return Err(BagError::BagNotOpen);
        }

        // Check if connection exists
        if !self.connections.iter().any(|c| c.id == connection.id) {
            return Err(BagError::ConnectionNotFound {
                topic: connection.topic.clone(),
            });
        }

        // Apply compression if needed
        let final_data = match self.compression_mode {
            CompressionMode::Message => {
                #[cfg(feature = "compression")]
                {
                    if self.compression_format == CompressionFormat::Zstd {
                        zstd::encode_all(data, 0)?
                    } else {
                        data.to_vec()
                    }
                }
                #[cfg(not(feature = "compression"))]
                {
                    data.to_vec()
                }
            }
            _ => data.to_vec(),
        };

        // Add message to buffer
        let buffered_message = BufferedMessage {
            connection: connection.clone(),
            timestamp,
            data: final_data.clone(),
        };

        self.current_buffer_size += final_data.len();
        self.message_buffer.push(buffered_message);

        // Update statistics
        *self.message_counts.entry(connection.id).or_insert(0) += 1;
        self.min_timestamp = self.min_timestamp.min(timestamp);
        self.max_timestamp = self.max_timestamp.max(timestamp);

        // Flush buffer if it's full
        if self.should_flush_buffer() {
            self.flush_buffer()?;
        }

        Ok(())
    }

    /// Close the bag and write metadata
    pub fn close(&mut self) -> Result<()> {
        if !self.is_open {
            return Ok(());
        }

        // Flush any remaining buffered messages
        self.flush_buffer()?;

        // Generate metadata
        let bag_info = self.generate_metadata()?;
        let metadata = BagMetadata {
            rosbag2_bagfile_information: bag_info,
        };
        let metadata_yaml = serde_yml::to_string(&metadata)?;

        // Close storage
        if let Some(mut storage) = self.storage.take() {
            storage.close(self.version, &metadata_yaml)?;
        }

        // Write metadata.yaml
        std::fs::write(&self.metadata_path, &metadata_yaml)?;

        // Handle file compression if needed
        if self.compression_mode == CompressionMode::File {
            self.compress_storage_file()?;
        }

        self.is_open = false;
        Ok(())
    }

    /// Get all connections
    pub fn connections(&self) -> &[Connection] {
        &self.connections
    }

    /// Check if the bag is currently open
    pub fn is_open(&self) -> bool {
        self.is_open
    }

    /// Generate bag metadata
    fn generate_metadata(&self) -> Result<BagFileInformation> {
        let storage_file_name = match self.storage_plugin {
            StoragePlugin::Sqlite3 => format!(
                "{}.db3",
                self.bag_path.file_name().unwrap().to_string_lossy()
            ),
            StoragePlugin::Mcap => format!(
                "{}.mcap",
                self.bag_path.file_name().unwrap().to_string_lossy()
            ),
        };

        let final_file_name = if self.compression_mode == CompressionMode::File {
            format!("{}.{}", storage_file_name, self.compression_format.as_str())
        } else {
            storage_file_name
        };

        let duration = self.max_timestamp.saturating_sub(self.min_timestamp);

        let total_message_count: u64 = self.message_counts.values().sum();

        let topics_with_message_count = self
            .connections
            .iter()
            .map(|conn| crate::metadata::TopicWithMessageCount {
                message_count: *self.message_counts.get(&conn.id).unwrap_or(&0),
                topic_metadata: crate::metadata::TopicMetadata {
                    name: conn.topic.clone(),
                    message_type: conn.message_type.clone(),
                    serialization_format: conn.serialization_format.clone(),
                    offered_qos_profiles: crate::metadata::QosProfilesField::List(
                        conn.offered_qos_profiles.clone(),
                    ),
                    type_description_hash: conn.type_description_hash.clone(),
                },
            })
            .collect();

        Ok(BagFileInformation {
            version: self.version,
            storage_identifier: self.storage_plugin.as_str().to_string(),
            relative_file_paths: vec![final_file_name.clone()],
            duration: crate::types::Duration {
                nanoseconds: duration,
            },
            starting_time: crate::types::StartingTime {
                nanoseconds_since_epoch: self.min_timestamp,
            },
            message_count: total_message_count,
            compression_format: if self.compression_mode == CompressionMode::None {
                String::new()
            } else {
                self.compression_format.as_str().to_string()
            },
            compression_mode: if self.compression_mode == CompressionMode::None {
                String::new()
            } else {
                self.compression_mode.as_str().to_string()
            },
            topics_with_message_count,
            files: vec![crate::metadata::FileInformation {
                path: final_file_name,
                starting_time: crate::types::StartingTime {
                    nanoseconds_since_epoch: self.min_timestamp,
                },
                duration: crate::types::Duration {
                    nanoseconds: duration,
                },
                message_count: total_message_count,
            }],
            custom_data: if self.custom_data.is_empty() {
                None
            } else {
                Some(self.custom_data.clone())
            },
            ros_distro: Some("rosbags".to_string()),
        })
    }

    /// Serialize QoS profiles to YAML
    fn serialize_qos_profiles(&self, profiles: &[QosProfile]) -> Result<String> {
        if profiles.is_empty() {
            return Ok(String::new());
        }

        // Simple YAML serialization for QoS profiles
        let yaml = serde_yml::to_string(profiles)?;
        Ok(yaml.trim().to_string())
    }

    /// Compress storage file (for file-level compression)
    fn compress_storage_file(&self) -> Result<()> {
        #[cfg(feature = "compression")]
        {
            let storage_file = match self.storage_plugin {
                StoragePlugin::Sqlite3 => self.bag_path.join(format!(
                    "{}.db3",
                    self.bag_path.file_name().unwrap().to_string_lossy()
                )),
                StoragePlugin::Mcap => self.bag_path.join(format!(
                    "{}.mcap",
                    self.bag_path.file_name().unwrap().to_string_lossy()
                )),
            };

            let compressed_file = storage_file.with_extension(format!(
                "{}.{}",
                storage_file.extension().unwrap().to_string_lossy(),
                self.compression_format.as_str()
            ));

            let input_data = std::fs::read(&storage_file)?;
            let compressed_data = zstd::encode_all(input_data.as_slice(), 0)?;
            std::fs::write(&compressed_file, compressed_data)?;
            std::fs::remove_file(&storage_file)?;
            Ok(())
        }

        #[cfg(not(feature = "compression"))]
        {
            return Err(BagError::UnsupportedCompressionFormat {
                format: "zstd (feature not enabled)".to_string(),
            });
        }
    }

    /// Write a raw serialized message directly without any deserialization/serialization overhead.
    /// This is the fastest way to copy messages between bags when no processing is needed.
    ///
    /// This method bypasses all message processing and directly writes the raw bytes,
    /// similar to how ROS2 bag convert achieves high performance.
    pub fn write_raw_message(
        &mut self,
        connection: &Connection,
        timestamp: u64,
        raw_data: &[u8],
    ) -> Result<()> {
        if !self.is_open {
            return Err(BagError::BagNotOpen);
        }

        // Update min/max timestamps
        if timestamp < self.min_timestamp {
            self.min_timestamp = timestamp;
        }
        if timestamp > self.max_timestamp {
            self.max_timestamp = timestamp;
        }

        // Update message count
        *self.message_counts.entry(connection.id).or_insert(0) += 1;

        // Add to buffer for batch writing
        let buffered_msg = BufferedMessage {
            connection: connection.clone(),
            timestamp,
            data: raw_data.to_vec(),
        };

        self.current_buffer_size += raw_data.len();
        self.message_buffer.push(buffered_msg);

        // Flush if buffer is full
        if self.should_flush_buffer() {
            self.flush_buffer()?;
        }

        Ok(())
    }

    /// Write multiple raw messages in a batch for maximum performance.
    /// This is optimized for bulk transfer operations and skips individual buffer checks.
    pub fn write_raw_messages_batch(
        &mut self,
        messages: &[(Connection, u64, Vec<u8>)],
    ) -> Result<()> {
        if !self.is_open {
            return Err(BagError::BagNotOpen);
        }

        if messages.is_empty() {
            return Ok(());
        }

        // Flush existing buffer first
        self.flush_buffer()?;

        // Update statistics
        for (connection, timestamp, _data) in messages {
            if *timestamp < self.min_timestamp {
                self.min_timestamp = *timestamp;
            }
            if *timestamp > self.max_timestamp {
                self.max_timestamp = *timestamp;
            }
            *self.message_counts.entry(connection.id).or_insert(0) += 1;
        }

        // Use storage's direct batch write if available
        if let Some(storage) = &mut self.storage {
            storage.write_batch(messages)?;
        }

        Ok(())
    }

    /// Copy a message directly from a reader without any processing.
    /// This is the equivalent of ROS2's direct SerializedBagMessage transfer.
    pub fn copy_raw_message_from_reader(
        &mut self,
        connection: &Connection,
        timestamp: u64,
        raw_message_data: &[u8],
    ) -> Result<()> {
        self.write_raw_message(connection, timestamp, raw_message_data)
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_writer_creation() {
        let temp_dir = TempDir::new().unwrap();
        let bag_path = temp_dir.path().join("test_bag");

        let writer = Writer::new(&bag_path, None, None);
        assert!(writer.is_ok());

        let writer = writer.unwrap();
        assert!(!writer.is_open());
        assert_eq!(writer.version, Writer::VERSION_LATEST);
    }

    #[test]
    fn test_writer_rejects_existing_path() {
        let temp_dir = TempDir::new().unwrap();
        let bag_path = temp_dir.path().join("existing_bag");
        std::fs::create_dir(&bag_path).unwrap();

        let writer = Writer::new(&bag_path, None, None);
        assert!(writer.is_err());
        assert!(matches!(
            writer.unwrap_err(),
            BagError::BagAlreadyExists { .. }
        ));
    }

    #[test]
    fn test_writer_open_close() {
        let temp_dir = TempDir::new().unwrap();
        let bag_path = temp_dir.path().join("test_bag");

        let mut writer = Writer::new(&bag_path, None, None).unwrap();
        assert!(!writer.is_open());

        writer.open().unwrap();
        assert!(writer.is_open());

        writer.close().unwrap();
        assert!(!writer.is_open());

        // Check that files were created
        assert!(bag_path.exists());
        assert!(bag_path.join("metadata.yaml").exists());
    }

    #[test]
    fn test_set_compression() {
        let temp_dir = TempDir::new().unwrap();
        let bag_path = temp_dir.path().join("test_bag");

        let mut writer = Writer::new(&bag_path, None, None).unwrap();

        // Should succeed before opening
        let result = writer.set_compression(CompressionMode::Message, CompressionFormat::Zstd);
        #[cfg(feature = "compression")]
        assert!(result.is_ok());
        #[cfg(not(feature = "compression"))]
        assert!(result.is_err());

        writer.open().unwrap();

        // Should fail after opening
        let result = writer.set_compression(CompressionMode::File, CompressionFormat::Zstd);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), BagError::BagAlreadyOpen));
    }

    #[test]
    fn test_add_connection() {
        let temp_dir = TempDir::new().unwrap();
        let bag_path = temp_dir.path().join("test_bag");

        let mut writer = Writer::new(&bag_path, None, None).unwrap();
        writer.open().unwrap();

        let connection = writer
            .add_connection(
                "/test_topic".to_string(),
                "std_msgs/msg/String".to_string(),
                None,
                None,
                None,
                None,
            )
            .unwrap();

        assert_eq!(connection.topic, "/test_topic");
        assert_eq!(connection.message_type, "std_msgs/msg/String");
        assert_eq!(connection.id, 1);
        assert_eq!(writer.connections().len(), 1);
    }

    #[test]
    fn test_duplicate_connection() {
        let temp_dir = TempDir::new().unwrap();
        let bag_path = temp_dir.path().join("test_bag");

        let mut writer = Writer::new(&bag_path, None, None).unwrap();
        writer.open().unwrap();

        // Add first connection
        writer
            .add_connection(
                "/test_topic".to_string(),
                "std_msgs/msg/String".to_string(),
                None,
                None,
                None,
                None,
            )
            .unwrap();

        // Try to add duplicate connection
        let result = writer.add_connection(
            "/test_topic".to_string(),
            "std_msgs/msg/String".to_string(),
            None,
            None,
            None,
            None,
        );

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            BagError::ConnectionAlreadyExists { .. }
        ));
    }

    #[test]
    fn test_write_message() {
        let temp_dir = TempDir::new().unwrap();
        let bag_path = temp_dir.path().join("test_bag");

        let mut writer = Writer::new(&bag_path, None, None).unwrap();
        writer.open().unwrap();

        let connection = writer
            .add_connection(
                "/test_topic".to_string(),
                "std_msgs/msg/String".to_string(),
                None,
                None,
                None,
                None,
            )
            .unwrap();

        let test_data = b"Hello, ROS2!";
        let timestamp = 1_234_567_890_000_000_000; // nanoseconds

        let result = writer.write(&connection, timestamp, test_data);
        assert!(result.is_ok());

        // Check that message count was updated
        assert_eq!(*writer.message_counts.get(&connection.id).unwrap(), 1);
    }

    /// Test writing all supported message types to a bag file
    #[test]
    fn test_write_all_supported_topics() {
        let temp_dir = TempDir::new().unwrap();
        let bag_path = temp_dir.path().join("comprehensive_test_bag");

        let mut writer = Writer::new(&bag_path, None, None).unwrap();
        writer.open().unwrap();

        // Define message types to test
        let message_types = vec![
            // std_msgs
            ("std_msgs/msg/String", "/test/std_msgs/string"),
            ("std_msgs/msg/Int32", "/test/std_msgs/int32"),
            ("std_msgs/msg/Float64", "/test/std_msgs/float64"),
            ("std_msgs/msg/Bool", "/test/std_msgs/bool"),
            ("std_msgs/msg/Header", "/test/std_msgs/header"),
            ("std_msgs/msg/ColorRGBA", "/test/std_msgs/color_rgba"),
            // geometry_msgs
            ("geometry_msgs/msg/Point", "/test/geometry_msgs/point"),
            ("geometry_msgs/msg/Point32", "/test/geometry_msgs/point32"),
            ("geometry_msgs/msg/Vector3", "/test/geometry_msgs/vector3"),
            (
                "geometry_msgs/msg/Quaternion",
                "/test/geometry_msgs/quaternion",
            ),
            ("geometry_msgs/msg/Pose", "/test/geometry_msgs/pose"),
            (
                "geometry_msgs/msg/PoseStamped",
                "/test/geometry_msgs/pose_stamped",
            ),
            (
                "geometry_msgs/msg/PoseWithCovariance",
                "/test/geometry_msgs/pose_with_covariance",
            ),
            (
                "geometry_msgs/msg/PoseWithCovarianceStamped",
                "/test/geometry_msgs/pose_with_covariance_stamped",
            ),
            (
                "geometry_msgs/msg/Transform",
                "/test/geometry_msgs/transform",
            ),
            (
                "geometry_msgs/msg/TransformStamped",
                "/test/geometry_msgs/transform_stamped",
            ),
            ("geometry_msgs/msg/Twist", "/test/geometry_msgs/twist"),
            (
                "geometry_msgs/msg/TwistStamped",
                "/test/geometry_msgs/twist_stamped",
            ),
            (
                "geometry_msgs/msg/PointStamped",
                "/test/geometry_msgs/point_stamped",
            ),
            // sensor_msgs
            ("sensor_msgs/msg/Image", "/test/sensor_msgs/image"),
            (
                "sensor_msgs/msg/CompressedImage",
                "/test/sensor_msgs/compressed_image",
            ),
            (
                "sensor_msgs/msg/CameraInfo",
                "/test/sensor_msgs/camera_info",
            ),
            (
                "sensor_msgs/msg/PointCloud2",
                "/test/sensor_msgs/point_cloud2",
            ),
            ("sensor_msgs/msg/LaserScan", "/test/sensor_msgs/laser_scan"),
            ("sensor_msgs/msg/Imu", "/test/sensor_msgs/imu"),
            ("sensor_msgs/msg/NavSatFix", "/test/sensor_msgs/nav_sat_fix"),
            (
                "sensor_msgs/msg/NavSatStatus",
                "/test/sensor_msgs/nav_sat_status",
            ),
            (
                "sensor_msgs/msg/MagneticField",
                "/test/sensor_msgs/magnetic_field",
            ),
            (
                "sensor_msgs/msg/Temperature",
                "/test/sensor_msgs/temperature",
            ),
            (
                "sensor_msgs/msg/RelativeHumidity",
                "/test/sensor_msgs/relative_humidity",
            ),
            (
                "sensor_msgs/msg/FluidPressure",
                "/test/sensor_msgs/fluid_pressure",
            ),
            (
                "sensor_msgs/msg/Illuminance",
                "/test/sensor_msgs/illuminance",
            ),
            ("sensor_msgs/msg/Range", "/test/sensor_msgs/range"),
            (
                "sensor_msgs/msg/PointField",
                "/test/sensor_msgs/point_field",
            ),
            // nav_msgs
            ("nav_msgs/msg/Odometry", "/test/nav_msgs/odometry"),
            (
                "nav_msgs/msg/OccupancyGrid",
                "/test/nav_msgs/occupancy_grid",
            ),
            ("nav_msgs/msg/MapMetaData", "/test/nav_msgs/map_meta_data"),
            ("nav_msgs/msg/GridCells", "/test/nav_msgs/grid_cells"),
            ("nav_msgs/msg/Path", "/test/nav_msgs/path"),
            // diagnostic_msgs
            (
                "diagnostic_msgs/msg/DiagnosticArray",
                "/test/diagnostic_msgs/diagnostic_array",
            ),
            (
                "diagnostic_msgs/msg/DiagnosticStatus",
                "/test/diagnostic_msgs/diagnostic_status",
            ),
            (
                "diagnostic_msgs/msg/KeyValue",
                "/test/diagnostic_msgs/key_value",
            ),
            // builtin_interfaces
            (
                "builtin_interfaces/msg/Time",
                "/test/builtin_interfaces/time",
            ),
            (
                "builtin_interfaces/msg/Duration",
                "/test/builtin_interfaces/duration",
            ),
            // action_msgs
            ("action_msgs/msg/GoalInfo", "/test/action_msgs/goal_info"),
            (
                "action_msgs/msg/GoalStatus",
                "/test/action_msgs/goal_status",
            ),
            (
                "action_msgs/msg/GoalStatusArray",
                "/test/action_msgs/goal_status_array",
            ),
            // tf2_msgs
            ("tf2_msgs/msg/TFMessage", "/test/tf2_msgs/tf_message"),
            ("tf2_msgs/msg/TF2Error", "/test/tf2_msgs/tf2_error"),
            // stereo_msgs
            (
                "stereo_msgs/msg/DisparityImage",
                "/test/stereo_msgs/disparity_image",
            ),
        ];

        let mut connections = Vec::new();

        // Add all connections
        for (msg_type, topic) in &message_types {
            match writer.add_connection(
                topic.to_string(),
                msg_type.to_string(),
                None, // Use default message definition
                Some(format!("hash_{msg_type}")),
                None, // Use default CDR serialization
                None, // Use default QoS
            ) {
                Ok(connection) => connections.push(connection),
                Err(e) => {
                    eprintln!("Warning: Failed to add connection for {topic} ({msg_type}): {e}");
                    // Continue with other connections
                }
            }
        }

        // Successfully added connections - no need to print in production
        assert!(
            !connections.is_empty(),
            "Should have added at least some connections"
        );

        // Write test messages for each connection
        let base_timestamp = 1_234_567_890_000_000_000; // nanoseconds since epoch

        for (i, connection) in connections.iter().enumerate() {
            // Create simple test data for each message type
            let test_data = create_test_message_data(&connection.message_type);
            let timestamp = base_timestamp + (i as u64 * 1_000_000); // 1ms apart

            match writer.write(connection, timestamp, &test_data) {
                Ok(()) => {
                    // Successfully wrote message - no need to print in production
                }
                Err(e) => {
                    eprintln!(
                        "Warning: Failed to write message for {}: {}",
                        connection.topic, e
                    );
                    // Continue with other messages
                }
            }
        }

        // Close the writer
        writer.close().unwrap();

        // Verify the bag was created successfully
        assert!(bag_path.exists());
        assert!(bag_path.join("metadata.yaml").exists());

        // Check that metadata file contains expected content
        let metadata_content = std::fs::read_to_string(bag_path.join("metadata.yaml")).unwrap();
        assert!(metadata_content.contains("rosbag2_bagfile_information"));
        assert!(metadata_content.contains("topics_with_message_count"));

        // Successfully created comprehensive test bag - no need to print in production
    }

    /// Create test message data for different message types
    fn create_test_message_data(message_type: &str) -> Vec<u8> {
        // This is a simplified implementation that creates basic CDR-encoded data
        // In a real implementation, you would use proper message serialization

        match message_type {
            "std_msgs/msg/String" => {
                // CDR-encoded string: 4-byte length + string data + padding
                let test_string = "Hello ROS2";
                let mut data = Vec::new();
                data.extend_from_slice(&(test_string.len() as u32).to_le_bytes());
                data.extend_from_slice(test_string.as_bytes());
                // Add padding to align to 4 bytes
                while data.len() % 4 != 0 {
                    data.push(0);
                }
                data
            }
            "std_msgs/msg/Int32" => {
                // CDR-encoded int32: 4 bytes
                vec![42u8, 0, 0, 0] // 42 in little-endian
            }
            "std_msgs/msg/Float64" => {
                // CDR-encoded float64: 8 bytes
                let value = std::f64::consts::PI;
                value.to_le_bytes().to_vec()
            }
            "std_msgs/msg/Bool" => {
                // CDR-encoded bool: 1 byte
                vec![1u8] // true
            }
            "geometry_msgs/msg/Point" => {
                // CDR-encoded Point: 3 float64s (24 bytes)
                let mut data = Vec::new();
                data.extend_from_slice(&1.0f64.to_le_bytes()); // x
                data.extend_from_slice(&2.0f64.to_le_bytes()); // y
                data.extend_from_slice(&3.0f64.to_le_bytes()); // z
                data
            }
            "sensor_msgs/msg/Imu" => {
                // CDR-encoded IMU message (simplified)
                let mut data = Vec::new();
                // Header (simplified)
                data.extend_from_slice(&0u32.to_le_bytes()); // seq (if present)
                data.extend_from_slice(&1234567890u32.to_le_bytes()); // stamp.sec
                data.extend_from_slice(&0u32.to_le_bytes()); // stamp.nanosec
                let frame_id = "imu_frame";
                data.extend_from_slice(&(frame_id.len() as u32).to_le_bytes());
                data.extend_from_slice(frame_id.as_bytes());
                while data.len() % 4 != 0 {
                    data.push(0);
                }
                // Orientation quaternion (4 float64s)
                data.extend_from_slice(&0.0f64.to_le_bytes()); // x
                data.extend_from_slice(&0.0f64.to_le_bytes()); // y
                data.extend_from_slice(&0.0f64.to_le_bytes()); // z
                data.extend_from_slice(&1.0f64.to_le_bytes()); // w
                                                               // Add more fields as needed...
                data
            }
            _ => {
                // Generic message data
                vec![0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07]
            }
        }
    }
}
