//! # rosbags-rs
//!
//! A high-performance Rust library for reading ROS2 bag files with full Python rosbags compatibility.
//!
//! This library provides comprehensive functionality to read ROS2 bag files, supporting both SQLite3 and MCAP
//! storage formats. It focuses on reading capabilities and provides a safe, efficient interface
//! for accessing ROS2 bag data with guaranteed compatibility with the Python rosbags library.
//!
//! ## Features
//!
//! - ✅ **Read ROS2 bag files** in SQLite3 and MCAP formats
//! - ✅ **Parse metadata.yaml** files with full validation
//! - ✅ **Filter messages** by topic, time range, and connections
//! - ✅ **Compression support** for zstd compressed bags
//! - ✅ **Type-safe message handling** with comprehensive error handling
//! - ✅ **Cross-compatibility** with Python rosbags library (byte-for-byte identical results)
//! - ✅ **94+ ROS2 message types** supported across all major categories
//! - ✅ **High performance** with zero-copy message reading where possible
//!
//! ## Supported ROS2 Versions
//!
//! - ROS2 Humble Hawksbill
//! - ROS2 Iron Irwini
//! - ROS2 Jazzy Jalopy
//! - ROS2 Rolling Ridley
//!
//! ## Quick Start
//!
//! ### Reading a bag
//! ```no_run
//! use rosbags_rs::Reader;
//! use std::path::Path;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut reader = Reader::new(Path::new("path/to/bag"))?;
//! reader.open()?;
//!
//! println!("Bag duration: {:.2}s", reader.duration() as f64 / 1e9);
//! println!("Topics: {}", reader.topics().len());
//!
//! for message_result in reader.messages()? {
//!     let message = message_result?;
//!     println!("Topic: {}, Time: {}", message.connection.topic, message.timestamp);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ### Writing a bag with performance optimization
//! ```no_run
//! use rosbags_rs::{Writer, StoragePlugin};
//! use std::path::Path;
//! 
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut writer = Writer::new("output_bag", None, Some(StoragePlugin::Sqlite3))?;
//! 
//! // Configure high-performance buffering (20MB buffer, 500 message batches)
//! writer.configure_buffer(20, 500)?;
//! 
//! writer.open()?;
//! 
//! let connection = writer.add_connection(
//!     "/my_topic".to_string(),
//!     "std_msgs/msg/String".to_string(),
//!     None, None, None, None
//! )?;
//! 
//! // Write messages - automatically batched for optimal performance
//! for i in 0..1000 {
//!     let timestamp = 1000000000u64 + i * 100000000; // 100ms intervals
//!     writer.write(&connection, timestamp, b"hello")?;
//! }
//! 
//! writer.close()?; // Automatically flushes remaining buffered messages
//! # Ok(())
//! # }
//! ```
//!
//! ### Fast metadata reading
//! ```no_run
//! use rosbags_rs::read_bag_metadata_fast;
//! use std::path::Path;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let metadata = read_bag_metadata_fast(Path::new("path/to/bag"))?;
//! 
//! println!("Duration: {:.2}s", metadata.duration() as f64 / 1e9);
//! println!("Message count: {}", metadata.message_count());
//! println!("Topics: {}", metadata.info().topics_with_message_count.len());
//! # Ok(())
//! # }
//! ```
//!
//! ## Advanced Usage
//!
//! ### Filter by Topic
//!
//! ```rust,no_run
//! use rosbags_rs::Reader;
//! # use rosbags_rs::ReaderError;
//! # fn main() -> Result<(), ReaderError> {
//! # let bag_path = std::path::Path::new("/path/to/rosbag");
//! let mut reader = Reader::new(bag_path)?;
//! reader.open()?;
//!
//! // Filter messages for specific topics
//! let target_topics = vec!["/camera/image_raw", "/imu/data"];
//! for message_result in reader.messages()? {
//!     let message = message_result?;
//!     if target_topics.contains(&message.topic.as_str()) {
//!         println!("Found message on topic: {}", message.topic);
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ### Time Range Filtering
//!
//! ```rust,no_run
//! use rosbags_rs::Reader;
//! # use rosbags_rs::ReaderError;
//! # fn main() -> Result<(), ReaderError> {
//! # let bag_path = std::path::Path::new("/path/to/rosbag");
//! let mut reader = Reader::new(bag_path)?;
//! reader.open()?;
//!
//! let start_time = 1000000000; // nanoseconds
//! let end_time = 2000000000;
//!
//! for message_result in reader.messages()? {
//!     let message = message_result?;
//!     if message.timestamp >= start_time && message.timestamp <= end_time {
//!         println!("Message in time range: {}", message.timestamp);
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Supported Message Types
//!
//! The library supports 94+ ROS2 message types including:
//!
//! - **std_msgs**: String, Header, Int32, Float64, etc.
//! - **geometry_msgs**: Point, Pose, Transform, Twist, etc.
//! - **sensor_msgs**: Image, PointCloud2, Imu, NavSatFix, etc.
//! - **nav_msgs**: Odometry, Path, MapMetaData, etc.
//! - **diagnostic_msgs**: DiagnosticArray, DiagnosticStatus, etc.
//! - **builtin_interfaces**: Time, Duration
//!
//! ## Cross-Compatibility
//!
//! This library guarantees byte-for-byte identical results compared to the Python rosbags library,
//! making it a drop-in replacement for performance-critical applications.

/// Core CDR (Common Data Representation) deserialization functionality.
///
/// This module provides efficient deserialization of ROS2 message data from CDR format.
pub mod cdr;

/// Comprehensive error types and handling.
///
/// All library operations return structured errors that can be matched and handled appropriately.
pub mod error;

/// ROS2 message type definitions.
///
/// Contains Rust definitions for common ROS2 message types with full CDR deserialization support.
pub mod messages;

/// Metadata parsing and validation.
///
/// Handles parsing of `metadata.yaml` files and validation of bag metadata.
pub mod metadata;

/// Main reader interface.
///
/// The [`Reader`] struct provides the primary interface for reading ROS2 bag files.
pub mod reader;

/// Main writer interface.
///
/// The [`Writer`] struct provides the primary interface for writing ROS2 bag files.
pub mod writer;

/// Storage backend implementations.
///
/// Supports both SQLite3 and MCAP storage formats with pluggable architecture.
pub mod storage;

/// Core data types and structures.
///
/// Defines the fundamental types used throughout the library.
pub mod types;

// Re-export main types for convenience
pub use error::{BagError, ReaderError, Result, WriterResult};
pub use metadata::{BagMetadata, TopicMetadata};
pub use reader::Reader;
pub use types::{
    CompressionFormat, CompressionMode, Connection, Message, StoragePlugin, TopicInfo,
};
pub use writer::Writer;

/// Fast bag metadata reading without opening storage files
/// 
/// This function reads only the metadata.yaml file to quickly extract bag information
/// without the overhead of opening and parsing storage files. This is ideal for
/// getting basic bag information like duration, message count, and topic list.
/// 
/// # Arguments
/// * `bag_path` - Path to the ROS2 bag directory
/// 
/// # Returns
/// * `BagMetadata` containing all bag information from metadata.yaml
/// 
/// # Example
/// ```no_run
/// use rosbags_rs::read_bag_metadata_fast;
/// use std::path::Path;
/// 
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let metadata = read_bag_metadata_fast(Path::new("path/to/bag"))?;
/// 
/// println!("Duration: {:.2}s", metadata.duration() as f64 / 1e9);
/// println!("Message count: {}", metadata.message_count());
/// println!("Start time: {}", metadata.start_time());
/// println!("End time: {}", metadata.end_time());
/// 
/// for topic in &metadata.info().topics_with_message_count {
///     println!("Topic: {} ({}), Count: {}", 
///         topic.topic_metadata.name,
///         topic.topic_metadata.message_type,
///         topic.message_count
///     );
/// }
/// # Ok(())
/// # }
/// ```
pub fn read_bag_metadata_fast<P: AsRef<std::path::Path>>(bag_path: P) -> Result<BagMetadata> {
    let bag_path = bag_path.as_ref();
    let metadata_path = bag_path.join("metadata.yaml");
    
    BagMetadata::from_file(metadata_path)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_library_compiles() {
        // Basic compilation test - this test ensures the library compiles correctly
    }
}
