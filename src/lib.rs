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
//! ```rust,no_run
//! use rosbags_rs::{Reader, ReaderError};
//! use std::path::Path;
//!
//! fn main() -> Result<(), ReaderError> {
//!     let bag_path = Path::new("/path/to/rosbag");
//!     let mut reader = Reader::new(bag_path)?;
//!
//!     reader.open()?;
//!
//!     println!("Bag contains {} messages", reader.message_count());
//!     println!("Duration: {:.2} seconds", reader.duration() as f64 / 1_000_000_000.0);
//!
//!     // List all topics
//!     for topic in reader.topics() {
//!         println!("Topic: {}, Type: {}, Count: {}",
//!                  topic.name, topic.message_type, topic.message_count);
//!     }
//!
//!     // Read all messages
//!     for message_result in reader.messages()? {
//!         let message = message_result?;
//!         println!("Topic: {}, Timestamp: {}, Data length: {}",
//!                  message.topic, message.timestamp, message.data.len());
//!     }
//!
//!     Ok(())
//! }
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

#[cfg(test)]
mod tests {
    #[test]
    fn test_library_compiles() {
        // Basic compilation test - this test ensures the library compiles correctly
    }
}
