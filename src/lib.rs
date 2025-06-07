//! # rosbag2-rs
//!
//! A Rust library for reading ROS2 bag files.
//!
//! This library provides functionality to read ROS2 bag files, supporting both SQLite3 and MCAP
//! storage formats. It focuses on reading capabilities and provides a safe, efficient interface
//! for accessing ROS2 bag data.
//!
//! ## Features
//!
//! - Read ROS2 bag files in SQLite3 format
//! - Parse metadata.yaml files
//! - Filter messages by topic, time range, and connections
//! - Support for compressed bag files (zstd)
//! - Type-safe message handling
//!
//! ## Example
//!
//! ```rust,no_run
//! use rosbag2_rs::{Reader, ReaderError};
//! use std::path::Path;
//!
//! fn main() -> Result<(), ReaderError> {
//!     let bag_path = Path::new("/path/to/rosbag");
//!     let mut reader = Reader::new(bag_path)?;
//!     
//!     reader.open()?;
//!     
//!     println!("Bag contains {} messages", reader.message_count());
//!     println!("Duration: {} ns", reader.duration());
//!     
//!     for topic in reader.topics() {
//!         println!("Topic: {}, Type: {}, Count: {}", 
//!                  topic.name, topic.message_type, topic.message_count);
//!     }
//!     
//!     for message_result in reader.messages()? {
//!         let message = message_result?;
//!         println!("Topic: {}, Timestamp: {}, Data length: {}",
//!                  message.topic, message.timestamp, message.data.len());
//!     }
//!     
//!     Ok(())
//! }
//! ```

pub mod cdr;
pub mod error;
pub mod messages;
pub mod metadata;
pub mod reader;
pub mod storage;
pub mod types;

// Re-export main types for convenience
pub use error::{ReaderError, Result};
pub use metadata::{BagMetadata, TopicMetadata};
pub use reader::Reader;
pub use types::{Connection, Message, TopicInfo};

#[cfg(test)]
mod tests {
    #[test]
    fn test_library_compiles() {
        // Basic compilation test
        assert!(true);
    }
}
