#![cfg(not(feature = "write-only"))]
//! Example: Display comprehensive ROS2 bag file information
//!
//! This example provides detailed information about a ROS2 bag file including:
//! - Storage files and sizes
//! - Duration and timing information
//! - Topic details with message counts
//! - Human-readable timestamps
//!
//! This version is optimized for speed by reading only the metadata.yaml file
//! without opening the storage files, making it much faster for large bags.
//!
//! Usage: cargo run --bin bag_info <bag_path>

use chrono::TimeZone;
use rosbags_rs::{read_bag_metadata_fast, ReaderError};
use std::env;
use std::path::Path;

fn main() -> Result<(), ReaderError> {
    // Get bag path from command line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <bag_path>", args[0]);
        eprintln!("Example: {} /path/to/rosbag2_directory", args[0]);
        std::process::exit(1);
    }

    let bag_path = Path::new(&args[1]);

    // Read metadata directly - this is very fast as it only reads metadata.yaml
    let metadata = read_bag_metadata_fast(bag_path)?;
    let info = metadata.info();

    // Calculate storage file information
    let storage_files = &info.relative_file_paths;
    let total_size = calculate_total_size(bag_path, storage_files)?;
    let storage_id = get_storage_id(info);

    // Get timing information directly from metadata
    let duration_ns = metadata.duration();
    let duration_s = duration_ns as f64 / 1_000_000_000.0;
    let start_time_ns = metadata.start_time();
    let end_time_ns = metadata.end_time();
    let message_count = metadata.message_count();

    // Print information in reference format
    println!("Files:             {}", format_file_list(storage_files));
    println!("Bag size:          {}", format_size(total_size));
    println!("Storage id:        {storage_id}");
    println!("Duration:          {duration_s:.9}s");
    println!("Start:             {}", format_timestamp(start_time_ns));
    println!("End:               {}", format_timestamp(end_time_ns));
    println!("Messages:          {message_count}");

    // Print topic information directly from metadata
    if !info.topics_with_message_count.is_empty() {
        println!(
            "Topic information: {}",
            format_first_topic_from_metadata(&info.topics_with_message_count[0])
        );
        for topic in info.topics_with_message_count.iter().skip(1) {
            println!("                   {}", format_topic_from_metadata(topic));
        }
    }

    Ok(())
}

/// Calculate total size of storage files
fn calculate_total_size(bag_path: &Path, files: &[String]) -> Result<u64, ReaderError> {
    let mut total_size = 0u64;

    for file in files {
        let file_path = bag_path.join(file);
        if let Ok(metadata) = std::fs::metadata(&file_path) {
            total_size += metadata.len();
        }
    }

    Ok(total_size)
}

/// Get storage identifier from metadata
fn get_storage_id(info: &rosbags_rs::metadata::BagFileInformation) -> String {
    if !info.storage_identifier.is_empty() {
        info.storage_identifier.clone()
    } else {
        // Auto-detect from file extensions
        for file in &info.relative_file_paths {
            if file.ends_with(".db3") {
                return "sqlite3".to_string();
            } else if file.ends_with(".mcap") {
                return "mcap".to_string();
            }
        }
        "unknown".to_string()
    }
}

/// Format file list for display
fn format_file_list(files: &[String]) -> String {
    files.join(", ")
}

/// Format size in human-readable format
fn format_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.1} {}", size, UNITS[unit_index])
    }
}

/// Format timestamp in human-readable format
fn format_timestamp(timestamp_ns: u64) -> String {
    let timestamp_s = (timestamp_ns / 1_000_000_000) as i64;
    let timestamp_ns_frac = (timestamp_ns % 1_000_000_000) as u32;

    if let Some(datetime) = chrono::Utc
        .timestamp_opt(timestamp_s, timestamp_ns_frac)
        .single()
    {
        format!(
            "{} ({}.{:09})",
            datetime.format("%b %e %Y %H:%M:%S%.9f"),
            timestamp_s,
            timestamp_ns_frac
        )
    } else {
        format!("Invalid timestamp ({timestamp_s}.{timestamp_ns_frac:09})")
    }
}

/// Format first topic with "Topic:" prefix from metadata
fn format_first_topic_from_metadata(topic: &rosbags_rs::metadata::TopicWithMessageCount) -> String {
    format!(
        "Topic: {} | Type: {} | Count: {} | Serialization Format: {}",
        topic.topic_metadata.name,
        topic.topic_metadata.message_type,
        topic.message_count,
        topic.topic_metadata.serialization_format
    )
}

/// Format subsequent topics with proper alignment from metadata
fn format_topic_from_metadata(topic: &rosbags_rs::metadata::TopicWithMessageCount) -> String {
    format!(
        "Topic: {} | Type: {} | Count: {} | Serialization Format: {}",
        topic.topic_metadata.name,
        topic.topic_metadata.message_type,
        topic.message_count,
        topic.topic_metadata.serialization_format
    )
}
