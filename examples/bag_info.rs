//! Example: Display comprehensive ROS2 bag file information
//!
//! This example provides detailed information about a ROS2 bag file including:
//! - Storage files and sizes
//! - Duration and timing information
//! - Topic details with message counts
//! - Human-readable timestamps
//!
//! Usage: cargo run --example bag_info <bag_path>

use rosbag2_rs::{Reader, ReaderError};
use std::env;
use std::path::Path;
use chrono::TimeZone;

fn main() -> Result<(), ReaderError> {
    // Get bag path from command line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <bag_path>", args[0]);
        eprintln!("Example: {} /path/to/rosbag2_directory", args[0]);
        std::process::exit(1);
    }

    let bag_path = Path::new(&args[1]);

    // Create and open the reader
    let mut reader = Reader::new(bag_path)?;
    reader.open()?;

    // Calculate storage file information
    let storage_files = get_storage_files(bag_path)?;
    let total_size = calculate_total_size(bag_path, &storage_files)?;
    let storage_id = detect_storage_id(&storage_files);

    // Get timing information
    let duration_ns = reader.duration();
    let duration_s = duration_ns as f64 / 1_000_000_000.0;
    let start_time_ns = reader.start_time();
    let end_time_ns = reader.end_time();

    // Print information in reference format
    println!("Files:             {}", format_file_list(&storage_files));
    println!("Bag size:          {}", format_size(total_size));
    println!("Storage id:        {}", storage_id);
    println!("Duration:          {:.9}s", duration_s);
    println!("Start:             {}", format_timestamp(start_time_ns));
    println!("End:               {}", format_timestamp(end_time_ns));
    println!("Messages:          {}", reader.message_count());

    // Print topic information
    let topics = reader.topics();
    if !topics.is_empty() {
        println!("Topic information: {}", format_first_topic(&topics[0]));
        for topic in topics.iter().skip(1) {
            println!("                   {}", format_topic(topic));
        }
    }

    // Close the reader
    reader.close()?;

    Ok(())
}

/// Get list of storage files in the bag directory
fn get_storage_files(bag_path: &Path) -> Result<Vec<String>, ReaderError> {
    let mut files = Vec::new();

    if let Ok(entries) = std::fs::read_dir(bag_path) {
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                if let Some(extension) = path.extension() {
                    if extension == "db3" || extension == "mcap" {
                        if let Some(filename) = path.file_name() {
                            if let Some(filename_str) = filename.to_str() {
                                files.push(filename_str.to_string());
                            }
                        }
                    }
                }
            }
        }
    }

    files.sort();
    Ok(files)
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

/// Detect storage identifier from file extensions
fn detect_storage_id(files: &[String]) -> String {
    for file in files {
        if file.ends_with(".db3") {
            return "sqlite3".to_string();
        } else if file.ends_with(".mcap") {
            return "mcap".to_string();
        }
    }
    "unknown".to_string()
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

    if let Some(datetime) = chrono::Utc.timestamp_opt(timestamp_s, timestamp_ns_frac).single() {
        format!("{} ({}.{:09})",
                datetime.format("%b %e %Y %H:%M:%S%.9f"),
                timestamp_s,
                timestamp_ns_frac)
    } else {
        format!("Invalid timestamp ({}.{:09})", timestamp_s, timestamp_ns_frac)
    }
}

/// Format first topic with "Topic:" prefix
fn format_first_topic(topic: &rosbag2_rs::types::TopicInfo) -> String {
    format!("Topic: {} | Type: {} | Count: {} | Serialization Format: cdr",
            topic.name, topic.message_type, topic.message_count)
}

/// Format subsequent topics with proper alignment
fn format_topic(topic: &rosbag2_rs::types::TopicInfo) -> String {
    format!("Topic: {} | Type: {} | Count: {} | Serialization Format: cdr",
            topic.name, topic.message_type, topic.message_count)
}
