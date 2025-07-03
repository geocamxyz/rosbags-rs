//! Example: Copy and filter ROS2 bag files
//!
//! This example demonstrates how to read an existing ROS2 bag file and write it to a new
//! location with optional topic filtering. It supports both SQLite3 and MCAP formats.
//!
//! Usage:
//!   cargo run --example copy_bag_filtered <input_bag> <output_bag> [--topics topic1,topic2,...]
//!
//! Arguments:
//!   input_bag   - Path to the source bag file
//!   output_bag  - Path where the filtered bag will be created  
//!   --topics    - Comma-separated list of topics to include (optional)
//!   --compression - Enable zstd compression for output
//!   --start     - Start timestamp in nanoseconds (optional)
//!   --end       - End timestamp in nanoseconds (optional)
//!
//! Examples:
//!   # Copy entire bag
//!   cargo run --example copy_bag_filtered ./input_bag ./output_bag
//!
//!   # Copy only specific topics
//!   cargo run --example copy_bag_filtered ./input_bag ./output_bag --topics /camera/image_raw,/imu/data
//!
//!   # Copy with time filtering
//!   cargo run --example copy_bag_filtered ./input_bag ./output_bag --start 1000000000 --end 2000000000
//!
//!   # Copy with compression
//!   cargo run --example copy_bag_filtered ./input_bag ./output_bag --compression

use rosbags_rs::{read_bag_metadata_fast, CompressionFormat, CompressionMode, Reader, StoragePlugin, Writer};
use std::collections::HashSet;
use std::env;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        eprintln!("Usage: {} <input_bag> <output_bag> [--topics topic1,topic2,...] [--compression] [--start timestamp] [--end timestamp]", args[0]);
        eprintln!("\nExamples:");
        eprintln!("  {} ./input_bag ./output_bag", args[0]);
        eprintln!(
            "  {} ./input_bag ./output_bag --topics /camera/image_raw,/imu/data",
            args[0]
        );
        eprintln!(
            "  {} ./input_bag ./output_bag --compression --start 1000000000 --end 2000000000",
            args[0]
        );
        std::process::exit(1);
    }

    let input_path = &args[1];
    let output_path = &args[2];

    // Parse arguments
    let mut topic_filter: Option<HashSet<String>> = None;
    let mut enable_compression = false;
    let mut start_time: Option<u64> = None;
    let mut end_time: Option<u64> = None;

    let mut i = 3;
    while i < args.len() {
        match args[i].as_str() {
            "--topics" => {
                if i + 1 < args.len() {
                    let topics: HashSet<String> = args[i + 1]
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
                    topic_filter = Some(topics);
                    i += 2;
                } else {
                    eprintln!("Error: --topics requires a value");
                    std::process::exit(1);
                }
            }
            "--compression" => {
                enable_compression = true;
                i += 1;
            }
            "--start" => {
                if i + 1 < args.len() {
                    start_time = Some(args[i + 1].parse()?);
                    i += 2;
                } else {
                    eprintln!("Error: --start requires a timestamp value");
                    std::process::exit(1);
                }
            }
            "--end" => {
                if i + 1 < args.len() {
                    end_time = Some(args[i + 1].parse()?);
                    i += 2;
                } else {
                    eprintln!("Error: --end requires a timestamp value");
                    std::process::exit(1);
                }
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                std::process::exit(1);
            }
        }
    }

    println!("🔄 Copying ROS2 bag: {} -> {}", input_path, output_path);

    if let Some(ref topics) = topic_filter {
        println!(
            "📝 Topic filter: {}",
            topics.iter().cloned().collect::<Vec<_>>().join(", ")
        );
    } else {
        println!("📝 Copying all topics");
    }

    if let Some(start) = start_time {
        println!("⏰ Start time filter: {} ns", start);
    }
    if let Some(end) = end_time {
        println!("⏰ End time filter: {} ns", end);
    }

    if enable_compression {
        println!("📦 Output compression: ENABLED (zstd)");
    }

    // First, read metadata quickly to show bag information
    println!("📖 Reading bag metadata...");
    let metadata = read_bag_metadata_fast(Path::new(input_path))?;
    
    println!("📊 Input bag information:");
    println!(
        "   Duration: {:.2} seconds",
        metadata.duration() as f64 / 1_000_000_000.0
    );
    println!("   Message count: {}", metadata.message_count());
    println!("   Topics: {}", metadata.info().topics_with_message_count.len());

    // Show available topics from metadata
    println!("\n📋 Available topics:");
    for topic in &metadata.info().topics_with_message_count {
        println!(
            "   {} ({}) - {} messages",
            topic.topic_metadata.name, 
            topic.topic_metadata.message_type, 
            topic.message_count
        );
    }

    // Now open the input bag for actual reading
    println!("\n📖 Opening input bag for reading...");
    let mut reader = Reader::new(Path::new(input_path))?;
    reader.open()?;

    // Create output writer
    println!("\n✍️  Creating output bag...");
    let storage_plugin = if input_path.contains(".mcap") || output_path.contains(".mcap") {
        StoragePlugin::Mcap
    } else {
        StoragePlugin::Sqlite3
    };

    let mut writer = Writer::new(output_path, Some(9), Some(storage_plugin))?;

    if enable_compression {
        writer.set_compression(CompressionMode::Message, CompressionFormat::Zstd)?;
    }

    // Configure buffer for optimal performance based on estimated message count
    let estimated_messages = metadata.message_count();
    let (buffer_size_mb, batch_threshold) = if estimated_messages > 100_000 {
        // Large bags: Use bigger buffer and batch size
        (50, 1000)
    } else if estimated_messages > 10_000 {
        // Medium bags: Use moderate buffer
        (20, 500)
    } else {
        // Small bags: Use smaller buffer for responsive feedback
        (10, 100)
    };
    
    writer.configure_buffer(buffer_size_mb, batch_threshold)?;
    println!("📊 Optimized buffer: {}MB, {} message batches", buffer_size_mb, batch_threshold);

    // Add metadata about the copy operation
    writer.set_custom_data("original_bag".to_string(), input_path.to_string())?;
    writer.set_custom_data(
        "copy_tool".to_string(),
        "rosbags-rs-copy-example".to_string(),
    )?;

    if let Some(ref topics) = topic_filter {
        writer.set_custom_data(
            "filtered_topics".to_string(),
            topics.iter().cloned().collect::<Vec<_>>().join(","),
        )?;
    }

    writer.open()?;

    // Filter connections based on topic filter
    let mut connection_map = std::collections::HashMap::new();
    let mut copied_connections = 0;

    for topic_info in reader.topics() {
        // Check if this topic should be included
        let include_topic = match &topic_filter {
            Some(filter) => filter.contains(&topic_info.name),
            None => true,
        };

        if include_topic {
            println!(
                "➕ Adding topic: {} ({})",
                topic_info.name, topic_info.message_type
            );

            // Add all connections for this topic
            for connection in &topic_info.connections {
                let new_connection = writer.add_connection(
                    connection.topic.clone(),
                    connection.message_type.clone(),
                    Some(connection.message_definition.clone()),
                    Some(connection.type_description_hash.clone()),
                    Some(connection.serialization_format.clone()),
                    Some(connection.offered_qos_profiles.clone()),
                )?;

                connection_map.insert(connection.id, new_connection);
                copied_connections += 1;
            }
        }
    }

    println!("✅ Added {} connections to output bag", copied_connections);

    // Copy messages
    println!("📨 Copying messages...");
    let mut copied_messages = 0;
    let mut filtered_messages = 0;

    for message_result in reader.messages()? {
        let message = message_result?;

        // Check time filter
        if let Some(start) = start_time {
            if message.timestamp < start {
                filtered_messages += 1;
                continue;
            }
        }

        if let Some(end) = end_time {
            if message.timestamp > end {
                filtered_messages += 1;
                continue;
            }
        }

        // Check if we have a corresponding connection in the output bag
        if let Some(output_connection) = connection_map.get(&message.connection.id) {
            writer.write(output_connection, message.timestamp, &message.data)?;
            copied_messages += 1;

            if copied_messages % batch_threshold == 0 {
                println!("  Copied {} messages (batch flushed)...", copied_messages);
            }
        } else {
            filtered_messages += 1;
        }
    }

    // Close bags
    writer.close()?;

    println!("🎉 Successfully copied bag!");
    println!("📊 Copy statistics:");
    println!("   Messages copied: {}", copied_messages);
    println!("   Messages filtered: {}", filtered_messages);
    println!("   Topics copied: {}", copied_connections);
    println!("📁 Output bag: {}", output_path);

    // Verify the output bag
    println!("\n🔍 Verifying output bag...");
    let mut verify_reader = Reader::new(Path::new(output_path))?;
    verify_reader.open()?;

    println!("✅ Output bag verification:");
    println!(
        "   Duration: {:.2} seconds",
        verify_reader.duration() as f64 / 1_000_000_000.0
    );
    println!("   Message count: {}", verify_reader.message_count());
    println!("   Topics: {}", verify_reader.topics().len());

    Ok(())
}
