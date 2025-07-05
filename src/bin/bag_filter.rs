//! Example: Copy and filter ROS2 bag files
//!
//! This example demonstrates how to read an existing ROS2 bag file and write it to a new
//! location with optional topic filtering. It supports both SQLite3 and MCAP formats.
//!
//! Usage:
//!   cargo run --bin bag_filter -- <input_bag> <output_bag> [--topics topic1,topic2,...]
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
//!   cargo run --bin bag_filter -- ./input_bag ./output_bag
//!
//!   # Copy only specific topics
//!   cargo run --bin bag_filter -- ./input_bag ./output_bag --topics /camera/image_raw,/imu/data
//!
//!   # Copy with time filtering
//!   cargo run --bin bag_filter -- ./input_bag ./output_bag --start 1000000000 --end 2000000000
//!
//!   # Copy with compression
//!   cargo run --bin bag_filter -- ./input_bag ./output_bag --compression

use anyhow::{Context, Result};
use clap::Parser;
use rosbags_rs::types::{CompressionFormat, CompressionMode, Connection, StoragePlugin};
use rosbags_rs::{Reader, Writer};
use std::collections::HashMap;
use std::path::PathBuf;

/// Arguments for copy functions
struct CopyArgs<'a> {
    connections: &'a [Connection],
    conn_map: &'a HashMap<String, Connection>,
    start: Option<u64>,
    end: Option<u64>,
    batch_size: usize,
    verbose: bool,
}

/// Copy a ROS2 bag file with optional topic filtering
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input bag directory
    input: PathBuf,

    /// Output bag directory
    output: PathBuf,

    /// Topics to include (comma-separated list, if empty, all topics are included)
    #[arg(short, long, value_delimiter = ',')]
    topics: Vec<String>,

    /// Topics to exclude (comma-separated list)
    #[arg(short = 'x', long = "exclude", value_delimiter = ',')]
    exclude_topics: Vec<String>,

    /// Start time (nanoseconds since epoch)
    #[arg(short, long)]
    start: Option<u64>,

    /// End time (nanoseconds since epoch)
    #[arg(short, long)]
    end: Option<u64>,

    /// Storage plugin to use for output (sqlite3 or mcap)
    #[arg(long, default_value = "sqlite3")]
    storage: String,

    /// Compression mode (none, file, or message)
    #[arg(long, default_value = "none")]
    compression_mode: String,

    /// Compression format (none or zstd)
    #[arg(long, default_value = "none")]
    compression_format: String,

    /// Use standard (slower) copying with deserialization/serialization.
    /// Default is a high-performance raw copy.
    #[arg(long)]
    standard_copy: bool,

    /// Buffer size in MB for raw copy mode (default: 50MB for high-throughput)
    #[arg(long, default_value = "50")]
    buffer_size_mb: usize,

    /// Batch size for bulk operations in raw copy mode
    #[arg(long, default_value = "1000")]
    batch_size: usize,

    /// List all topics in the bag and exit
    #[arg(long)]
    list_topics: bool,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Open input bag
    let mut reader = Reader::new(&args.input).context("Failed to create reader")?;
    reader.open().context("Failed to open input bag")?;

    // Get all connections
    let connections = reader.connections();

    // If user just wants to list topics, print them and exit
    if args.list_topics {
        println!("Available topics in bag:");
        for (i, conn) in connections.iter().enumerate() {
            println!("  {}: {} ({})", i + 1, conn.topic, conn.message_type);
        }
        println!("\nTotal topics: {}", connections.len());
        return Ok(());
    }

    if args.verbose {
        println!(
            "Copying bag from {} to {}",
            args.input.display(),
            args.output.display()
        );
        if !args.standard_copy {
            println!(
                "Using raw copy mode for maximum performance (buffer: {}MB, batch: {})",
                args.buffer_size_mb, args.batch_size
            );
        }
    }

    // Parse storage plugin
    let storage_plugin = match args.storage.as_str() {
        "sqlite3" => StoragePlugin::Sqlite3,
        "mcap" => StoragePlugin::Mcap,
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported storage plugin: {}. Use 'sqlite3' or 'mcap'",
                args.storage
            ));
        }
    };

    // Parse compression mode
    let compression_mode = match args.compression_mode.as_str() {
        "none" => CompressionMode::None,
        "file" => CompressionMode::File,
        "message" => CompressionMode::Message,
        "storage" => CompressionMode::Storage,
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported compression mode: {}. Use 'none', 'file', 'message', or 'storage'",
                args.compression_mode
            ));
        }
    };

    // Parse compression format
    let compression_format = match args.compression_format.as_str() {
        "none" => CompressionFormat::None,
        "zstd" => CompressionFormat::Zstd,
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported compression format: {}. Use 'none' or 'zstd'",
                args.compression_format
            ));
        }
    };

    // Create output bag
    let mut writer =
        Writer::new(&args.output, None, Some(storage_plugin)).context("Failed to create writer")?;
    writer.set_compression(compression_mode, compression_format)?;

    if !args.standard_copy {
        // Configure buffer for high-performance raw copying
        writer.configure_buffer(args.buffer_size_mb, args.batch_size)?;
    }

    writer.open().context("Failed to open output bag")?;

    // Filter connections
    let filtered_connections: Vec<_> = connections
        .iter()
        .filter(|conn| {
            // Include filter
            let include = if args.topics.is_empty() {
                true
            } else {
                args.topics.contains(&conn.topic)
            };

            // Exclude filter
            let exclude = args.exclude_topics.contains(&conn.topic);

            include && !exclude
        })
        .cloned()
        .collect();

    if filtered_connections.is_empty() {
        println!("No topics match the filter criteria");
        return Ok(());
    }

    if args.verbose {
        println!(
            "Selected {} topics for copying:",
            filtered_connections.len()
        );
        for conn in &filtered_connections {
            println!("  {} ({})", conn.topic, conn.message_type);
        }
    }

    // Create a map from reader topic to writer connection for fast lookup
    let mut conn_map = HashMap::new();
    for r_conn in &filtered_connections {
        let w_conn = writer.add_connection(
            r_conn.topic.clone(),
            r_conn.message_type.clone(),
            Some(r_conn.message_definition.clone()),
            Some(r_conn.type_description_hash.clone()),
            Some(r_conn.serialization_format.clone()),
            Some(r_conn.offered_qos_profiles.clone()),
        )?;
        // Use topic name as key since that's what we need to look up by
        conn_map.insert(r_conn.topic.clone(), w_conn);
    }

    let copy_args = CopyArgs {
        connections: &filtered_connections,
        conn_map: &conn_map,
        start: args.start,
        end: args.end,
        batch_size: args.batch_size,
        verbose: args.verbose,
    };

    if !args.standard_copy {
        // High-performance raw copy mode
        copy_raw_messages(&mut reader, &mut writer, &copy_args)?;
    } else {
        // Standard copy mode
        copy_messages(&mut reader, &mut writer, &copy_args)?;
    }

    // Close bags
    writer.close().context("Failed to close output bag")?;
    reader.close().context("Failed to close input bag")?;

    println!("Bag copy completed successfully");
    Ok(())
}

/// High-performance raw message copying (similar to ROS2 bag convert)
fn copy_raw_messages(reader: &mut Reader, writer: &mut Writer, args: &CopyArgs) -> Result<()> {
    if args.verbose {
        println!("Starting high-performance raw copy...");
    }
    let start_time = std::time::Instant::now();

    // Use batch reading for maximum performance
    let raw_messages = reader
        .read_raw_messages_batch(Some(args.connections), args.start, args.end)
        .context("Failed to read raw messages")?;

    if args.verbose {
        println!(
            "Read {} messages in {:?}",
            raw_messages.len(),
            start_time.elapsed()
        );
    }

    let write_start = std::time::Instant::now();

    // Convert to the format expected by write_raw_messages_batch, using the connection map
    let batch_messages: Result<Vec<(Connection, u64, Vec<u8>)>> = raw_messages
        .into_iter()
        .map(|msg| {
            let w_conn = args
                .conn_map
                .get(&msg.connection.topic)
                .with_context(|| {
                    format!(
                        "Connection for topic '{}' not found in writer",
                        msg.connection.topic
                    )
                })?
                .clone();
            Ok((w_conn, msg.timestamp, msg.raw_data))
        })
        .collect();

    // Process in batches to avoid memory pressure
    let mut total_written = 0;
    for chunk in batch_messages?.chunks(args.batch_size) {
        writer
            .write_raw_messages_batch(chunk)
            .context("Failed to write raw message batch")?;
        total_written += chunk.len();
    }

    if args.verbose {
        println!(
            "Wrote {} messages in {:?}",
            total_written,
            write_start.elapsed()
        );
        println!("Total time: {:?}", start_time.elapsed());
    }

    Ok(())
}

/// Standard message copying with deserialization/serialization
fn copy_messages(reader: &mut Reader, writer: &mut Writer, args: &CopyArgs) -> Result<()> {
    if args.verbose {
        println!("Starting standard copy...");
    }
    let start_time = std::time::Instant::now();

    let messages = reader
        .messages_filtered(Some(args.connections), args.start, args.end)
        .context("Failed to get message iterator")?;

    let mut count = 0;
    for message_result in messages {
        let message = message_result.context("Failed to read message")?;

        let w_conn = args
            .conn_map
            .get(&message.connection.topic)
            .with_context(|| {
                format!(
                    "Connection for topic '{}' not found in writer",
                    message.connection.topic
                )
            })?;

        writer
            .write(w_conn, message.timestamp, &message.data)
            .context("Failed to write message")?;

        count += 1;
    }

    if args.verbose {
        println!("Copied {} messages in {:?}", count, start_time.elapsed());
    }
    Ok(())
}
