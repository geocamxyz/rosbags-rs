//! Example demonstrating how to read both SQLite3 and MCAP test bag files
//!
//! This example shows how to:
//! 1. Load and read SQLite3 format bag files
//! 2. Load and read MCAP format bag files  
//! 3. Extract and display message data from both formats
//! 4. Compare the data between formats

use rosbags_rs::{Reader, ReaderError};
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== ROS2 Bag Reader Example ===\n");

    // Test bag file paths
    let sqlite3_bag = "tests/test_bags/test_bag_sqlite3";
    #[cfg(feature = "mcap")]
    let mcap_bag = "tests/test_bags/test_bag_mcap";

    // Read SQLite3 bag
    println!("📁 Reading SQLite3 bag: {}", sqlite3_bag);
    let sqlite3_data = read_bag_file(sqlite3_bag)?;
    print_bag_summary("SQLite3", &sqlite3_data);

    // Read MCAP bag (if MCAP feature is enabled)
    #[cfg(feature = "mcap")]
    {
        println!("\n📁 Reading MCAP bag: {}", mcap_bag);
        let mcap_data = read_bag_file(mcap_bag)?;
        print_bag_summary("MCAP", &mcap_data);

        // Compare the two formats
        println!("\n🔍 Comparing bag formats:");
        compare_bag_data(&sqlite3_data, &mcap_data);
    }

    #[cfg(not(feature = "mcap"))]
    {
        println!(
            "\n⚠️  MCAP support not enabled. Compile with --features mcap to test MCAP format."
        );
    }

    // Demonstrate message filtering
    println!("\n🔍 Demonstrating message filtering:");
    demonstrate_filtering(sqlite3_bag)?;

    println!("\n✅ Example completed successfully!");
    Ok(())
}

/// Data structure to hold extracted bag information
#[derive(Debug)]
struct BagData {
    storage_format: String,
    topic_count: usize,
    message_count: usize,
    duration_ns: u64,
    topics_by_type: HashMap<String, Vec<String>>,
    sample_messages: Vec<SampleMessage>,
}

#[derive(Debug)]
struct SampleMessage {
    topic: String,
    msgtype: String,
    data_size: usize,
}

/// Read and extract data from a bag file
fn read_bag_file(bag_path: &str) -> Result<BagData, ReaderError> {
    let mut reader = Reader::new(bag_path)?;
    reader.open()?;

    let metadata = reader.metadata().unwrap();
    let info = metadata.info();

    // Group topics by message type
    let mut topics_by_type: HashMap<String, Vec<String>> = HashMap::new();
    for connection in reader.connections() {
        topics_by_type
            .entry(connection.msgtype().to_string())
            .or_default()
            .push(connection.topic.clone());
    }

    // Read sample messages (first 10)
    let mut sample_messages = Vec::new();
    for (count, message_result) in (reader.messages()?).enumerate() {
        if count >= 10 {
            break;
        }

        let message = message_result?;

        // Find the connection for this message
        let connection = reader
            .connections()
            .iter()
            .find(|c| c.topic == message.topic)
            .unwrap();

        sample_messages.push(SampleMessage {
            topic: message.topic.clone(),
            msgtype: connection.msgtype().to_string(),
            data_size: message.data.len(),
        });
    }

    Ok(BagData {
        storage_format: info.storage_identifier.clone(),
        topic_count: reader.connections().len(),
        message_count: info.message_count as usize,
        duration_ns: info.duration.nanoseconds,
        topics_by_type,
        sample_messages,
    })
}

/// Print a summary of bag data
fn print_bag_summary(format_name: &str, data: &BagData) {
    println!("  Format: {}", format_name);
    println!("  Storage ID: {}", data.storage_format);
    println!("  Topics: {}", data.topic_count);
    println!("  Messages: {}", data.message_count);
    println!("  Duration: {:.2} seconds", data.duration_ns as f64 / 1e9);

    println!("  Message types:");
    let mut sorted_types: Vec<_> = data.topics_by_type.iter().collect();
    sorted_types.sort_by_key(|(msgtype, _)| *msgtype);

    for (msgtype, topics) in sorted_types.iter().take(10) {
        println!("    {} ({} topics)", msgtype, topics.len());
    }

    if data.topics_by_type.len() > 10 {
        println!(
            "    ... and {} more message types",
            data.topics_by_type.len() - 10
        );
    }

    println!("  Sample messages:");
    for (i, msg) in data.sample_messages.iter().enumerate() {
        println!(
            "    {}: {} ({}) - {} bytes",
            i + 1,
            msg.topic,
            msg.msgtype,
            msg.data_size
        );
    }
}

/// Compare data between two bag formats
#[cfg(feature = "mcap")]
fn compare_bag_data(sqlite3_data: &BagData, mcap_data: &BagData) {
    println!(
        "  Topic count: SQLite3={}, MCAP={} {}",
        sqlite3_data.topic_count,
        mcap_data.topic_count,
        if sqlite3_data.topic_count == mcap_data.topic_count {
            "✅"
        } else {
            "❌"
        }
    );

    println!(
        "  Message count: SQLite3={}, MCAP={} {}",
        sqlite3_data.message_count,
        mcap_data.message_count,
        if sqlite3_data.message_count == mcap_data.message_count {
            "✅"
        } else {
            "❌"
        }
    );

    println!(
        "  Duration: SQLite3={:.2}s, MCAP={:.2}s {}",
        sqlite3_data.duration_ns as f64 / 1e9,
        mcap_data.duration_ns as f64 / 1e9,
        if sqlite3_data.duration_ns == mcap_data.duration_ns {
            "✅"
        } else {
            "❌"
        }
    );

    // Compare message types
    let sqlite3_types: std::collections::HashSet<_> = sqlite3_data.topics_by_type.keys().collect();
    let mcap_types: std::collections::HashSet<_> = mcap_data.topics_by_type.keys().collect();

    println!(
        "  Message types match: {}",
        if sqlite3_types == mcap_types {
            "✅"
        } else {
            "❌"
        }
    );

    if sqlite3_types != mcap_types {
        let only_sqlite3: Vec<_> = sqlite3_types.difference(&mcap_types).collect();
        let only_mcap: Vec<_> = mcap_types.difference(&sqlite3_types).collect();

        if !only_sqlite3.is_empty() {
            println!("    Only in SQLite3: {:?}", only_sqlite3);
        }
        if !only_mcap.is_empty() {
            println!("    Only in MCAP: {:?}", only_mcap);
        }
    }
}

/// Demonstrate message filtering capabilities
fn demonstrate_filtering(bag_path: &str) -> Result<(), ReaderError> {
    let mut reader = Reader::new(bag_path)?;
    reader.open()?;

    let connections = reader.connections();

    // Filter by a specific topic
    let test_topic = "/test/std_msgs/string";
    println!("  Filtering by topic: {}", test_topic);

    let filtered_connections: Vec<_> = connections
        .iter()
        .filter(|c| c.topic == test_topic)
        .cloned()
        .collect();

    if !filtered_connections.is_empty() {
        let mut count = 0;
        for message_result in reader.messages_filtered(Some(&filtered_connections), None, None)? {
            let message = message_result?;
            println!(
                "    Message {}: {} bytes at timestamp {}",
                count + 1,
                message.data.len(),
                message.timestamp
            );
            count += 1;
        }
        println!("    Total messages for this topic: {}", count);
    } else {
        println!("    Topic not found in bag");
    }

    // Filter by message type
    let test_msgtype = "geometry_msgs/msg/Point";
    println!("  Filtering by message type: {}", test_msgtype);

    let type_filtered_connections: Vec<_> = connections
        .iter()
        .filter(|c| c.msgtype() == test_msgtype)
        .cloned()
        .collect();

    if !type_filtered_connections.is_empty() {
        let mut count = 0;
        for message_result in
            reader.messages_filtered(Some(&type_filtered_connections), None, None)?
        {
            let message = message_result?;
            println!("    Topic: {}, {} bytes", message.topic, message.data.len());
            count += 1;
        }
        println!("    Total messages for this type: {}", count);
    } else {
        println!("    Message type not found in bag");
    }

    Ok(())
}
