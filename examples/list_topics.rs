//! Example: List all topics in a ROS2 bag file

use rosbag2_rs::{Reader, ReaderError};
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
    
    // Create and open the reader
    let mut reader = Reader::new(bag_path)?;
    reader.open()?;

    // Get and display topics
    let topics = reader.topics();
    
    if topics.is_empty() {
        println!("No topics found in bag: {}", bag_path.display());
        return Ok(());
    }

    println!("Topics in bag: {}", bag_path.display());
    println!("{:-<80}", "");
    println!("{:<30} {:<40} {:<10}", "Topic Name", "Message Type", "Count");
    println!("{:-<80}", "");

    // Sort topics by name for consistent output
    let mut sorted_topics = topics;
    sorted_topics.sort_by(|a, b| a.name.cmp(&b.name));

    let mut total_messages = 0;
    for topic in &sorted_topics {
        println!("{:<30} {:<40} {:<10}", 
                 topic.name, 
                 topic.message_type, 
                 topic.message_count);
        total_messages += topic.message_count;
    }

    println!("{:-<80}", "");
    println!("Total topics: {}", sorted_topics.len());
    println!("Total messages: {}", total_messages);

    // Show detailed information if requested
    if env::var("VERBOSE").is_ok() {
        println!("\n{:=<80}", "");
        println!("DETAILED TOPIC INFORMATION");
        println!("{:=<80}", "");

        for topic in &sorted_topics {
            println!("\nTopic: {}", topic.name);
            println!("  Message Type: {}", topic.message_type);
            println!("  Message Count: {}", topic.message_count);
            println!("  Connections: {}", topic.connections.len());
            
            for (i, conn) in topic.connections.iter().enumerate() {
                println!("    Connection {}: ID={}, Serialization={}", 
                         i + 1, conn.id, conn.serialization_format);
                if !conn.type_description_hash.is_empty() {
                    println!("      Type Hash: {}", conn.type_description_hash);
                }
                if !conn.offered_qos_profiles.is_empty() {
                    println!("      QoS Profiles: {}", conn.offered_qos_profiles.len());
                }
            }
        }
    } else {
        println!("\nTip: Set VERBOSE=1 environment variable for detailed information");
    }

    reader.close()?;
    Ok(())
}
