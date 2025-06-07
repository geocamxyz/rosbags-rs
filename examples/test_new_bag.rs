use rosbag2_rs::Reader;

fn main() {
    println!("Testing new bag file...");
    
    match Reader::new(std::path::Path::new("../rosbag2_2025_06_03-09_28_50")) {
        Ok(mut reader) => {
            println!("Reader created successfully");
            
            match reader.open() {
                Ok(()) => {
                    println!("Bag opened successfully");
                    println!("Duration: {:.2} seconds", reader.duration() as f64 / 1_000_000_000.0);
                    println!("Total messages: {}", reader.message_count());
                    
                    println!("\nTopics:");
                    for topic in reader.topics() {
                        println!("  - {} ({}): {} messages", topic.name, topic.message_type, topic.message_count);
                    }
                }
                Err(e) => {
                    println!("Failed to open bag: {}", e);
                }
            }
        }
        Err(e) => {
            println!("Failed to create reader: {}", e);
        }
    }
}
