use rosbag2_rs::Reader;
use rosbag2_rs::cdr::CdrDeserializer;
use rosbag2_rs::error::Result;
use std::path::Path;

fn debug_pointstamped_message(data: &[u8]) -> Result<()> {
    println!("Raw CDR data ({} bytes): {:02x?}", data.len(), &data[..std::cmp::min(64, data.len())]);
    
    // Scan for f64 values that look like reasonable coordinates
    println!("\n=== Scanning for reasonable f64 values ===");
    for i in (4..data.len()-7).step_by(4) {
        if i + 8 <= data.len() {
            let bytes = &data[i..i+8];
            let value = f64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
            
            // Check if this looks like a reasonable coordinate
            if value.abs() < 1000.0 && value.abs() > 0.0001 {
                println!("Position {}: {:02x?} -> {}", i, bytes, value);
            }
        }
    }
    
    // Now let's manually parse the structure step by step
    let mut deserializer = CdrDeserializer::new(data)?;
    println!("\n=== Manual Structure Parsing ===");
    println!("After CDR header, position: {}", deserializer.position());
    
    // Read header
    println!("\n--- Header ---");
    let header_sec = deserializer.read_i32()?;
    println!("header.stamp.sec: {} (pos: {})", header_sec, deserializer.position());
    
    let header_nanosec = deserializer.read_u32()?;
    println!("header.stamp.nanosec: {} (pos: {})", header_nanosec, deserializer.position());
    
    let frame_id_len = deserializer.read_u32()?;
    println!("frame_id length: {} (pos: {})", frame_id_len, deserializer.position());
    
    // Read frame_id string
    let frame_id_bytes = &data[deserializer.position()..deserializer.position() + frame_id_len as usize];
    let frame_id = String::from_utf8_lossy(frame_id_bytes);
    println!("frame_id: '{}' (pos: {})", frame_id, deserializer.position());
    
    // Manually advance position
    let mut pos = deserializer.position() + frame_id_len as usize;
    println!("After frame_id, position: {}", pos);
    
    // Show the next 32 bytes to see the pattern
    println!("\nNext 32 bytes from position {}:", pos);
    for i in 0..32 {
        if pos + i < data.len() {
            print!("{:02x} ", data[pos + i]);
            if (i + 1) % 8 == 0 {
                println!();
            }
        }
    }
    println!();
    
    // Try to find the point data by looking for reasonable f64 values
    println!("\n--- Searching for Point data ---");
    
    // PointStamped structure:
    // - Header (already read)
    // - Point (x, y, z) - 3 f64 values
    
    // Try different starting positions for the point data
    for test_pos in [pos, pos + 4, pos + 8, pos + 12, pos + 16] {
        if test_pos + 24 <= data.len() { // 3 f64 values = 24 bytes
            println!("\nTrying position {}:", test_pos);
            
            // Point (x, y, z)
            let x_bytes = &data[test_pos..test_pos + 8];
            let x = f64::from_le_bytes([
                x_bytes[0], x_bytes[1], x_bytes[2], x_bytes[3],
                x_bytes[4], x_bytes[5], x_bytes[6], x_bytes[7],
            ]);
            
            let y_bytes = &data[test_pos + 8..test_pos + 16];
            let y = f64::from_le_bytes([
                y_bytes[0], y_bytes[1], y_bytes[2], y_bytes[3],
                y_bytes[4], y_bytes[5], y_bytes[6], y_bytes[7],
            ]);
            
            let z_bytes = &data[test_pos + 16..test_pos + 24];
            let z = f64::from_le_bytes([
                z_bytes[0], z_bytes[1], z_bytes[2], z_bytes[3],
                z_bytes[4], z_bytes[5], z_bytes[6], z_bytes[7],
            ]);
            
            println!("  Point: ({}, {}, {})", x, y, z);
            
            // Check if these look reasonable
            let reasonable = x.abs() < 1000.0 && y.abs() < 1000.0 && z.abs() < 1000.0;
            
            if reasonable {
                println!("  *** This looks like valid point data! ***");
            }
        }
    }
    
    Ok(())
}

fn main() -> Result<()> {
    let bag_path = Path::new("../scaled_07_30_00");
    let mut reader = Reader::new(bag_path)?;
    reader.open()?;
    
    // Find PointStamped topic
    let target_connections: Vec<_> = reader.connections()
        .iter()
        .filter(|conn| conn.topic == "/vio/altitude")
        .cloned()
        .collect();
    
    if target_connections.is_empty() {
        eprintln!("PointStamped topic not found");
        return Ok(());
    }
    
    let point_connection = &target_connections[0];
    println!("Found PointStamped topic: {}", point_connection.topic);
    println!("Message type: {}", point_connection.message_type);
    
    // Read first few messages
    let mut message_count = 0;
    match reader.messages_filtered(Some(&target_connections), None, None) {
        Ok(messages) => {
            for message_result in messages {
                match message_result {
                    Ok(message) => {
                        println!("\n{}", "=".repeat(60));
                        println!("PointStamped Message #{}", message_count + 1);
                        println!("Timestamp: {}", message.timestamp);
                        println!("Data size: {} bytes", message.data.len());
                        
                        if let Err(e) = debug_pointstamped_message(&message.data) {
                            println!("Error debugging message: {}", e);
                        }
                        
                        message_count += 1;
                        if message_count >= 3 {
                            break;
                        }
                    }
                    Err(e) => {
                        println!("Error reading message: {}", e);
                        break;
                    }
                }
            }
        }
        Err(e) => {
            println!("Error getting messages: {}", e);
        }
    }
    
    Ok(())
}
