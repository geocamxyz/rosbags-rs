use rosbag2_rs::Reader;
use rosbag2_rs::cdr::CdrDeserializer;
use rosbag2_rs::error::Result;
use std::path::Path;

fn debug_pose_message(data: &[u8]) -> Result<()> {
    println!("Raw CDR data ({} bytes): {:02x?}", data.len(), &data[..std::cmp::min(128, data.len())]);
    
    // Scan for f64 values that look like reasonable coordinates or quaternion components
    println!("\n=== Scanning for reasonable f64 values ===");
    for i in (4..data.len()-7).step_by(4) {
        if i + 8 <= data.len() {
            let bytes = &data[i..i+8];
            let value = f64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
            
            // Check if this looks like a reasonable coordinate or quaternion component
            if (value >= -100.0 && value <= 100.0 && value.abs() > 0.0001) || (value >= -1.0 && value <= 1.0 && value.abs() > 0.0001) {
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
    let pos = deserializer.position() + frame_id_len as usize;
    println!("After frame_id, position: {}", pos);
    
    // Show the next 128 bytes to see the pattern
    println!("\nNext 128 bytes from position {}:", pos);
    for i in 0..128 {
        if pos + i < data.len() {
            print!("{:02x} ", data[pos + i]);
            if (i + 1) % 16 == 0 {
                println!();
            }
        }
    }
    println!();
    
    // Try to find the pose data by looking for reasonable f64 values
    println!("\n--- Searching for Pose data ---");
    
    // Try different starting positions for the pose data
    for test_pos in [pos, pos + 4, pos + 8, pos + 12, pos + 16, pos + 20, pos + 24, pos + 28, pos + 32] {
        if test_pos + 56 <= data.len() { // 7 f64 values = 56 bytes
            println!("\nTrying position {}:", test_pos);
            
            // Position (x, y, z)
            let pos_x_bytes = &data[test_pos..test_pos + 8];
            let pos_x = f64::from_le_bytes([
                pos_x_bytes[0], pos_x_bytes[1], pos_x_bytes[2], pos_x_bytes[3],
                pos_x_bytes[4], pos_x_bytes[5], pos_x_bytes[6], pos_x_bytes[7],
            ]);
            
            let pos_y_bytes = &data[test_pos + 8..test_pos + 16];
            let pos_y = f64::from_le_bytes([
                pos_y_bytes[0], pos_y_bytes[1], pos_y_bytes[2], pos_y_bytes[3],
                pos_y_bytes[4], pos_y_bytes[5], pos_y_bytes[6], pos_y_bytes[7],
            ]);
            
            let pos_z_bytes = &data[test_pos + 16..test_pos + 24];
            let pos_z = f64::from_le_bytes([
                pos_z_bytes[0], pos_z_bytes[1], pos_z_bytes[2], pos_z_bytes[3],
                pos_z_bytes[4], pos_z_bytes[5], pos_z_bytes[6], pos_z_bytes[7],
            ]);
            
            // Orientation (x, y, z, w)
            let ori_x_bytes = &data[test_pos + 24..test_pos + 32];
            let ori_x = f64::from_le_bytes([
                ori_x_bytes[0], ori_x_bytes[1], ori_x_bytes[2], ori_x_bytes[3],
                ori_x_bytes[4], ori_x_bytes[5], ori_x_bytes[6], ori_x_bytes[7],
            ]);
            
            let ori_y_bytes = &data[test_pos + 32..test_pos + 40];
            let ori_y = f64::from_le_bytes([
                ori_y_bytes[0], ori_y_bytes[1], ori_y_bytes[2], ori_y_bytes[3],
                ori_y_bytes[4], ori_y_bytes[5], ori_y_bytes[6], ori_y_bytes[7],
            ]);
            
            let ori_z_bytes = &data[test_pos + 40..test_pos + 48];
            let ori_z = f64::from_le_bytes([
                ori_z_bytes[0], ori_z_bytes[1], ori_z_bytes[2], ori_z_bytes[3],
                ori_z_bytes[4], ori_z_bytes[5], ori_z_bytes[6], ori_z_bytes[7],
            ]);
            
            let ori_w_bytes = &data[test_pos + 48..test_pos + 56];
            let ori_w = f64::from_le_bytes([
                ori_w_bytes[0], ori_w_bytes[1], ori_w_bytes[2], ori_w_bytes[3],
                ori_w_bytes[4], ori_w_bytes[5], ori_w_bytes[6], ori_w_bytes[7],
            ]);
            
            println!("  Position: ({}, {}, {})", pos_x, pos_y, pos_z);
            println!("  Orientation: ({}, {}, {}, {})", ori_x, ori_y, ori_z, ori_w);
            
            // Check if these look reasonable
            let pos_reasonable = pos_x.abs() < 1000.0 && pos_y.abs() < 1000.0 && pos_z.abs() < 1000.0;
            let ori_reasonable = ori_x.abs() <= 1.0 && ori_y.abs() <= 1.0 && ori_z.abs() <= 1.0 && ori_w.abs() <= 1.0;
            let ori_magnitude = (ori_x*ori_x + ori_y*ori_y + ori_z*ori_z + ori_w*ori_w).sqrt();
            let ori_unit = (ori_magnitude - 1.0).abs() < 0.1; // Allow some tolerance
            
            if pos_reasonable && ori_reasonable && ori_unit {
                println!("  *** This looks like valid pose data! ***");
            } else if pos_reasonable && ori_reasonable {
                println!("  *** Position and orientation ranges look good (magnitude: {:.3}) ***", ori_magnitude);
            }
        }
    }
    
    Ok(())
}

fn main() -> Result<()> {
    let bag_path = Path::new("../scaled_07_30_00");
    let mut reader = Reader::new(bag_path)?;
    reader.open()?;
    
    // Find PoseWithCovarianceStamped topic
    let target_connections: Vec<_> = reader.connections()
        .iter()
        .filter(|conn| conn.topic == "/vio/relative_pwc")
        .cloned()
        .collect();
    
    if target_connections.is_empty() {
        eprintln!("PoseWithCovarianceStamped topic not found");
        return Ok(());
    }
    
    let pose_connection = &target_connections[0];
    println!("Found PoseWithCovarianceStamped topic: {}", pose_connection.topic);
    println!("Message type: {}", pose_connection.message_type);
    
    // Read first few messages
    let mut message_count = 0;
    match reader.messages_filtered(Some(&target_connections), None, None) {
        Ok(messages) => {
            for message_result in messages {
                match message_result {
                    Ok(message) => {
                        println!("\n{}", "=".repeat(60));
                        println!("PoseWithCovarianceStamped Message #{}", message_count + 1);
                        println!("Timestamp: {}", message.timestamp);
                        println!("Data size: {} bytes", message.data.len());
                        
                        if let Err(e) = debug_pose_message(&message.data) {
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
