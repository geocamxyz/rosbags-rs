use rosbag2_rs::Reader;
use rosbag2_rs::cdr::CdrDeserializer;
use rosbag2_rs::error::Result;
use std::path::Path;

fn debug_gps_message(data: &[u8]) -> Result<()> {
    println!("Raw CDR data ({} bytes): {:02x?}", data.len(), &data[..std::cmp::min(128, data.len())]);
    
    // Scan for f64 values that look like GPS coordinates
    println!("\n=== Scanning for reasonable GPS coordinate values ===");
    for i in (4..data.len()-7).step_by(4) {
        if i + 8 <= data.len() {
            let bytes = &data[i..i+8];
            let value = f64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
            
            // Check if this looks like a GPS coordinate (latitude: -90 to 90, longitude: -180 to 180)
            if (value >= -90.0 && value <= 90.0) || (value >= -180.0 && value <= 180.0) {
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
    
    // Show the next 64 bytes to see the pattern
    println!("\nNext 64 bytes from position {}:", pos);
    for i in 0..64 {
        if pos + i < data.len() {
            print!("{:02x} ", data[pos + i]);
            if (i + 1) % 8 == 0 {
                println!();
            }
        }
    }
    println!();
    
    // Try to find the GPS data by looking for reasonable f64 values
    println!("\n--- Searching for GPS data ---");
    
    // NavSatFix structure:
    // - Header (already read)
    // - NavSatStatus status
    // - f64 latitude
    // - f64 longitude  
    // - f64 altitude
    // - f64[9] position_covariance
    // - u8 position_covariance_type
    
    // Try different starting positions for the GPS data
    for test_pos in [pos, pos + 4, pos + 8, pos + 12, pos + 16] {
        if test_pos + 32 <= data.len() { // At least space for status + lat/lon/alt
            println!("\nTrying position {}:", test_pos);
            
            // Try to read status (2 i8 values)
            let status1 = data[test_pos] as i8;
            let status2 = data[test_pos + 1] as i8;
            println!("  Status: ({}, {})", status1, status2);
            
            // Look for latitude at different offsets
            for lat_offset in [8, 12, 16, 20] {
                if test_pos + lat_offset + 24 <= data.len() {
                    let lat_bytes = &data[test_pos + lat_offset..test_pos + lat_offset + 8];
                    let lat = f64::from_le_bytes([
                        lat_bytes[0], lat_bytes[1], lat_bytes[2], lat_bytes[3],
                        lat_bytes[4], lat_bytes[5], lat_bytes[6], lat_bytes[7],
                    ]);
                    
                    let lon_bytes = &data[test_pos + lat_offset + 8..test_pos + lat_offset + 16];
                    let lon = f64::from_le_bytes([
                        lon_bytes[0], lon_bytes[1], lon_bytes[2], lon_bytes[3],
                        lon_bytes[4], lon_bytes[5], lon_bytes[6], lon_bytes[7],
                    ]);
                    
                    let alt_bytes = &data[test_pos + lat_offset + 16..test_pos + lat_offset + 24];
                    let alt = f64::from_le_bytes([
                        alt_bytes[0], alt_bytes[1], alt_bytes[2], alt_bytes[3],
                        alt_bytes[4], alt_bytes[5], alt_bytes[6], alt_bytes[7],
                    ]);
                    
                    println!("  Offset {}: Lat={}, Lon={}, Alt={}", lat_offset, lat, lon, alt);
                    
                    // Check if these look like reasonable GPS coordinates
                    if lat >= -90.0 && lat <= 90.0 && lon >= -180.0 && lon <= 180.0 && alt >= -1000.0 && alt <= 10000.0 {
                        println!("    *** This looks like valid GPS data! ***");
                    }
                }
            }
        }
    }
    
    Ok(())
}

fn main() -> Result<()> {
    let bag_path = Path::new("../rosbag2_2025_06_02-17_08_59");
    let mut reader = Reader::new(bag_path)?;
    reader.open()?;
    
    // Find GPS topic
    let target_connections: Vec<_> = reader.connections()
        .iter()
        .filter(|conn| conn.topic == "/ros_ap_forwarder/gps")
        .cloned()
        .collect();
    
    if target_connections.is_empty() {
        eprintln!("GPS topic not found");
        return Ok(());
    }
    
    let gps_connection = &target_connections[0];
    println!("Found GPS topic: {}", gps_connection.topic);
    println!("Message type: {}", gps_connection.message_type);
    
    // Read first few messages
    let mut message_count = 0;
    match reader.messages_filtered(Some(&target_connections), None, None) {
        Ok(messages) => {
            for message_result in messages {
                match message_result {
                    Ok(message) => {
                        println!("\n{}", "=".repeat(60));
                        println!("GPS Message #{}", message_count + 1);
                        println!("Timestamp: {}", message.timestamp);
                        println!("Data size: {} bytes", message.data.len());
                        
                        if let Err(e) = debug_gps_message(&message.data) {
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
