use rosbag2_rs::Reader;
use rosbag2_rs::cdr::CdrDeserializer;
use rosbag2_rs::error::Result;
use std::path::Path;

fn debug_gps_message(data: &[u8]) -> Result<()> {
    println!("Raw CDR data ({} bytes): {:02x?}", data.len(), &data[..std::cmp::min(128, data.len())]);

    // Scan for f64 values that look like reasonable GPS coordinates
    println!("\n=== Scanning for GPS coordinates ===");
    for i in (4..data.len()-7).step_by(4) {
        if i + 8 <= data.len() {
            let bytes = &data[i..i+8];
            let value = f64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5], bytes[6], bytes[7],
            ]);

            // Check if this looks like a reasonable GPS coordinate
            if (value >= -90.0 && value <= 90.0) || (value >= -180.0 && value <= 180.0) || (value >= -1000.0 && value <= 10000.0) {
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

    // Read NavSatStatus
    println!("\n--- NavSatStatus ---");
    let status = data[pos] as i8;
    println!("status: {} (pos: {})", status, pos);
    pos += 1;

    // Align for u16
    pos = (pos + 2 - 1) & !(2 - 1);
    println!("After alignment for u16, position: {}", pos);

    let service = u16::from_le_bytes([data[pos], data[pos + 1]]);
    println!("service: {} (pos: {})", service, pos);
    pos += 2;

    println!("After NavSatStatus, position: {}", pos);

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

    // Try different alignments for f64
    println!("\n--- Testing different f64 alignments ---");
    for test_pos in [pos, pos + 4, pos + 8] {
        if test_pos + 24 <= data.len() {
            println!("\nTrying position {}:", test_pos);

            let lat_bytes = &data[test_pos..test_pos + 8];
            let latitude = f64::from_le_bytes([
                lat_bytes[0], lat_bytes[1], lat_bytes[2], lat_bytes[3],
                lat_bytes[4], lat_bytes[5], lat_bytes[6], lat_bytes[7],
            ]);
            println!("  Latitude: {:02x?} -> {}", lat_bytes, latitude);

            let lon_bytes = &data[test_pos + 8..test_pos + 16];
            let longitude = f64::from_le_bytes([
                lon_bytes[0], lon_bytes[1], lon_bytes[2], lon_bytes[3],
                lon_bytes[4], lon_bytes[5], lon_bytes[6], lon_bytes[7],
            ]);
            println!("  Longitude: {:02x?} -> {}", lon_bytes, longitude);

            let alt_bytes = &data[test_pos + 16..test_pos + 24];
            let altitude = f64::from_le_bytes([
                alt_bytes[0], alt_bytes[1], alt_bytes[2], alt_bytes[3],
                alt_bytes[4], alt_bytes[5], alt_bytes[6], alt_bytes[7],
            ]);
            println!("  Altitude: {:02x?} -> {}", alt_bytes, altitude);

            // Check if these look reasonable
            if latitude >= -90.0 && latitude <= 90.0 && longitude >= -180.0 && longitude <= 180.0 {
                println!("  *** This looks like valid GPS coordinates! ***");
            }
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let bag_path = Path::new("../scaled_07_30_00");
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

    // Read first GPS message
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
