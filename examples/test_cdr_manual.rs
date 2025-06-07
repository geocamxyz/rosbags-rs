//! Manual CDR parsing test

use rosbag2_rs::cdr::CdrDeserializer;
use rosbag2_rs::Reader;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting CDR manual test...");
    let mut reader = Reader::new(std::path::Path::new("/Users/amin/Downloads/V1_03_difficult"))?;
    println!("Reader created, opening...");
    reader.open()?;
    println!("Reader opened successfully");
    
    let target_connections: Vec<_> = reader.connections()
        .iter()
        .filter(|conn| conn.topic == "/fcu/imu")
        .cloned()
        .collect();
    
    if let Ok(messages) = reader.messages_filtered(Some(&target_connections), None, None) {
        for message_result in messages.take(1) {
            if let Ok(message) = message_result {
                println!("Message size: {} bytes", message.data.len());
                
                // Print first 32 bytes in hex
                print!("First 32 bytes: ");
                for i in 0..32.min(message.data.len()) {
                    print!("{:02x} ", message.data[i]);
                }
                println!();
                
                // Try manual parsing
                let mut deserializer = CdrDeserializer::new(&message.data)?;
                
                println!("Position after header: {}", deserializer.position());
                
                // Read header.stamp.sec
                let sec = deserializer.read_i32()?;
                println!("Header.stamp.sec: {} (pos: {})", sec, deserializer.position());
                
                // Read header.stamp.nanosec
                let nanosec = deserializer.read_u32()?;
                println!("Header.stamp.nanosec: {} (pos: {})", nanosec, deserializer.position());
                
                // Read header.frame_id
                let frame_id = deserializer.read_string()?;
                println!("Header.frame_id: '{}' (pos: {})", frame_id, deserializer.position());
                
                // Read orientation quaternion (4 f64s)
                let qx = deserializer.read_f64()?;
                let qy = deserializer.read_f64()?;
                let qz = deserializer.read_f64()?;
                let qw = deserializer.read_f64()?;
                println!("Orientation: x={:.6}, y={:.6}, z={:.6}, w={:.6} (pos: {})", qx, qy, qz, qw, deserializer.position());
                
                // Read orientation covariance (9 f64s)
                println!("Reading orientation covariance at pos: {}", deserializer.position());
                for i in 0..9 {
                    let val = deserializer.read_f64()?;
                    if i < 3 {
                        println!("  cov[{}] = {} (pos: {})", i, val, deserializer.position());
                    }
                }
                
                // Read angular velocity (3 f64s)
                println!("Reading angular velocity at pos: {}", deserializer.position());
                let avx = deserializer.read_f64()?;
                let avy = deserializer.read_f64()?;
                let avz = deserializer.read_f64()?;
                println!("Angular velocity: x={:.6}, y={:.6}, z={:.6} (pos: {})", avx, avy, avz, deserializer.position());
                
                // Read angular velocity covariance (9 f64s)
                println!("Reading angular velocity covariance at pos: {}", deserializer.position());
                for i in 0..9 {
                    let val = deserializer.read_f64()?;
                    if i < 3 {
                        println!("  av_cov[{}] = {} (pos: {})", i, val, deserializer.position());
                    }
                }
                
                // Read linear acceleration (3 f64s)
                println!("Reading linear acceleration at pos: {}", deserializer.position());
                let lax = deserializer.read_f64()?;
                let lay = deserializer.read_f64()?;
                let laz = deserializer.read_f64()?;
                println!("Linear acceleration: x={:.6}, y={:.6}, z={:.6} (pos: {})", lax, lay, laz, deserializer.position());
                
                // Read linear acceleration covariance (9 f64s)
                println!("Reading linear acceleration covariance at pos: {}", deserializer.position());
                for i in 0..9 {
                    if deserializer.position() + 8 > message.data.len() {
                        println!("  Would exceed data length at element {}, pos: {}, data len: {}", i, deserializer.position(), message.data.len());
                        break;
                    }
                    let val = deserializer.read_f64()?;
                    if i < 3 {
                        println!("  la_cov[{}] = {} (pos: {})", i, val, deserializer.position());
                    }
                }
                
                println!("Final position: {}", deserializer.position());
                break;
            }
        }
    }
    
    Ok(())
}
