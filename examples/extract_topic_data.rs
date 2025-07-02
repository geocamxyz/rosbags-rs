//! Example: Extract topic data from ROS2 bag files
//!
//! This example reads a specific topic from a ROS2 bag file and exports the data
//! in an appropriate format based on the message type:
//! - Image messages (sensor_msgs/msg/Image, sensor_msgs/msg/CompressedImage) -> Image files (PNG)
//! - All other message types -> CSV files with timestamped rows
//!
//! Usage:
//!   cargo run --example extract_topic_data <bag_path> <topic_name> <output_folder>
//!
//! Arguments:
//!   bag_path      - Path to the ROS2 bag file
//!   topic_name    - Name of the topic to extract (e.g., "/camera/image_raw")
//!   output_folder - Directory where extracted data will be saved
//!
//! Examples:
//!   # Extract camera images
//!   cargo run --example extract_topic_data ./my_bag /camera/image_raw ./extracted_images/
//!
//!   # Extract IMU data to CSV
//!   cargo run --example extract_topic_data ./my_bag /imu/data ./extracted_imu/
//!
//!   # Extract odometry data to CSV
//!   cargo run --example extract_topic_data ./my_bag /odom ./extracted_odom/

use image::{save_buffer, ColorType};
use rosbags_rs::cdr::CdrDeserializer;
use rosbags_rs::messages::{FromCdr, Imu};
use rosbags_rs::{Message, Reader};
use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 4 {
        eprintln!("Usage: {} <bag_path> <topic_name> <output_folder>", args[0]);
        eprintln!("\nExamples:");
        eprintln!(
            "  {} ./my_bag /camera/image_raw ./extracted_images/",
            args[0]
        );
        eprintln!("  {} ./my_bag /imu/data ./extracted_imu/", args[0]);
        eprintln!("  {} ./my_bag /odom ./extracted_odom/", args[0]);
        std::process::exit(1);
    }

    let bag_path = &args[1];
    let topic_name = &args[2];
    let output_folder = &args[3];

    println!("📦 Opening bag: {}", bag_path);
    println!("🎯 Target topic: {}", topic_name);
    println!("📁 Output folder: {}", output_folder);

    // Create output directory
    fs::create_dir_all(output_folder)?;

    // Open the bag
    let mut reader = Reader::new(Path::new(bag_path))?;
    reader.open()?;

    // Get topics and find the target topic
    let topics = reader.topics();
    let target_topic = topics
        .iter()
        .find(|topic| topic.name.as_str() == topic_name)
        .ok_or_else(|| format!("Topic '{}' not found in bag", topic_name))?;

    println!(
        "✅ Found topic: {} ({})",
        target_topic.name, target_topic.message_type
    );
    println!("📊 Message count: {}", target_topic.message_count);

    // Show available topics if requested topic not found or for reference
    println!("\n📋 Available topics in bag:");
    for topic in &topics {
        let marker = if topic.name.as_str() == topic_name {
            "👉 "
        } else {
            "   "
        };
        println!(
            "{}  {} ({}) - {} messages",
            marker, topic.name, topic.message_type, topic.message_count
        );
    }

    // Determine export strategy based on message type
    let export_strategy = determine_export_strategy(&target_topic.message_type);

    match export_strategy {
        ExportStrategy::Images => {
            println!("\n🖼️  Exporting as PNG image files...");
            extract_images(&mut reader, topic_name, output_folder)?;
        }
        ExportStrategy::Csv => {
            println!("\n📄 Exporting as CSV file...");
            extract_to_csv(
                &mut reader,
                topic_name,
                output_folder,
                &target_topic.message_type,
            )?;
        }
    }

    println!("\n🎉 Extraction completed successfully!");
    println!("📁 Check output folder: {}", output_folder);

    Ok(())
}

#[derive(Debug)]
enum ExportStrategy {
    Images,
    Csv,
}

fn determine_export_strategy(message_type: &str) -> ExportStrategy {
    match message_type {
        "sensor_msgs/msg/Image" | "sensor_msgs/msg/CompressedImage" => ExportStrategy::Images,
        _ => ExportStrategy::Csv,
    }
}

fn extract_images(
    reader: &mut Reader,
    topic_name: &str,
    output_folder: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut image_count = 0;

    for message_result in reader.messages()? {
        let message = message_result?;

        if message.topic == topic_name {
            let output_path = PathBuf::from(output_folder).join(format!(
                "image_{:06}_{}.png",
                image_count, message.timestamp
            ));

            match extract_image_data(&message) {
                Ok(image_info) => {
                    // Save as PNG image
                    save_image_as_png(&output_path, &image_info)?;

                    if image_count % 100 == 0 {
                        println!("  Extracted {} images...", image_count + 1);
                    }
                }
                Err(e) => {
                    eprintln!(
                        "⚠️  Warning: Failed to extract image {}: {}",
                        image_count, e
                    );
                }
            }

            image_count += 1;
        }
    }

    println!("✅ Extracted {} images", image_count);

    // Create a summary file
    let summary_path = PathBuf::from(output_folder).join("image_summary.txt");
    let mut summary_file = fs::File::create(summary_path)?;
    writeln!(summary_file, "Image Extraction Summary")?;
    writeln!(summary_file, "=======================")?;
    writeln!(summary_file, "Topic: {}", topic_name)?;
    writeln!(summary_file, "Total images: {}", image_count)?;
    writeln!(summary_file, "Format: PNG files")?;
    writeln!(summary_file, "Naming: image_XXXXXX_timestamp.png")?;

    Ok(())
}

fn extract_to_csv(
    reader: &mut Reader,
    topic_name: &str,
    output_folder: &str,
    message_type: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let csv_path = PathBuf::from(output_folder).join(format!(
        "{}.csv",
        topic_name.replace('/', "_").trim_start_matches('_')
    ));

    let mut csv_file = fs::File::create(&csv_path)?;
    let mut message_count = 0;
    let mut headers_written = false;

    for message_result in reader.messages()? {
        let message = message_result?;

        if message.topic == topic_name {
            let csv_data = extract_message_to_csv(&message, message_type)?;

            // Write headers on first message
            if !headers_written {
                writeln!(csv_file, "{}", csv_data.headers.join(","))?;
                headers_written = true;
            }

            // Write data row
            writeln!(csv_file, "{}", csv_data.values.join(","))?;

            message_count += 1;
            if message_count % 1000 == 0 {
                println!("  Processed {} messages...", message_count);
            }
        }
    }

    println!("✅ Exported {} messages to CSV", message_count);
    println!("📄 CSV file: {}", csv_path.display());

    Ok(())
}

#[derive(Debug)]
struct ImageInfo {
    width: u32,
    height: u32,
    encoding: String,
    data: Vec<u8>,
}

fn extract_image_data(message: &Message) -> Result<ImageInfo, Box<dyn std::error::Error>> {
    let data = &message.data;

    if data.len() < 32 {
        return Err("Message too short for image data".into());
    }

    // Parse basic image message structure (simplified CDR parsing)
    // This is a simplified parser - in production you'd want full CDR parsing
    let mut offset = 0;

    // Skip header (simplified)
    // Header typically contains: seq, timestamp, frame_id
    offset += 4; // seq
    offset += 8; // timestamp (sec + nsec)

    // Read frame_id string length and skip it
    if offset + 4 <= data.len() {
        let frame_id_len = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4 + frame_id_len;
        // Align to 4 bytes
        while offset % 4 != 0 && offset < data.len() {
            offset += 1;
        }
    }

    // Read image dimensions
    if offset + 8 <= data.len() {
        let height = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        offset += 4;

        let width = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        offset += 4;

        // Read encoding string
        let encoding = if offset + 4 <= data.len() {
            let encoding_len = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + encoding_len <= data.len() {
                String::from_utf8_lossy(&data[offset..offset + encoding_len]).to_string()
            } else {
                "unknown".to_string()
            }
        } else {
            "unknown".to_string()
        };

        // The rest would be image data
        let image_data = if offset < data.len() {
            data[offset..].to_vec()
        } else {
            vec![]
        };

        Ok(ImageInfo {
            width,
            height,
            encoding,
            data: image_data,
        })
    } else {
        Err("Insufficient data for image dimensions".into())
    }
}

fn save_image_as_png(
    output_path: &Path,
    image_info: &ImageInfo,
) -> Result<(), Box<dyn std::error::Error>> {
    let png_path = output_path.with_extension("png");

    match image_info.encoding.as_str() {
        "mono8" => {
            // 8-bit grayscale
            save_buffer(
                &png_path,
                &image_info.data,
                image_info.width,
                image_info.height,
                ColorType::L8,
            )?;
        }
        "rgb8" => {
            // 8-bit RGB
            save_buffer(
                &png_path,
                &image_info.data,
                image_info.width,
                image_info.height,
                ColorType::Rgb8,
            )?;
        }
        "bgr8" => {
            // 8-bit BGR - need to convert to RGB
            let mut rgb_data = Vec::with_capacity(image_info.data.len());
            for chunk in image_info.data.chunks(3) {
                if chunk.len() == 3 {
                    rgb_data.push(chunk[2]); // R
                    rgb_data.push(chunk[1]); // G
                    rgb_data.push(chunk[0]); // B
                }
            }
            save_buffer(
                &png_path,
                &rgb_data,
                image_info.width,
                image_info.height,
                ColorType::Rgb8,
            )?;
        }
        "mono16" => {
            // 16-bit grayscale - convert to 8-bit for PNG
            let mut mono8_data = Vec::with_capacity(image_info.data.len() / 2);
            for chunk in image_info.data.chunks(2) {
                if chunk.len() == 2 {
                    let val = u16::from_le_bytes([chunk[0], chunk[1]]);
                    mono8_data.push((val / 256) as u8); // Scale down to 8-bit
                }
            }
            save_buffer(
                &png_path,
                &mono8_data,
                image_info.width,
                image_info.height,
                ColorType::L8,
            )?;
        }
        _ => {
            // For unsupported encodings, save as grayscale if possible
            if image_info.data.len() >= (image_info.width * image_info.height) as usize {
                let mono_data: Vec<u8> = image_info
                    .data
                    .iter()
                    .take((image_info.width * image_info.height) as usize)
                    .copied()
                    .collect();
                save_buffer(
                    &png_path,
                    &mono_data,
                    image_info.width,
                    image_info.height,
                    ColorType::L8,
                )?;
            } else {
                return Err(format!("Unsupported encoding: {}", image_info.encoding).into());
            }
        }
    }

    Ok(())
}

#[derive(Debug)]
struct CsvData {
    headers: Vec<String>,
    values: Vec<String>,
}

fn extract_message_to_csv(
    message: &Message,
    message_type: &str,
) -> Result<CsvData, Box<dyn std::error::Error>> {
    let mut headers = vec!["timestamp".to_string(), "topic".to_string()];
    let mut values = vec![message.timestamp.to_string(), message.topic.clone()];

    // Extract fields based on message type
    match message_type {
        "geometry_msgs/msg/Point" => {
            extract_point_message(&message.data, &mut headers, &mut values)?;
        }
        "geometry_msgs/msg/Vector3" => {
            extract_vector3_message(&message.data, &mut headers, &mut values)?;
        }
        "geometry_msgs/msg/Quaternion" => {
            extract_quaternion_message(&message.data, &mut headers, &mut values)?;
        }
        "geometry_msgs/msg/Pose" => {
            extract_pose_message(&message.data, &mut headers, &mut values)?;
        }
        "geometry_msgs/msg/Twist" => {
            extract_twist_message(&message.data, &mut headers, &mut values)?;
        }
        "sensor_msgs/msg/Imu" => {
            extract_imu_message(&message.data, &mut headers, &mut values)?;
        }
        "nav_msgs/msg/Odometry" => {
            extract_odometry_message(&message.data, &mut headers, &mut values)?;
        }
        "geometry_msgs/msg/PointStamped" => {
            extract_point_stamped_message(&message.data, &mut headers, &mut values)?;
        }
        "std_msgs/msg/String" => {
            extract_string_message(&message.data, &mut headers, &mut values)?;
        }
        "std_msgs/msg/Int32" => {
            extract_int32_message(&message.data, &mut headers, &mut values)?;
        }
        "std_msgs/msg/Float64" => {
            extract_float64_message(&message.data, &mut headers, &mut values)?;
        }
        _ => {
            // Generic extraction for unknown message types
            headers.push("data_length".to_string());
            headers.push("data_hex".to_string());
            values.push(message.data.len().to_string());
            values.push(hex::encode(&message.data[..message.data.len().min(32)]));
        }
    }

    Ok(CsvData { headers, values })
}

fn extract_point_message(
    data: &[u8],
    headers: &mut Vec<String>,
    values: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    if data.len() >= 24 {
        let x = f64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let y = f64::from_le_bytes([
            data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
        ]);
        let z = f64::from_le_bytes([
            data[16], data[17], data[18], data[19], data[20], data[21], data[22], data[23],
        ]);

        headers.extend_from_slice(&["x".to_string(), "y".to_string(), "z".to_string()]);
        values.extend_from_slice(&[x.to_string(), y.to_string(), z.to_string()]);
    }
    Ok(())
}

fn extract_vector3_message(
    data: &[u8],
    headers: &mut Vec<String>,
    values: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    extract_point_message(data, headers, values) // Same structure as Point
}

fn extract_quaternion_message(
    data: &[u8],
    headers: &mut Vec<String>,
    values: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    if data.len() >= 32 {
        let x = f64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let y = f64::from_le_bytes([
            data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
        ]);
        let z = f64::from_le_bytes([
            data[16], data[17], data[18], data[19], data[20], data[21], data[22], data[23],
        ]);
        let w = f64::from_le_bytes([
            data[24], data[25], data[26], data[27], data[28], data[29], data[30], data[31],
        ]);

        headers.extend_from_slice(&[
            "qx".to_string(),
            "qy".to_string(),
            "qz".to_string(),
            "qw".to_string(),
        ]);
        values.extend_from_slice(&[x.to_string(), y.to_string(), z.to_string(), w.to_string()]);
    }
    Ok(())
}

fn extract_pose_message(
    data: &[u8],
    headers: &mut Vec<String>,
    values: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Pose = Point + Quaternion (56 bytes total)
    if data.len() >= 56 {
        // Extract position
        extract_point_message(&data[0..24], headers, values)?;

        // Change the last headers to position_*
        if headers.len() >= 3 {
            let len = headers.len();
            headers[len - 3] = "position_x".to_string();
            headers[len - 2] = "position_y".to_string();
            headers[len - 1] = "position_z".to_string();
        }

        // Extract orientation
        let mut orientation_headers = vec![];
        let mut orientation_values = vec![];
        extract_quaternion_message(
            &data[24..56],
            &mut orientation_headers,
            &mut orientation_values,
        )?;

        // Rename quaternion headers for orientation
        headers.extend_from_slice(&[
            "orientation_x".to_string(),
            "orientation_y".to_string(),
            "orientation_z".to_string(),
            "orientation_w".to_string(),
        ]);
        values.extend_from_slice(&orientation_values[2..]); // Skip timestamp and topic
    }
    Ok(())
}

fn extract_twist_message(
    data: &[u8],
    headers: &mut Vec<String>,
    values: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Twist = Vector3 linear + Vector3 angular (48 bytes total)
    if data.len() >= 48 {
        // Linear velocity
        let linear_x = f64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let linear_y = f64::from_le_bytes([
            data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
        ]);
        let linear_z = f64::from_le_bytes([
            data[16], data[17], data[18], data[19], data[20], data[21], data[22], data[23],
        ]);

        // Angular velocity
        let angular_x = f64::from_le_bytes([
            data[24], data[25], data[26], data[27], data[28], data[29], data[30], data[31],
        ]);
        let angular_y = f64::from_le_bytes([
            data[32], data[33], data[34], data[35], data[36], data[37], data[38], data[39],
        ]);
        let angular_z = f64::from_le_bytes([
            data[40], data[41], data[42], data[43], data[44], data[45], data[46], data[47],
        ]);

        headers.extend_from_slice(&[
            "linear_x".to_string(),
            "linear_y".to_string(),
            "linear_z".to_string(),
            "angular_x".to_string(),
            "angular_y".to_string(),
            "angular_z".to_string(),
        ]);
        values.extend_from_slice(&[
            linear_x.to_string(),
            linear_y.to_string(),
            linear_z.to_string(),
            angular_x.to_string(),
            angular_y.to_string(),
            angular_z.to_string(),
        ]);
    }
    Ok(())
}

fn extract_imu_message(
    data: &[u8],
    headers: &mut Vec<String>,
    values: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Use the proper CDR deserialization infrastructure
    match CdrDeserializer::new(data) {
        Ok(mut deserializer) => match Imu::from_cdr(&mut deserializer) {
            Ok(imu) => {
                headers.extend_from_slice(&[
                    "angular_velocity_x".to_string(),
                    "angular_velocity_y".to_string(),
                    "angular_velocity_z".to_string(),
                    "linear_acceleration_x".to_string(),
                    "linear_acceleration_y".to_string(),
                    "linear_acceleration_z".to_string(),
                ]);
                values.extend_from_slice(&[
                    imu.angular_velocity.x.to_string(),
                    imu.angular_velocity.y.to_string(),
                    imu.angular_velocity.z.to_string(),
                    imu.linear_acceleration.x.to_string(),
                    imu.linear_acceleration.y.to_string(),
                    imu.linear_acceleration.z.to_string(),
                ]);
            }
            Err(e) => {
                return Err(format!("Failed to deserialize IMU message: {}", e).into());
            }
        },
        Err(e) => {
            return Err(format!("Failed to create CDR deserializer: {}", e).into());
        }
    }

    Ok(())
}

fn extract_odometry_message(
    data: &[u8],
    headers: &mut Vec<String>,
    values: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Simplified odometry extraction
    if data.len() >= 32 {
        headers.extend_from_slice(&["odom_data_length".to_string(), "sample_data".to_string()]);
        values.extend_from_slice(&[data.len().to_string(), hex::encode(&data[..32])]);
    }
    Ok(())
}

fn extract_point_stamped_message(
    data: &[u8],
    headers: &mut Vec<String>,
    values: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    // PointStamped = Header + Point
    // Skip header for now and extract the point (last 24 bytes)
    if data.len() >= 24 {
        let offset = data.len() - 24;
        extract_point_message(&data[offset..], headers, values)?;
    }
    Ok(())
}

fn extract_string_message(
    data: &[u8],
    headers: &mut Vec<String>,
    values: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    if data.len() >= 4 {
        let str_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() >= 4 + str_len {
            let string_data = String::from_utf8_lossy(&data[4..4 + str_len]);
            headers.push("data".to_string());
            values.push(format!("\"{}\"", string_data.replace('"', "\"\"")));
        }
    }
    Ok(())
}

fn extract_int32_message(
    data: &[u8],
    headers: &mut Vec<String>,
    values: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    if data.len() >= 4 {
        let value = i32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        headers.push("data".to_string());
        values.push(value.to_string());
    }
    Ok(())
}

fn extract_float64_message(
    data: &[u8],
    headers: &mut Vec<String>,
    values: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    if data.len() >= 8 {
        let value = f64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        headers.push("data".to_string());
        values.push(value.to_string());
    }
    Ok(())
}
