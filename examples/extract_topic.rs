//! Example: Extract a specific topic from a ROS2 bag file and write to text file
//!
//! This example demonstrates how to:
//! - Open a ROS2 bag file
//! - Filter messages for a specific topic
//! - Write message data to a text file for analysis
//!
//! Usage:
//!   cargo run --example extract_topic <bag_path> <topic_name> <output_file>
//!
//! Example:
//!   cargo run --example extract_topic ~/Downloads/V1_03_difficult /fcu/imu imu_data.txt

use rosbags_rs::cdr::CdrDeserializer;
use rosbags_rs::messages::{
    FromCdr, Image, Imu, NavSatFix, Odometry, PointCloud2, PointStamped, PoseWithCovarianceStamped,
    StdString, TransformStamped,
};
use rosbags_rs::Reader;
use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("Usage: {} <bag_path> <topic_name> <output_file>", args[0]);
        eprintln!(
            "Example: {} ~/Downloads/V1_03_difficult /fcu/imu imu_data.txt",
            args[0]
        );
        eprintln!();
        eprintln!("Arguments:");
        eprintln!("  bag_path    - Path to the ROS2 bag directory");
        eprintln!("  topic_name  - Name of the topic to extract (e.g., /fcu/imu)");
        eprintln!("  output_file - Path to the output text file");
        std::process::exit(1);
    }

    let bag_path = Path::new(&args[1]);
    let topic_name = &args[2];
    let output_file = &args[3];

    println!(
        "Extracting topic '{}' from bag: {}",
        topic_name,
        bag_path.display()
    );
    println!("Output file: {}", output_file);

    // Create and open the reader
    let mut reader = Reader::new(bag_path)
        .map_err(|e| format!("Failed to open bag file '{}': {}", bag_path.display(), e))?;

    reader
        .open()
        .map_err(|e| format!("Failed to open bag for reading: {}", e))?;

    // Print basic bag information
    println!("\n=== Bag Information ===");
    println!(
        "Duration: {:.2} seconds",
        reader.duration() as f64 / 1_000_000_000.0
    );
    println!("Total messages: {}", reader.message_count());

    // Find connections for the specified topic
    let target_connections: Vec<_> = reader
        .connections()
        .iter()
        .filter(|conn| conn.topic == *topic_name)
        .cloned()
        .collect();

    if target_connections.is_empty() {
        eprintln!("Error: Topic '{}' not found in bag file", topic_name);
        eprintln!("\nAvailable topics:");
        for topic in reader.topics() {
            eprintln!("  - {} ({})", topic.name, topic.message_type);
        }
        std::process::exit(1);
    }

    println!("\n=== Topic Information ===");
    for conn in &target_connections {
        println!("Topic: {}", conn.topic);
        println!("  Type: {}", conn.message_type);
        println!("  Messages: {}", conn.message_count);
        println!("  Serialization: {}", conn.serialization_format);
        if !conn.type_description_hash.is_empty() {
            println!("  Type Hash: {}", conn.type_description_hash);
        }
    }

    // Create output file
    let output_path = Path::new(output_file);
    let file = File::create(output_path)
        .map_err(|e| format!("Failed to create output file '{}': {}", output_file, e))?;
    let mut writer = BufWriter::new(file);

    // Write header to output file
    writeln!(writer, "# ROS2 Bag Topic Extraction")?;
    writeln!(writer, "# Bag: {}", bag_path.display())?;
    writeln!(writer, "# Topic: {}", topic_name)?;
    writeln!(
        writer,
        "# Extracted at: {}",
        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
    )?;
    writeln!(writer, "#")?;

    // Determine output format based on message type
    let message_type = &target_connections[0].message_type;
    match message_type.as_str() {
        "sensor_msgs/msg/Imu" => {
            writeln!(writer, "# Format: timestamp_ns,header_sec,header_nanosec,frame_id,orientation_x,orientation_y,orientation_z,orientation_w,angular_velocity_x,angular_velocity_y,angular_velocity_z,linear_acceleration_x,linear_acceleration_y,linear_acceleration_z")?;
        }
        "geometry_msgs/msg/TransformStamped" => {
            writeln!(writer, "# Format: timestamp_ns,header_sec,header_nanosec,frame_id,child_frame_id,translation_x,translation_y,translation_z,rotation_x,rotation_y,rotation_z,rotation_w")?;
        }
        "geometry_msgs/msg/PoseWithCovarianceStamped" => {
            writeln!(writer, "# Format: timestamp_ns,header_sec,header_nanosec,frame_id,position_x,position_y,position_z,orientation_x,orientation_y,orientation_z,orientation_w,covariance_00,covariance_01,covariance_02,covariance_03,covariance_04,covariance_05,covariance_06,covariance_07,covariance_08,covariance_09,covariance_10,covariance_11,covariance_12,covariance_13,covariance_14,covariance_15,covariance_16,covariance_17,covariance_18,covariance_19,covariance_20,covariance_21,covariance_22,covariance_23,covariance_24,covariance_25,covariance_26,covariance_27,covariance_28,covariance_29,covariance_30,covariance_31,covariance_32,covariance_33,covariance_34,covariance_35")?;
        }
        "sensor_msgs/msg/NavSatFix" => {
            writeln!(writer, "# Format: timestamp_ns,header_sec,header_nanosec,frame_id,status,service,latitude,longitude,altitude,position_covariance_type")?;
        }
        "std_msgs/msg/String" => {
            writeln!(writer, "# Format: timestamp_ns,data")?;
        }
        "sensor_msgs/msg/PointCloud2" => {
            writeln!(writer, "# Format: timestamp_ns,header_sec,header_nanosec,frame_id,height,width,fields_count,is_bigendian,point_step,row_step,data_size,is_dense")?;
        }
        "sensor_msgs/msg/Image" => {
            writeln!(writer, "# Format: timestamp_ns,header_sec,header_nanosec,frame_id,height,width,encoding,is_bigendian,step,data_size")?;
        }
        "nav_msgs/msg/Odometry" => {
            writeln!(writer, "# Format: timestamp_ns,header_sec,header_nanosec,frame_id,child_frame_id,position_x,position_y,position_z,orientation_x,orientation_y,orientation_z,orientation_w,linear_x,linear_y,linear_z,angular_x,angular_y,angular_z")?;
        }
        "geometry_msgs/msg/PointStamped" => {
            writeln!(
                writer,
                "# Format: timestamp_ns,header_sec,header_nanosec,frame_id,point_x,point_y,point_z"
            )?;
        }
        _ => {
            writeln!(writer, "# Format: timestamp_ns,data_size_bytes,data_hex")?;
        }
    }
    writeln!(writer, "#")?;

    // Extract messages for the specified topic
    println!("\n=== Extracting Messages ===");
    let mut message_count = 0;
    let mut total_bytes = 0;

    match reader.messages_filtered(Some(&target_connections), None, None) {
        Ok(messages) => {
            for message_result in messages {
                match message_result {
                    Ok(message) => {
                        // Try to deserialize the message based on its type
                        let output_line = match message.connection.message_type.as_str() {
                            "sensor_msgs/msg/Imu" => match deserialize_imu_message(&message.data) {
                                Ok(imu) => {
                                    format!("{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6}",
                                            message.timestamp,
                                            imu.header.stamp.sec,
                                            imu.header.stamp.nanosec,
                                            imu.header.frame_id,
                                            imu.orientation.x,
                                            imu.orientation.y,
                                            imu.orientation.z,
                                            imu.orientation.w,
                                            imu.angular_velocity.x,
                                            imu.angular_velocity.y,
                                            imu.angular_velocity.z,
                                            imu.linear_acceleration.x,
                                            imu.linear_acceleration.y,
                                            imu.linear_acceleration.z
                                        )
                                }
                                Err(e) => {
                                    eprintln!(
                                        "Warning: Failed to deserialize IMU message {}: {}",
                                        message_count + 1,
                                        e
                                    );
                                    format!(
                                        "{},{},deserialize_error",
                                        message.timestamp,
                                        message.data.len()
                                    )
                                }
                            },
                            "geometry_msgs/msg/TransformStamped" => {
                                match deserialize_transform_message(&message.data) {
                                    Ok(transform) => {
                                        format!("{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6}",
                                            message.timestamp,
                                            transform.header.stamp.sec,
                                            transform.header.stamp.nanosec,
                                            transform.header.frame_id,
                                            transform.child_frame_id,
                                            transform.transform.translation.x,
                                            transform.transform.translation.y,
                                            transform.transform.translation.z,
                                            transform.transform.rotation.x,
                                            transform.transform.rotation.y,
                                            transform.transform.rotation.z,
                                            transform.transform.rotation.w
                                        )
                                    }
                                    Err(e) => {
                                        eprintln!("Warning: Failed to deserialize Transform message {}: {}", message_count + 1, e);
                                        format!(
                                            "{},{},deserialize_error",
                                            message.timestamp,
                                            message.data.len()
                                        )
                                    }
                                }
                            }
                            "geometry_msgs/msg/PoseWithCovarianceStamped" => {
                                match deserialize_pose_with_covariance_message(&message.data) {
                                    Ok(pose) => {
                                        // Format pose data with covariance matrix (36 elements)
                                        let mut output = format!(
                                            "{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6}",
                                            message.timestamp,
                                            pose.header.stamp.sec,
                                            pose.header.stamp.nanosec,
                                            pose.header.frame_id,
                                            pose.pose.pose.position.x,
                                            pose.pose.pose.position.y,
                                            pose.pose.pose.position.z,
                                            pose.pose.pose.orientation.x,
                                            pose.pose.pose.orientation.y,
                                            pose.pose.pose.orientation.z,
                                            pose.pose.pose.orientation.w
                                        );

                                        // Add covariance matrix elements
                                        for cov_val in &pose.pose.covariance {
                                            output.push_str(&format!(",{:.6}", cov_val));
                                        }

                                        output
                                    }
                                    Err(e) => {
                                        eprintln!("Warning: Failed to deserialize PoseWithCovariance message {}: {}", message_count + 1, e);
                                        format!(
                                            "{},{},deserialize_error",
                                            message.timestamp,
                                            message.data.len()
                                        )
                                    }
                                }
                            }
                            "sensor_msgs/msg/NavSatFix" => {
                                match deserialize_navsat_message(&message.data) {
                                    Ok(navsat) => {
                                        format!(
                                            "{},{},{},{},{},{},{:.9},{:.9},{:.6},{}",
                                            message.timestamp,
                                            navsat.header.stamp.sec,
                                            navsat.header.stamp.nanosec,
                                            navsat.header.frame_id,
                                            navsat.status.status,
                                            navsat.status.service,
                                            navsat.latitude,
                                            navsat.longitude,
                                            navsat.altitude,
                                            navsat.position_covariance_type
                                        )
                                    }
                                    Err(e) => {
                                        eprintln!("Warning: Failed to deserialize NavSatFix message {}: {}", message_count + 1, e);
                                        format!(
                                            "{},{},deserialize_error",
                                            message.timestamp,
                                            message.data.len()
                                        )
                                    }
                                }
                            }
                            "std_msgs/msg/String" => {
                                match deserialize_string_message(&message.data) {
                                    Ok(string_msg) => {
                                        format!(
                                            "{},\"{}\"",
                                            message.timestamp,
                                            string_msg.data.replace("\"", "\"\"") // Escape quotes for CSV
                                        )
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "Warning: Failed to deserialize String message {}: {}",
                                            message_count + 1,
                                            e
                                        );
                                        format!(
                                            "{},{},deserialize_error",
                                            message.timestamp,
                                            message.data.len()
                                        )
                                    }
                                }
                            }
                            "sensor_msgs/msg/PointCloud2" => {
                                match deserialize_pointcloud2_message(&message.data) {
                                    Ok(pointcloud) => {
                                        format!(
                                            "{},{},{},{},{},{},{},{},{},{},{},{}",
                                            message.timestamp,
                                            pointcloud.header.stamp.sec,
                                            pointcloud.header.stamp.nanosec,
                                            pointcloud.header.frame_id,
                                            pointcloud.height,
                                            pointcloud.width,
                                            pointcloud.fields.len(),
                                            pointcloud.is_bigendian,
                                            pointcloud.point_step,
                                            pointcloud.row_step,
                                            pointcloud.data.len(),
                                            pointcloud.is_dense
                                        )
                                    }
                                    Err(e) => {
                                        eprintln!("Warning: Failed to deserialize PointCloud2 message {}: {}", message_count + 1, e);
                                        format!(
                                            "{},{},deserialize_error",
                                            message.timestamp,
                                            message.data.len()
                                        )
                                    }
                                }
                            }
                            "sensor_msgs/msg/Image" => {
                                match deserialize_image_message(&message.data) {
                                    Ok(image) => {
                                        format!(
                                            "{},{},{},{},{},{},{},{},{},{}",
                                            message.timestamp,
                                            image.header.stamp.sec,
                                            image.header.stamp.nanosec,
                                            image.header.frame_id,
                                            image.height,
                                            image.width,
                                            image.encoding,
                                            image.is_bigendian,
                                            image.step,
                                            image.data.len()
                                        )
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "Warning: Failed to deserialize Image message {}: {}",
                                            message_count + 1,
                                            e
                                        );
                                        format!(
                                            "{},{},deserialize_error",
                                            message.timestamp,
                                            message.data.len()
                                        )
                                    }
                                }
                            }
                            "nav_msgs/msg/Odometry" => {
                                match deserialize_odometry_message(&message.data) {
                                    Ok(odometry) => {
                                        format!("{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6}",
                                            message.timestamp,
                                            odometry.header.stamp.sec,
                                            odometry.header.stamp.nanosec,
                                            odometry.header.frame_id,
                                            odometry.child_frame_id,
                                            odometry.pose.pose.position.x,
                                            odometry.pose.pose.position.y,
                                            odometry.pose.pose.position.z,
                                            odometry.pose.pose.orientation.x,
                                            odometry.pose.pose.orientation.y,
                                            odometry.pose.pose.orientation.z,
                                            odometry.pose.pose.orientation.w,
                                            odometry.twist.twist.linear.x,
                                            odometry.twist.twist.linear.y,
                                            odometry.twist.twist.linear.z,
                                            odometry.twist.twist.angular.x,
                                            odometry.twist.twist.angular.y,
                                            odometry.twist.twist.angular.z
                                        )
                                    }
                                    Err(e) => {
                                        eprintln!("Warning: Failed to deserialize Odometry message {}: {}", message_count + 1, e);
                                        format!(
                                            "{},{},deserialize_error",
                                            message.timestamp,
                                            message.data.len()
                                        )
                                    }
                                }
                            }
                            "geometry_msgs/msg/PointStamped" => {
                                match deserialize_pointstamped_message(&message.data) {
                                    Ok(point) => {
                                        format!(
                                            "{},{},{},{},{:.6},{:.6},{:.6}",
                                            message.timestamp,
                                            point.header.stamp.sec,
                                            point.header.stamp.nanosec,
                                            point.header.frame_id,
                                            point.point.x,
                                            point.point.y,
                                            point.point.z
                                        )
                                    }
                                    Err(e) => {
                                        eprintln!("Warning: Failed to deserialize PointStamped message {}: {}", message_count + 1, e);
                                        format!(
                                            "{},{},deserialize_error",
                                            message.timestamp,
                                            message.data.len()
                                        )
                                    }
                                }
                            }
                            _ => {
                                // Fallback to hex output for unsupported message types
                                let hex_data = message
                                    .data
                                    .iter()
                                    .map(|b| format!("{:02x}", b))
                                    .collect::<Vec<_>>()
                                    .join("");
                                format!("{},{},{}", message.timestamp, message.data.len(), hex_data)
                            }
                        };

                        // Write message data to file
                        writeln!(writer, "{}", output_line)?;

                        message_count += 1;
                        total_bytes += message.data.len();

                        // Print progress every 1000 messages
                        if message_count % 1000 == 0 {
                            println!("Processed {} messages...", message_count);
                        }
                    }
                    Err(e) => {
                        eprintln!("Error reading message {}: {}", message_count + 1, e);
                        break;
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("Error getting message iterator: {}", e);
            std::process::exit(1);
        }
    }

    // Flush and close the output file
    writer.flush()?;
    drop(writer);

    // Print summary
    println!("\n=== Extraction Complete ===");
    println!("Messages extracted: {}", message_count);
    println!(
        "Total data size: {} bytes ({:.2} KB)",
        total_bytes,
        total_bytes as f64 / 1024.0
    );
    println!("Output written to: {}", output_file);

    if message_count == 0 {
        println!("\nWarning: No messages found for topic '{}'", topic_name);
        println!("This could mean:");
        println!("  - The topic name is incorrect");
        println!("  - The topic has no messages in this time range");
        println!("  - There's an issue with the bag file");
    } else {
        println!("\nTo view the extracted data:");
        println!("  head -20 {}", output_file);
        println!("  tail -10 {}", output_file);
    }

    // Close the reader
    reader.close()?;

    Ok(())
}

/// Deserialize an IMU message from CDR data
fn deserialize_imu_message(data: &[u8]) -> Result<Imu, Box<dyn std::error::Error>> {
    let mut deserializer = CdrDeserializer::new(data)?;
    let imu = Imu::from_cdr(&mut deserializer)?;
    Ok(imu)
}

/// Deserialize a TransformStamped message from CDR data
fn deserialize_transform_message(
    data: &[u8],
) -> Result<TransformStamped, Box<dyn std::error::Error>> {
    let mut deserializer = CdrDeserializer::new(data)?;
    let transform = TransformStamped::from_cdr(&mut deserializer)?;
    Ok(transform)
}

/// Deserialize a PoseWithCovarianceStamped message from CDR data
fn deserialize_pose_with_covariance_message(
    data: &[u8],
) -> Result<PoseWithCovarianceStamped, Box<dyn std::error::Error>> {
    let mut deserializer = CdrDeserializer::new(data)?;
    let pose = PoseWithCovarianceStamped::from_cdr(&mut deserializer)?;
    Ok(pose)
}

/// Deserialize a NavSatFix message from CDR data
fn deserialize_navsat_message(data: &[u8]) -> Result<NavSatFix, Box<dyn std::error::Error>> {
    let mut deserializer = CdrDeserializer::new(data)?;
    let navsat = NavSatFix::from_cdr(&mut deserializer)?;
    Ok(navsat)
}

/// Deserialize a String message from CDR data
fn deserialize_string_message(data: &[u8]) -> Result<StdString, Box<dyn std::error::Error>> {
    let mut deserializer = CdrDeserializer::new(data)?;
    let string_msg = StdString::from_cdr(&mut deserializer)?;
    Ok(string_msg)
}

/// Deserialize a PointCloud2 message from CDR data
fn deserialize_pointcloud2_message(data: &[u8]) -> Result<PointCloud2, Box<dyn std::error::Error>> {
    let mut deserializer = CdrDeserializer::new(data)?;
    let pointcloud = PointCloud2::from_cdr(&mut deserializer)?;
    Ok(pointcloud)
}

/// Deserialize an Image message from CDR data
fn deserialize_image_message(data: &[u8]) -> Result<Image, Box<dyn std::error::Error>> {
    let mut deserializer = CdrDeserializer::new(data)?;
    let image = Image::from_cdr(&mut deserializer)?;
    Ok(image)
}

/// Deserialize an Odometry message from CDR data
fn deserialize_odometry_message(data: &[u8]) -> Result<Odometry, Box<dyn std::error::Error>> {
    let mut deserializer = CdrDeserializer::new(data)?;
    let odometry = Odometry::from_cdr(&mut deserializer)?;
    Ok(odometry)
}

/// Deserialize a PointStamped message from CDR data
fn deserialize_pointstamped_message(
    data: &[u8],
) -> Result<PointStamped, Box<dyn std::error::Error>> {
    let mut deserializer = CdrDeserializer::new(data)?;
    let point = PointStamped::from_cdr(&mut deserializer)?;
    Ok(point)
}
