# rosbags-rs

[![CI](https://github.com/your-org/rosbags-rs/workflows/CI/badge.svg)](https://github.com/your-org/rosbags-rs/actions)
[![Crates.io](https://img.shields.io/crates/v/rosbags-rs.svg)](https://crates.io/crates/rosbags-rs)
[![Documentation](https://docs.rs/rosbags-rs/badge.svg)](https://docs.rs/rosbags-rs)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A high-performance Rust library for reading ROS2 bag files with **full Python rosbags compatibility**. This library provides comprehensive functionality to read ROS2 bag files in both SQLite3 and MCAP formats, with guaranteed byte-for-byte identical results compared to the Python rosbags library.

## 🚀 Features

- ✅ **Complete ROS2 bag reading** - SQLite3 and MCAP formats
- ✅ **94+ ROS2 message types** - Full support across all major categories
- ✅ **Python rosbags compatibility** - Byte-for-byte identical results
- ✅ **High performance** - Zero-copy message reading where possible
- ✅ **Comprehensive CDR deserialization** - All standard ROS2 message types
- ✅ **Advanced filtering** - By topic, time range, and message type
- ✅ **Compression support** - zstd compressed bags
- ✅ **Type-safe error handling** - Comprehensive error types
- ✅ **Self-contained tests** - No external dependencies required
- ✅ **Production ready** - Extensive test coverage and CI/CD

## 🎯 Supported ROS2 Versions

- **ROS2 Jazzy Jalopy** (LTS)
- **ROS2 Humble Hawksbill** (LTS)
- **ROS2 Foxy Fitzroy** (LTS)

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rosbags-rs = "0.2.0"
```

## 🚀 Quick Start

```rust
use rosbags_rs::{Reader, ReaderError};
use std::path::Path;

fn main() -> Result<(), ReaderError> {
    // Open a ROS2 bag
    let bag_path = Path::new("/path/to/rosbag2_directory");
    let mut reader = Reader::new(bag_path)?;
    reader.open()?;

    // Print basic information
    println!("Duration: {:.2} seconds", reader.duration() as f64 / 1_000_000_000.0);
    println!("Messages: {}", reader.message_count());

    // List topics with detailed information
    for topic in reader.topics() {
        println!("📡 Topic: {}", topic.name);
        println!("   Type: {}", topic.message_type);
        println!("   Messages: {}", topic.message_count);
        println!("   Serialization: {}", topic.serialization_format);
    }

    // Read all messages
    for message_result in reader.messages()? {
        let message = message_result?;
        println!("📨 Topic: {}, Timestamp: {}, Size: {} bytes",
                 message.topic, message.timestamp, message.data.len());
    }

    Ok(())
}
```

## 📚 Examples

### Basic Bag Information

```bash
cargo run --example bag_info /path/to/rosbag2_directory
```

### List All Topics

```bash
cargo run --example list_topics /path/to/rosbag2_directory
```

For detailed topic information:

```bash
VERBOSE=1 cargo run --example list_topics /path/to/rosbag2_directory
```

### Extract Specific Topic

```bash
cargo run --example extract_topic /path/to/rosbag2_directory /topic_name output.txt
```

### Read Test Bags (Demo)

```bash
cargo run --example read_test_bags
```

## 🗂️ Supported ROS2 Bag Formats

### Storage Formats

- ✅ **SQLite3** - Primary storage format for ROS2 bags
- ✅ **MCAP** - Modern container format with high performance

### Compression

- ✅ **None** - Uncompressed bags
- ✅ **zstd** - File-level and message-level compression
- ❌ **lz4** - Not currently supported

### Bag Versions

- ✅ **Version 1-9** - All current ROS2 bag versions supported

## 🏗️ Architecture

The library is structured into several modules:

- **`reader`** - Main `Reader` struct for opening and reading bags
- **`metadata`** - Parsing and validation of `metadata.yaml` files
- **`storage`** - Storage backend implementations (SQLite3, MCAP)
- **`types`** - Core data structures (Connection, Message, TopicInfo, etc.)
- **`error`** - Comprehensive error handling
- **`cdr`** - CDR message deserialization
- **`messages`** - ROS2 message type definitions

## 🛡️ Error Handling

The library uses the `thiserror` crate for structured error handling:

```rust
use rosbags_rs::{Reader, ReaderError};

match Reader::new("/path/to/bag") {
    Ok(reader) => { /* success */ },
    Err(ReaderError::BagNotFound { path }) => {
        eprintln!("Bag not found: {}", path.display());
    },
    Err(ReaderError::UnsupportedVersion { version }) => {
        eprintln!("Unsupported bag version: {}", version);
    },
    Err(e) => {
        eprintln!("Other error: {}", e);
    }
}
```

## 🔍 Advanced Usage

### Filter Messages by Topic

```rust
use rosbags_rs::Reader;

let mut reader = Reader::new("/path/to/bag")?;
reader.open()?;

// Filter messages for specific topics
let target_topics = vec!["/camera/image_raw", "/imu/data"];
for message_result in reader.messages()? {
    let message = message_result?;
    if target_topics.contains(&message.topic.as_str()) {
        println!("Found message on topic: {}", message.topic);
    }
}
```

### Filter by Time Range

```rust
// Filter by time range (nanoseconds since epoch)
let start_time = 1234567890000000000;
let end_time = 1234567891000000000;

for message_result in reader.messages()? {
    let message = message_result?;
    if message.timestamp >= start_time && message.timestamp <= end_time {
        println!("Message in time range: {}", message.timestamp);
    }
}
```

## 📊 Supported ROS2 Message Types

This library supports **94+ ROS2 message types** across all major categories:

### Core Message Categories
- **📡 std_msgs** - Standard message types (String, Header, etc.)
- **📐 geometry_msgs** - Geometric primitives (Point, Pose, Transform, etc.)
- **🤖 sensor_msgs** - Sensor data (Image, PointCloud2, Imu, NavSatFix, etc.)
- **🗺️ nav_msgs** - Navigation messages (Odometry, Path, etc.)
- **🔧 diagnostic_msgs** - System diagnostics
- **⏰ builtin_interfaces** - Time and duration types

### Cross-Compatibility Guarantee

This Rust implementation provides **100% compatibility** with the Python rosbags library:

| Feature | Python rosbags | rosbags-rs |
|---------|----------------|------------|
| SQLite3 reading | ✅ | ✅ |
| MCAP reading | ✅ | ✅ |
| CDR deserialization | ✅ | ✅ |
| Message filtering | ✅ | ✅ |
| Compression support | ✅ | ✅ |
| Type safety | ❌ | ✅ |
| Memory safety | ❌ | ✅ |
| Performance | Good | **Excellent** |
| Cross-validation | N/A | **Byte-for-byte identical** |

## 🚀 Performance

- **Zero-copy message reading** where possible
- **Efficient memory usage** with Rust's ownership system
- **Fast SQLite3 and MCAP parsing** with optimized queries
- **Comprehensive benchmarks** included in test suite

## 🧪 Testing

The library includes a comprehensive, self-contained test suite:

### Run All Tests

```bash
cargo test
```

### Run Specific Test Categories

```bash
# Integration tests with cross-validation
cargo test --test integration_tests

# Performance tests
cargo test --release

# Feature-specific tests
cargo test --features sqlite
cargo test --features mcap
```

### Test Coverage

The test suite validates:
- ✅ **All 94 message types** across 6 categories
- ✅ **188 test messages** (2 per topic) in both SQLite3 and MCAP formats
- ✅ **Byte-for-byte compatibility** with Python rosbags library
- ✅ **Message filtering** by topic and timestamp
- ✅ **Metadata consistency** between formats
- ✅ **Error handling** for edge cases

All tests are **self-contained** and run without external dependencies or Python installations.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

### Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/amin-abouee/rosbags-rs.git
   cd rosbags-rs
   ```

2. **Install Rust** (1.70+ required)
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

3. **Run tests**
   ```bash
   cargo test
   ```

4. **Run examples**
   ```bash
   cargo run --example bag_info /path/to/bag
   ```

5. **Check formatting and linting**
   ```bash
   cargo fmt --check
   cargo clippy -- -D warnings
   ```

### Contribution Guidelines

- Follow Rust best practices and idioms
- Add tests for new functionality
- Update documentation for API changes
- Ensure CI passes on all platforms

## 📄 License

This project is licensed under the **Apache License 2.0** - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Inspired by the excellent [rosbags](https://gitlab.com/ternaris/rosbags) Python library by Ternaris
- Built for the ROS2 robotics ecosystem
- Thanks to all contributors and the Rust community

