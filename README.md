# rosbag2-rs

A Rust library for reading ROS2 bag files, inspired by the Python [rosbags](https://gitlab.com/ternaris/rosbags) library.

## Features

- ✅ Read ROS2 bag files in SQLite3 format
- ✅ Parse `metadata.yaml` files
- ✅ Support for multiple topics and message types
- ✅ Filter messages by topic, time range, and connections
- ✅ Support for compressed bag files (zstd)
- ✅ Type-safe error handling
- 🚧 MCAP format support (planned)
- 🚧 CDR message deserialization (planned)
- 🚧 Message writing capabilities (planned)

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rosbag2-rs = "0.1.0"
```

## Quick Start

```rust
use rosbag2_rs::{Reader, ReaderError};
use std::path::Path;

fn main() -> Result<(), ReaderError> {
    // Open a ROS2 bag
    let bag_path = Path::new("/path/to/rosbag2_directory");
    let mut reader = Reader::new(bag_path)?;
    reader.open()?;

    // Print basic information
    println!("Duration: {} ns", reader.duration());
    println!("Messages: {}", reader.message_count());

    // List topics
    for topic in reader.topics() {
        println!("Topic: {}, Type: {}, Messages: {}", 
                 topic.name, topic.message_type, topic.message_count);
    }

    // Read messages
    for message in reader.messages()? {
        let msg = message?;
        println!("Topic: {}, Timestamp: {}, Size: {} bytes",
                 msg.topic, msg.timestamp, msg.data.len());
    }

    Ok(())
}
```

## Examples

### List Topics

```bash
cargo run --example list_topics /path/to/rosbag2_directory
```

### Read Bag Information

```bash
cargo run --example read_bag /path/to/rosbag2_directory
```

For detailed topic information:

```bash
VERBOSE=1 cargo run --example list_topics /path/to/rosbag2_directory
```

## Supported ROS2 Bag Formats

### Storage Formats

- ✅ **SQLite3** - Primary storage format for ROS2 bags
- 🚧 **MCAP** - Modern container format (planned)

### Compression

- ✅ **None** - Uncompressed bags
- ✅ **zstd** - File-level and message-level compression
- ❌ **lz4** - Not supported

### Bag Versions

- ✅ Version 1-9 - All current ROS2 bag versions supported

## Architecture

The library is structured into several modules:

- **`reader`** - Main `Reader` struct for opening and reading bags
- **`metadata`** - Parsing and validation of `metadata.yaml` files
- **`storage`** - Storage backend implementations (SQLite3, MCAP)
- **`types`** - Core data structures (Connection, Message, TopicInfo, etc.)
- **`error`** - Comprehensive error handling

## Error Handling

The library uses the `thiserror` crate for structured error handling:

```rust
use rosbag2_rs::{Reader, ReaderError};

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

## Filtering Messages

You can filter messages by various criteria:

```rust
// Filter by time range (nanoseconds since epoch)
let start_time = 1234567890000000000;
let end_time = 1234567891000000000;
let messages = reader.messages_filtered(None, Some(start_time), Some(end_time))?;

// Filter by specific topics
let connections: Vec<_> = reader.connections()
    .iter()
    .filter(|c| c.topic == "/camera/image_raw")
    .cloned()
    .collect();
let messages = reader.messages_filtered(Some(&connections), None, None)?;
```

## Comparison with Python rosbags

This Rust implementation aims to provide similar functionality to the Python rosbags library:

| Feature | Python rosbags | rosbag2-rs |
|---------|----------------|------------|
| SQLite3 reading | ✅ | ✅ |
| MCAP reading | ✅ | 🚧 |
| CDR deserialization | ✅ | 🚧 |
| Message writing | ✅ | 🚧 |
| Compression support | ✅ | ✅ |
| Type safety | ❌ | ✅ |
| Memory safety | ❌ | ✅ |
| Performance | Good | Excellent |

## Development Status

This library is currently in **early development**. The core reading functionality is implemented and tested, but some advanced features are still being developed:

### Current Status (v0.1.0)
- ✅ Basic bag reading
- ✅ Metadata parsing
- ✅ SQLite3 storage backend
- ✅ Topic and connection information
- ✅ Message iteration (raw data)

### Planned Features
- 🚧 CDR message deserialization
- 🚧 MCAP storage backend
- 🚧 Message writing capabilities
- 🚧 Advanced filtering options
- 🚧 Async support

## Testing

Run the test suite:

```bash
cargo test
```

Run integration tests:

```bash
cargo test --test integration_tests
```

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

### Development Setup

1. Clone the repository
2. Install Rust (1.70+ recommended)
3. Run tests: `cargo test`
4. Run examples: `cargo run --example read_bag /path/to/bag`

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by the excellent [rosbags](https://gitlab.com/ternaris/rosbags) Python library by Ternaris
- Built for the ROS2 robotics ecosystem
