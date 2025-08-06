# Feature Flags

The rosbags-rs library supports several feature flags to customize functionality:

## Available Features

- `sqlite` - Enable SQLite3 storage backend (default)
- `mcap` - Enable MCAP storage backend (default)
- `compression` - Enable compression support (default)
- `bin-tools` - Enable binary tool dependencies (hex, image) for utilities (default)
- `async` - Enable async support (optional)
- `write-only` - Enable only writing functionality with minimal dependencies (optional)

## Usage

### Default Features

By default, the library includes SQLite3, MCAP, and compression support:

```toml
[dependencies]
rosbags-rs = "0.3.4"
```

### Custom Features

You can customize which features to include:

```toml
[dependencies]
rosbags-rs = { version = "0.3.4", features = ["sqlite", "mcap", "compression"] }
```

### Minimal Write-Only Installation

For applications that only need to write bag files with minimal dependencies:

```toml
[dependencies]
rosbags-rs = { version = "0.3.4", default-features = false, features = ["write-only"] }
```

The `write-only` feature provides just the writing functionality with minimal dependencies, making it ideal for embedded systems or applications where minimizing dependencies is important.

### Without Binary Tool Dependencies

If you're using the library in your own application and don't need the binary utilities:

```toml
[dependencies]
rosbags-rs = { version = "0.3.4", default-features = false, features = ["sqlite", "mcap", "compression"] }
```

This configuration excludes the `bin-tools` feature, avoiding the installation of dependencies like `hex` and `image` that are only used by the binary utilities.