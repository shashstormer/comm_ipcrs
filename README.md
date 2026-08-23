# CommIPC-RS

A high-performance Rust client for the `comm_ipc` protocol, providing seamless inter-process communication via Unix domain sockets. This library is designed to be fully compatible with the Python implementation of `comm_ipc`.

## Features

- **Unix Domain Sockets Only**: Lightweight and secure local communication.
- **Handshake & Security**: HMAC-SHA256 authentication and PBKDF2-derived channel encryption keys.
- **Request-Response (RPC)**: Standard async call/reply pattern.
- **Bi-directional Streaming**: Support for async generators and long-running data flows.
- **Pub/Sub**: Subscription ownership and broadcast capabilities.
- **Load-Balanced Groups**: Organize providers into groups for automatic request distribution.
- **Type Safety**: Built on `serde` and `serde_json` for robust data handling.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
commipcrs = "0.1.0"
tokio = { version = "1.0", features = ["full"] }
serde_json = "1.0"
```

## Quick Start

### As a Caller

```rust
use commipcrs::{CommIPC, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let client = CommIPC::new(Some("my-client".into()), "/tmp/comm_default.sock");
    client.connect().await?;

    let channel = client.open("system-monitor").await?;
    
    // Call an RPC event
    let res = channel.event("get_status", json!({})).await?;
    println!("Status: {:?}", res.data);

    Ok(())
}
```

### As a Provider

```rust
use commipcrs::{CommIPC, CommData, EventHandler, Result};
use serde_json::{json, Value};
use async_trait::async_trait;

struct MyHandler;

#[async_trait]
impl EventHandler for MyHandler {
    async fn handle(&self, cd: CommData) -> Result<Value> {
        Ok(json!({"status": "online", "received": cd.data}))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = CommIPC::new(Some("my-provider".into()), "/tmp/comm_default.sock");
    client.connect().await?;

    let channel = client.open("my-channel").await?;
    channel.add_event("check", MyHandler).await?;

    // Keep the provider running
    loop { tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; }
}
```

## Advanced Usage

### Streaming
Consume data streams from a provider:

```rust
let mut stream = channel.stream("my-channel", "log_stream", json!({})).await?;
while let Some(msg) = stream.recv().await {
    println!("Received chunk: {:?}", msg.data);
    if msg.is_final { break; }
}
```

### Load-Balanced Groups
Register a provider as part of a cluster:

```rust
let group = channel.group("worker-cluster");
group.provide("process_task", MyTaskHandler).await?;
```

## Running Tests

The library includes a comprehensive integration test suite that verifies compatibility with the Python server.

```bash
# Run all tests
cargo test

# Run a specific integration test
cargo test --test integration
```

Note: Integration tests expect a `comm_ipc` server to be reachable. You can specify the socket path via the `COMM_IPC_SOCKET` environment variable.

## Protocol Compatibility

`commipcrs` is designed to match the signature and framing logic of the [Python comm_ipc](https://github.com/shashstormer/comm_ipc) library, including:
- 4-byte big-endian length prefix.
- MsgPack serialization.
- Canonical JSON key sorting for cryptographic signatures.

## License

LGPL-3.0
