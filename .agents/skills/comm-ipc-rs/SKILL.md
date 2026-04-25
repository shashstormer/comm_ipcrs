---
name: comm-ipc-rs
description: Instructions and guidelines for building asynchronous, type-safe IPC (Inter-Process Communication) applications using the CommIPC Rust client. Use this skill whenever setting up a Rust-based CommIPC client, provider, or integration.
license: LGPL-3.0
compatibility: Requires Rust edition 2024 and comm-ipc-rs crate.
metadata:
  version: "1.0"
  target: "comm-ipc-rs"
---

# CommIPC Rust Skill

`comm-ipc-rs` is the high-performance Rust implementation of the CommIPC protocol. It allows Rust applications to seamlessly participate in a CommIPC mesh, communicating with other Rust, Python, or Federated components via Unix Domain Sockets or TCP.

When building applications using `comm-ipc-rs`, strictly follow the patterns and architectural guidelines outlined in this document.

## System Architecture

CommIPC uses a central Hub/Server to route messages between clients. It does not use a direct peer-to-peer connection.
1. **Server**: Typically the Python `CommIPCServer`, acting as the central router.
2. **Rust Clients (`CommIPC`)**: Connect to the server, join channels, and provide/consume events, streams, or pub/sub messages.
3. **Async Core**: Built on `tokio` for high-performance non-blocking I/O.

## Step-by-Step Instructions

### 1. Connecting to the Hub
Rust clients must initialize a `CommIPC` instance and connect to the server socket.
- Use `CommIPC::new(client_id, socket_path)` to create a client.
- Call `client.connect().await` to establish the connection and start the background listener loop.
- Refer to [`scripts/client_setup.rs`](scripts/client_setup.rs).

### 2. Standard RPC (Request-Response)
To perform RPC calls, open a channel and use the `call` method.
- **Provider**: Register a handler using `channel.add_event("event_name", handler).await`. Handlers are async closures or functions.
- **Consumer**: Call the event using `channel.call("event_name", json!({"data": "value"})).await`.
- Refer to [`scripts/client_rpc.rs`](scripts/client_rpc.rs).

### 3. Data Streaming
Rust's `Stream` trait is used for receiving large or continuous data.
- **Provider**: Use `channel.add_stream("name", handler).await`. The handler must return an async stream (e.g., using `async-stream` crate).
- **Consumer**: Use `channel.stream("name", data).await` which returns a `CommIPCStream`. Iterate over it using `while let Some(msg) = stream.next().await`.
- Refer to [`scripts/client_stream.rs`](scripts/client_stream.rs).

### 4. Publisher / Subscriber
For broadcasting events:
- **Subscriber**: Use `channel.subscribe("topic", callback).await`. The callback will be executed whenever data is published to that topic.
- **Publisher**: Use `channel.publish("topic", data).await`. Note that you should declare the subscription schema on the server-side or via another client to ensure subscribers know what to expect.
- Refer to [`scripts/client_pubsub.rs`](scripts/client_pubsub.rs).

### 5. Group Calls (Load Balancing)
Participate in load-balanced groups:
- **Provider**: Use `channel.add_event_to_group("group_name", "event_name", handler).await`.
- **Consumer**: Call using `channel.group_call("group_name", "event_name", data).await`.
- Refer to [`scripts/client_groups.rs`](scripts/client_groups.rs).

## Technical References
- [API Reference](references/API_REFERENCE.md): Rust-specific method signatures and trait bounds.
- [Security Model](references/SECURITY.md): Details on PBKDF2 key derivation and HMAC-SHA256 message signing.

## Best Practices
- **Error Handling**: Always handle `CommIPCError`. Network disconnections and deserialization failures are the most common issues.
- **Task Spawning**: The `CommIPC` client runs a background listener task. Do not block the thread where the client is running.
- **JSON Serialization**: Use the `serde_json::json!` macro for constructing ad-hoc payloads, or define structs with `#[derive(Serialize, Deserialize)]`.
- **Resource Cleanup**: Explicitly call `client.close().await` to gracefully notify the server of your departure.
