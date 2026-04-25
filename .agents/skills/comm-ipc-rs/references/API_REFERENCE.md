# API Reference (Rust)

## `CommIPC`
The main entry point for the library. Manages the connection and background listener.

### `new(client_id: Option<String>, socket_path: &str) -> Arc<Self>`
Creates a new client instance. If `client_id` is `None`, a random one is generated.

### `set_connection_secret(&mut self, secret: String)`
Sets the HMAC secret for the initial connection handshake with the server.

### `connect(&self) -> Result<()>`
Connects to the Unix socket and starts the background listener and heartbeat tasks.

### `open(&self, channel_name: &str) -> Result<Arc<CommIPCChannel>>`
Joins a channel. Returns an `Arc<CommIPCChannel>` for performing operations within that channel.

### `close(&self) -> Result<()>`
Gracefully shuts down the connection.

---

## `CommIPCChannel`
Handles operations scoped to a specific channel.

### `event(&self, name: &str, data: Value) -> Result<CommData>`
Calls a remote RPC event on this channel.

### `add_event<H>(&self, name: &str, handler: H) -> Result<()>`
Registers an RPC provider. `H` must implement the `EventHandler` trait.

### `add_stream<H>(&self, name: &str, handler: H) -> Result<()>`
Registers a streaming provider.

### `subscribe<H>(&self, sub_name: &str, handler: H) -> Result<()> `
Subscribes to a pub/sub topic and registers a handler for incoming messages.

### `create_subscription(&self, sub_name: &str) -> Result<()>`
Registers a subscription schema on the server (required before publishing).

### `group(&self, name: &str) -> CommIPCGroup`
Returns a group helper for load-balanced events.

---

## `CommIPCGroup`
Scoped helper for load-balanced group operations.

### `provide<H>(&self, event: &str, handler: H) -> Result<()>`
Registers a provider within this group.

### `event(&self, event: &str, data: Value) -> Result<CommData>`
Calls an event within this group (load-balanced across all providers in the group).

---

## `EventHandler` Trait
The trait used for all event and subscription handlers.

```rust
#[async_trait]
pub trait EventHandler: Send + Sync {
    async fn handle(&self, cd: CommData) -> Result<Value>;
}
```

You can implement this trait for your own structs, or use the provided blanket implementations for certain closures.
