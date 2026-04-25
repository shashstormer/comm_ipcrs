# Security Model

CommIPC implements several layers of security to ensure data integrity and authorized access.

## 1. Connection Handshake (HMAC-SHA256)
When a client connects to a server that has a `connection_secret` configured:
1. The server sends a random `challenge` string.
2. The client must compute a proof: `HMAC-SHA256(secret_hash, challenge)`.
3. The server verifies the proof before allowing any further messages.

In Rust, this is handled automatically if you provide a secret:
```rust
let mut client = CommIPC::new("client", socket);
client.set_connection_secret("my_secret".to_string());
client.connect().await?;
```

## 2. Channel Access (Passwords)
Channels can be protected with passwords. 
- The first client to join a channel becomes the **Owner**.
- The owner can set a password using `client.set_password(chan, pass)`.
- Subsequent clients must provide the password in `client.open(chan, password)`.

## 3. Message Integrity (Signatures)
For high-security environments, every message can be signed. The Hub can be configured to verify these signatures to prevent tampering.

## 4. Transport Security (TLS)
When using TCP instead of Unix Domain Sockets, CommIPC supports full SSL/TLS encryption for the transport layer.
