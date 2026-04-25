use commipcrs::client::CommIPC;
use serde_json::json;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket_path = env::var("COMM_IPC_SOCKET")
        .unwrap_or_else(|_| "/tmp/comm_default.sock".to_string());

    // 1. Initialize client
    let client = CommIPC::new("rust-client", &socket_path);

    // 2. Connect to Hub
    client.connect().await?;
    println!("Connected to CommIPC Hub!");

    // 3. Open a channel
    let channel = client.open("demo").await?;

    // 4. Perform an RPC call
    let response = channel.call("hello", json!({"name": "Rust"})).await?;
    println!("Received: {:?}", response.data);

    // 5. Close connection
    client.close().await?;
    Ok(())
}
