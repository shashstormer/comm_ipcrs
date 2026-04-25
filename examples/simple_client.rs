use async_trait::async_trait;
use commipcrs::{CommData, CommIPC, EventHandler, Result};
use serde_json::{Value, json};

struct MyHandler;

#[async_trait]
impl EventHandler for MyHandler {
    async fn handle(&self, cd: CommData) -> Result<Value> {
        println!("Received call: {:?}", cd);
        Ok(json!({"status": "ok", "received": cd.data}))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Start with a default socket path
    let socket_path = std::env::var("COMM_IPC_SOCKET")
        .unwrap_or_else(|_| "/run/user/1000/comm_ipc/comm_default.sock".to_string());
    let client = CommIPC::new(None, &socket_path);

    println!("Connecting...");
    client.connect().await?;
    println!("Connected!");

    let channel = client.open("test_channel").await?;
    println!("Opened channel");

    channel.add_event("hello", MyHandler).await?;
    println!("Registered event 'hello'");

    // Call ourselves
    let res = channel.event("hello", json!({"name": "Rust"})).await?;
    println!("Call response: {:?}", res.data);

    // Keep running to receive calls
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    Ok(())
}
