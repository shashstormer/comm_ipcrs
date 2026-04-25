use async_trait::async_trait;
use commipcrs::{CommData, CommIPC, EventHandler, Result};
use serde_json::{Value, json};

struct AlertListener;
#[async_trait]
impl EventHandler for AlertListener {
    async fn handle(&self, cd: CommData) -> Result<Value> {
        println!("[Caller] !!! RECEIVED ALERT: {:?}", cd.data);
        Ok(Value::Null)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let socket_path = "/run/user/1000/comm_ipc/comm_default.sock";
    let client = CommIPC::new(Some("rust-caller-client".to_string()), socket_path);

    println!("Connecting caller...");
    client.connect().await?;
    println!("Connected!");

    let channel = client.open("rust-demo-channel").await?;
    println!("Channel 'rust-demo-channel' opened!");

    // 1. Call Standard RPC
    println!("\n[TEST 1] Calling get_status...");
    let res = channel.event("get_status", json!({})).await?;
    println!("Result: {:?}", res.data);

    // 2. Call Group RPC
    println!("\n[TEST 2] Calling heavy-tasks.process...");
    let res = channel
        .group("heavy-tasks")
        .event("process", json!({"work": "some data"}))
        .await?;
    println!("Result: {:?}", res.data);

    // 3. Consume Stream
    println!("\n[TEST 3] Consuming get_logs stream...");
    let mut stream = client
        .stream("rust-demo-channel", "get_logs", json!({}))
        .await?;
    while let Some(msg) = stream.recv().await {
        println!("Stream msg: {:?}", msg.data);
        if msg.is_final {
            break;
        }
    }

    // 4. Subscribe to alerts
    println!("\n[TEST 4] Subscribing to 'alerts'...");
    channel.subscribe("alerts", AlertListener).await?;
    println!("Subscribed. Waiting for an alert (if the provider sends one).");

    // Since only the owner can publish, and the provider is the owner,
    // we can't publish from here. But we can keep running to see if the provider
    // (or another owner) sends something.

    println!("\nCaller finished its active tests. Waiting 5s for incoming alerts...");
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    client.close().await?;
    Ok(())
}
