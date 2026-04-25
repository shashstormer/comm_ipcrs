use async_trait::async_trait;
use commipcrs::{CommData, CommIPC, EventHandler, Result};
use serde_json::{Value, json};

struct SimpleHandler;
#[async_trait]
impl EventHandler for SimpleHandler {
    async fn handle(&self, cd: CommData) -> Result<Value> {
        println!("Received call on Rust provider: {:?}", cd.event);
        Ok(json!({"status": "received", "by": "rust-tester"}))
    }
}

struct AlertHandler;
#[async_trait]
impl EventHandler for AlertHandler {
    async fn handle(&self, cd: CommData) -> Result<Value> {
        println!("!!! ALERT RECEIVED in Rust: {:?}", cd.data);
        Ok(Value::Null)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let socket_path = "/run/user/1000/comm_ipc/comm_default.sock";
    let client = CommIPC::new(Some("rust-tester-final-3".to_string()), socket_path);

    println!("Connecting to {}...", socket_path);
    client.connect().await?;
    println!("Connected!");

    let channel = client.open("system-monitor").await?;
    println!("Channel 'system-monitor' opened!");

    // --- TEST 1: Standard RPC ---
    println!("\n[TEST 1] Calling 'get_status'...");
    let res = channel.event("get_status", json!({})).await?;
    println!("Response: {:?}", res.data);

    // --- TEST 2: Streaming ---
    println!("\n[TEST 2] Consuming stream 'monitor_logs'...");
    let mut stream_rx = client
        .stream("system-monitor", "monitor_logs", json!({}))
        .await?;
    let mut count = 0;
    while let Some(cd) = stream_rx.recv().await {
        println!("Stream Update {}: {:?}", count, cd.data);
        count += 1;
        if count >= 5 {
            break;
        }
    }

    // --- TEST 3: Pydantic Validated Event ---
    println!("\n[TEST 3] Calling 'secure_task' with valid data...");
    let res = channel
        .event("secure_task", json!({"task_id": 123, "payload": "hello"}))
        .await?;
    println!("Response: {:?}", res.data);

    println!("\n[TEST 3b] Calling 'secure_task' with INVALID data (should return error)...");
    match channel
        .event("secure_task", json!({"task_id": "not-an-int"}))
        .await
    {
        Ok(r) => println!("Unexpected success: {:?}", r.data),
        Err(e) => println!("Caught expected error: {}", e),
    }

    // --- TEST 4: Load-Balanced Group Event ---
    println!("\n[TEST 4] Calling 'run_job' (group event)...");
    let res = channel
        .group("compute-cluster")
        .event("run_job", json!({}))
        .await?;
    println!("Response: {:?}", res.data);

    // --- TEST 5: Pub/Sub ---
    println!("\n[TEST 5] Subscribing to 'emergency_broadcast'...");
    channel.create_subscription("emergency_broadcast").await?; // Become owner to allow publishing
    channel
        .subscribe("emergency_broadcast", AlertHandler)
        .await?;
    println!(
        "Subscribed! Now let's publish something from Rust to see if we get it (and the Python engine should too)."
    );

    // We can publish to test our own listener
    println!("Publishing alert from Rust...");
    client
        .publish(
            "system-monitor",
            "emergency_broadcast",
            json!({"msg": "TEST ALERT FROM RUST"}),
        )
        .await?;

    // Wait a bit for the alert to come back
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // --- TEST 6: Rust as Provider ---
    println!("\n[TEST 6] Registering Rust provider for 'ping_rust'...");
    channel.add_event("ping_rust", SimpleHandler).await?;
    println!(
        "Provider registered. You can now call 'ping_rust' on 'system-monitor' channel from Python."
    );

    println!("\nAll tests initiated. Staying alive for 5 seconds to handle incoming events...");
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    client.close().await?;
    println!("Done!");

    Ok(())
}
