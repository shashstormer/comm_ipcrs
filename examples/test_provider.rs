use async_trait::async_trait;
use commipcrs::{CommData, CommIPC, EventHandler, Result};
use serde_json::{Value, json};
use std::time::Duration;

struct StatusHandler;
#[async_trait]
impl EventHandler for StatusHandler {
    async fn handle(&self, cd: CommData) -> Result<Value> {
        println!("[Provider] Received get_status from {}", cd.sender_id);
        Ok(json!({"status": "online", "provider": "rust-provider"}))
    }
}

struct JobHandler;
#[async_trait]
impl EventHandler for JobHandler {
    async fn handle(&self, cd: CommData) -> Result<Value> {
        println!("[Provider] Received run_job in group from {}", cd.sender_id);
        tokio::time::sleep(Duration::from_millis(500)).await;
        Ok(json!({"done": true, "worker": "rust-worker-1"}))
    }
}

struct LogStreamHandler {
    parent: std::sync::Arc<CommIPC>,
}
#[async_trait]
impl EventHandler for LogStreamHandler {
    async fn handle(&self, cd: CommData) -> Result<Value> {
        let rid = cd.request_id.clone();
        let target_id = cd.sender_id.clone();
        let channel = cd.channel.clone();
        let event = cd.event.clone();
        let parent = self.parent.clone();

        println!("[Provider] Starting stream for {}", target_id);

        tokio::spawn(async move {
            for i in 0..5 {
                tokio::time::sleep(Duration::from_millis(300)).await;
                let mut resp = CommData::new();
                resp.channel = channel.clone();
                resp.event = event.clone();
                resp.data = json!({"log": format!("Entry #{}", i)});
                resp.request_id = rid.clone();
                resp.target_id = Some(target_id.clone());
                resp.is_stream = true;
                resp.is_final = i == 4;

                let _ = parent
                    .send_message(&commipcrs::Message::Response {
                        data: resp,
                        error: None,
                    })
                    .await;
            }
        });

        // The initial response to a stream call is usually empty or acknowledgement
        Ok(Value::Null)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let socket_path = "/run/user/1000/comm_ipc/comm_default.sock";
    let client = CommIPC::new(Some("rust-provider-service".to_string()), socket_path);

    println!("Connecting provider...");
    client.connect().await?;
    println!("Connected!");

    let channel = client.open("rust-demo-channel").await?;
    println!("Channel 'rust-demo-channel' opened!");

    // 1. Standard RPC
    channel.add_event("get_status", StatusHandler).await?;
    println!("Registered RPC: get_status");

    // 2. Group RPC
    channel
        .group("heavy-tasks")
        .provide("process", JobHandler)
        .await?;
    println!("Registered Group RPC: heavy-tasks.process");

    // 3. Streaming
    // Note: We need the client Arc to send stream chunks
    let stream_handler = LogStreamHandler {
        parent: client.clone(),
    };
    channel.add_stream("get_logs", stream_handler).await?;
    println!("Registered Stream: get_logs");

    // 4. Pub/Sub Owner
    channel.create_subscription("alerts").await?;
    println!("Created Subscription: alerts (Rust is now the owner/publisher)");

    println!("\nProvider is ready and waiting for calls...");
    println!("Press Ctrl+C to stop.");

    // Periodically publish alerts
    let mut count = 0;
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        count += 1;
        let _ = client
            .publish(
                "rust-demo-channel",
                "alerts",
                json!({
                    "msg": format!("Periodic Alert #{}", count),
                    "level": "info"
                }),
            )
            .await;
    }
}
