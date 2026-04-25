use async_trait::async_trait;
use commipcrs::{CommData, CommIPC, EventHandler, Result};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::mpsc;

struct EchoHandler;
#[async_trait]
impl EventHandler for EchoHandler {
    async fn handle(&self, cd: CommData) -> Result<Value> {
        Ok(cd.data)
    }
}

struct StreamHandler {
    client: Arc<CommIPC>,
}
#[async_trait]
impl EventHandler for StreamHandler {
    async fn handle(&self, cd: CommData) -> Result<Value> {
        let rid = cd.request_id.clone();
        let target_id = cd.sender_id.clone();
        let channel = cd.channel.clone();
        let event = cd.event.clone();
        let client = self.client.clone();

        tokio::spawn(async move {
            for i in 0..3 {
                let mut resp = CommData::new();
                resp.channel = channel.clone();
                resp.event = event.clone();
                resp.data = json!(i);
                resp.request_id = rid.clone();
                resp.target_id = Some(target_id.clone());
                resp.is_stream = true;
                resp.is_final = i == 2;

                let _ = client
                    .send_message(&commipcrs::Message::Response {
                        data: resp,
                        error: None,
                    })
                    .await;
            }
        });
        Ok(Value::Null)
    }
}

#[tokio::test]
async fn test_full_integration() -> Result<()> {
    let socket_path = std::env::var("COMM_IPC_SOCKET")
        .unwrap_or_else(|_| "/run/user/1000/comm_ipc/comm_default.sock".to_string());

    // Create two clients: one provider, one caller
    let provider = CommIPC::new(Some("test-provider".to_string()), &socket_path);
    let caller = CommIPC::new(Some("test-caller".to_string()), &socket_path);

    provider.connect().await?;
    caller.connect().await?;

    let p_chan = provider.open("test-channel").await?;
    let c_chan = caller.open("test-channel").await?;

    // 1. Test RPC (Echo)
    p_chan.add_event("echo", EchoHandler).await?;
    // Wait a bit for server to sync metadata
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let res = c_chan.event("echo", json!({"hello": "world"})).await?;
    assert_eq!(res.data, json!({"hello": "world"}));

    // 2. Test Streaming
    p_chan
        .add_stream(
            "stream",
            StreamHandler {
                client: provider.clone(),
            },
        )
        .await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let mut stream = caller.stream("test-channel", "stream", json!({})).await?;
    let mut count = 0;
    while let Some(msg) = stream.recv().await {
        if msg.data != Value::Null {
            assert_eq!(msg.data, json!(count));
            count += 1;
        }
        if msg.is_final {
            break;
        }
    }
    assert_eq!(count, 3);

    // 3. Test Pub/Sub
    let (tx, mut rx) = mpsc::channel(10);
    struct SubHandler(mpsc::Sender<Value>);
    #[async_trait]
    impl EventHandler for SubHandler {
        async fn handle(&self, cd: CommData) -> Result<Value> {
            let _ = self.0.send(cd.data).await;
            Ok(Value::Null)
        }
    }

    c_chan.subscribe("test-sub", SubHandler(tx)).await?;
    p_chan.create_subscription("test-sub").await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    provider
        .publish("test-channel", "test-sub", json!({"alert": "red"}))
        .await?;

    let received = tokio::time::timeout(tokio::time::Duration::from_secs(2), rx.recv())
        .await
        .unwrap();
    assert_eq!(received.unwrap(), json!({"alert": "red"}));

    // 4. Test Groups
    p_chan
        .group("test-group")
        .provide("work", EchoHandler)
        .await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let res = c_chan.group("test-group").event("work", json!(42)).await?;
    assert_eq!(res.data, json!(42));

    Ok(())
}
