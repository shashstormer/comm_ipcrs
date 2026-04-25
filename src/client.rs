use crate::channel::CommIPCChannel;
use crate::comm_data::CommData;
use crate::error::Result;
use crate::security;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::{Mutex, RwLock, mpsc, oneshot};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Message {
    #[serde(rename = "identify")]
    Identify { client_id: String, mode: String },
    #[serde(rename = "identified")]
    Identified {
        client_id: String,
        server_id: String,
    },
    #[serde(rename = "conn_challenge")]
    ConnChallenge { challenge: String },
    #[serde(rename = "conn_proof")]
    ConnProof { proof: String },
    #[serde(rename = "register")]
    Register {
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
        client_id: String,
        channel: String,
        #[serde(default)]
        is_provider: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        event: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        is_stream: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        is_group: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        param_schema: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        return_schema: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        proof: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        challenge_id: Option<String>,
    },
    #[serde(rename = "call")]
    Call {
        #[serde(flatten)]
        data: CommData,
    },
    #[serde(rename = "response")]
    Response {
        #[serde(flatten)]
        data: CommData,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    #[serde(rename = "broadcast")]
    Broadcast {
        #[serde(flatten)]
        data: CommData,
    },
    #[serde(rename = "send")]
    Send {
        #[serde(flatten)]
        data: CommData,
    },
    #[serde(rename = "receive")]
    Receive {
        #[serde(flatten)]
        data: CommData,
    },
    #[serde(rename = "publish")]
    Publish {
        channel: String,
        sub_name: String,
        data: Value,
    },
    #[serde(rename = "subscribe")]
    Subscribe {
        channel: String,
        sub_name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
    },
    #[serde(rename = "unsubscribe")]
    Unsubscribe {
        channel: String,
        sub_name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
    },
    #[serde(rename = "add_subscription")]
    AddSubscription {
        channel: String,
        sub_name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        return_schema: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
    },
    #[serde(rename = "remove_subscription")]
    RemoveSubscription {
        channel: String,
        sub_name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
    },
    #[serde(rename = "error")]
    Error {
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
        message: String,
    },
    #[serde(rename = "ping")]
    Ping,
    #[serde(rename = "pong")]
    Pong,
    #[serde(rename = "auth_challenge")]
    AuthChallenge {
        channel: String,
        challenge: String,
        challenge_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
    },
    #[serde(rename = "channel_sync")]
    ChannelSync {
        channel: String,
        events: HashMap<String, Value>,
        subscriptions: HashMap<String, Value>,
    },
    #[serde(rename = "metadata_update")]
    MetadataUpdate {
        channel: String,
        name: String,
        stype: String,
        owner: Option<String>,
        param_schema: Option<Value>,
        return_schema: Option<Value>,
    },
}

pub struct CommIPC {
    client_id: String,
    socket_path: PathBuf,
    connection_secret: Option<String>,
    server_id: RwLock<Option<String>>,
    channels: RwLock<HashMap<String, Arc<CommIPCChannel>>>,
    pending_calls: Mutex<HashMap<String, oneshot::Sender<Message>>>,
    active_streams: Mutex<HashMap<String, mpsc::Sender<CommData>>>,
    writer: Mutex<Option<tokio::io::WriteHalf<UnixStream>>>,
}

impl CommIPC {
    pub fn new(client_id: Option<String>, socket_path: &str) -> Arc<Self> {
        let client_id =
            client_id.unwrap_or_else(|| format!("cli-{}", &Uuid::new_v4().to_string()[..8]));
        Arc::new(Self {
            client_id,
            socket_path: PathBuf::from(socket_path),
            connection_secret: None,
            server_id: RwLock::new(None),
            channels: RwLock::new(HashMap::new()),
            pending_calls: Mutex::new(HashMap::new()),
            active_streams: Mutex::new(HashMap::new()),
            writer: Mutex::new(None),
        })
    }

    pub fn set_connection_secret(&mut self, secret: String) {
        self.connection_secret = Some(secret);
    }

    pub async fn connect(self: &Arc<Self>) -> Result<()> {
        let stream = UnixStream::connect(&self.socket_path).await?;
        let (mut reader, mut writer) = tokio::io::split(stream);

        // Identification
        let ident = Message::Identify {
            client_id: self.client_id.clone(),
            mode: "client".to_string(),
        };
        self.send_message_to_writer(&mut writer, &ident).await?;

        // Handle Handshake
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await?;
        let length = u32::from_be_bytes(len_buf) as usize;
        let mut data = vec![0u8; length];
        reader.read_exact(&mut data).await?;

        let mut resp: Message = rmp_serde::from_slice(&data)?;

        if let Message::ConnChallenge { challenge } = resp {
            let secret = self.connection_secret.as_ref().ok_or_else(|| {
                crate::error::Error::Auth(
                    "Server required connection secret but none provided".into(),
                )
            })?;

            let secret_hash = security::hash_secret(secret);
            let mut msg_dict = HashMap::new();
            msg_dict.insert("challenge".to_string(), Value::String(challenge));

            let proof = security::compute_signature(secret_hash.as_bytes(), &msg_dict);
            let proof_msg = Message::ConnProof { proof };
            self.send_message_to_writer(&mut writer, &proof_msg).await?;

            reader.read_exact(&mut len_buf).await?;
            let length = u32::from_be_bytes(len_buf) as usize;
            data.resize(length, 0);
            reader.read_exact(&mut data).await?;
            resp = rmp_serde::from_slice(&data)?;
        }

        if let Message::Identified {
            client_id: _,
            server_id,
        } = resp
        {
            *self.server_id.write().await = Some(server_id);
            // In a real implementation we'd update client_id if the server changed it
        } else if let Message::Error { message, .. } = resp {
            return Err(crate::error::Error::Connection(message));
        } else {
            return Err(crate::error::Error::Connection(
                "Unexpected message during identification".into(),
            ));
        }

        *self.writer.lock().await = Some(writer);

        // Start listen loop
        let self_clone = self.clone();
        tokio::spawn(async move {
            if let Err(e) = self_clone.listen_loop(reader).await {
                log::error!("Listen loop error: {}", e);
            }
        });

        // Start heartbeat loop
        let self_clone = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                if let Err(e) = self_clone.send_message(&Message::Ping).await {
                    log::error!("Heartbeat error: {}", e);
                    break;
                }
            }
        });

        Ok(())
    }

    async fn send_message_to_writer<W: AsyncWriteExt + Unpin>(
        &self,
        writer: &mut W,
        msg: &Message,
    ) -> Result<()> {
        let data = rmp_serde::to_vec_named(msg)?;
        let length = (data.len() as u32).to_be_bytes();
        writer.write_all(&length).await?;
        writer.write_all(&data).await?;
        writer.flush().await?;
        Ok(())
    }

    pub async fn send_message(&self, msg: &Message) -> Result<()> {
        let mut writer_lock = self.writer.lock().await;
        if let Some(ref mut writer) = *writer_lock {
            self.send_message_to_writer(writer, msg).await?;
            Ok(())
        } else {
            Err(crate::error::Error::Connection("Not connected".into()))
        }
    }

    async fn listen_loop<R: AsyncReadExt + Unpin>(self: Arc<Self>, mut reader: R) -> Result<()> {
        let mut len_buf = [0u8; 4];
        while (reader.read_exact(&mut len_buf).await).is_ok() {
            let length = u32::from_be_bytes(len_buf) as usize;
            let mut data = vec![0u8; length];
            reader.read_exact(&mut data).await?;

            let msg: Message = match rmp_serde::from_slice(&data) {
                Ok(m) => {
                    // println!("DEBUG: Received message: {:?}", m);
                    m
                }
                Err(e) => {
                    log::error!("Failed to deserialize message: {}", e);
                    // println!("DEBUG: Deserialization error: {}, raw data: {:?}", e, data);
                    continue;
                }
            };

            let self_clone = self.clone();
            tokio::spawn(async move {
                if let Err(e) = self_clone.handle_message(msg).await {
                    log::error!("Error handling message: {}", e);
                }
            });
        }
        Ok(())
    }

    async fn handle_message(&self, msg: Message) -> Result<()> {
        match msg {
            Message::Response { ref data, .. } => {
                if let Some(ref rid) = data.request_id {
                    if data.is_stream {
                        let mut streams = self.active_streams.lock().await;
                        if let Some(tx) = streams.get(rid) {
                            if tx.send(data.clone()).await.is_err() || data.is_final {
                                streams.remove(rid);
                            }
                            return Ok(());
                        }
                    }

                    // Check if it's a pending call
                    let mut pending = self.pending_calls.lock().await;
                    if let Some(tx) = pending.remove(rid) {
                        let _ = tx.send(msg);
                    }
                }
            }
            Message::Error {
                request_id: Some(ref rid),
                ..
            } => {
                let mut pending = self.pending_calls.lock().await;
                if let Some(tx) = pending.remove(rid) {
                    let _ = tx.send(msg);
                }
            }
            Message::Call { data: cd } => {
                // Dispatch to channel
                let channels = self.channels.read().await;
                if let Some(chan) = channels.get(&cd.channel) {
                    let chan_clone = chan.clone();
                    tokio::spawn(async move {
                        chan_clone.handle_call(cd).await;
                    });
                }
            }
            Message::Broadcast { data: cd }
            | Message::Send { data: cd }
            | Message::Receive { data: cd } => {
                let channels = self.channels.read().await;
                if let Some(chan) = channels.get(&cd.channel) {
                    let chan_clone = chan.clone();
                    tokio::spawn(async move {
                        chan_clone.handle_receive(cd).await;
                    });
                }
            }
            Message::Ping => {
                self.send_message(&Message::Pong).await?;
            }
            _ => {}
        }
        Ok(())
    }

    pub async fn open(self: &Arc<Self>, channel_name: &str) -> Result<Arc<CommIPCChannel>> {
        let mut channels = self.channels.write().await;
        if let Some(chan) = channels.get(channel_name) {
            return Ok(chan.clone());
        }

        let chan = CommIPCChannel::new(channel_name.to_string(), self.clone());
        let arc_chan = Arc::new(chan);

        let rid = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();

        {
            let mut pending = self.pending_calls.lock().await;
            pending.insert(rid.clone(), tx);
        }

        self.send_message(&Message::Register {
            request_id: Some(rid),
            client_id: self.client_id.clone(),
            channel: channel_name.to_string(),
            is_provider: false,
            event: None,
            is_stream: None,
            is_group: None,
            param_schema: None,
            return_schema: None,
            proof: None,
            challenge_id: None,
        })
        .await?;

        match rx.await {
            Ok(Message::Response { data: _, error: _ }) => {
                channels.insert(channel_name.to_string(), arc_chan.clone());
                Ok(arc_chan)
            }
            Ok(Message::Error { message, .. }) => Err(crate::error::Error::Connection(message)),
            _ => Err(crate::error::Error::Connection(
                "Failed to register channel".into(),
            )),
        }
    }

    pub async fn call(&self, channel: &str, event: &str, data: Value) -> Result<CommData> {
        let rid = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();

        {
            let mut pending = self.pending_calls.lock().await;
            pending.insert(rid.clone(), tx);
        }

        let mut cd = CommData::new();
        cd.channel = channel.to_string();
        cd.event = event.to_string();
        cd.data = data;
        cd.request_id = Some(rid);
        cd.sender_id = self.client_id.clone();
        if let Some(ref sid) = *self.server_id.read().await {
            cd.server_id = sid.clone();
        }

        self.send_message(&Message::Call { data: cd }).await?;

        match rx.await {
            Ok(Message::Response {
                data: res_cd,
                error,
            }) => {
                if let Some(err) = error {
                    Err(crate::error::Error::Call(err))
                } else {
                    Ok(res_cd)
                }
            }
            Ok(Message::Error { message, .. }) => Err(crate::error::Error::Call(message)),
            _ => Err(crate::error::Error::Connection("Call failed".into())),
        }
    }

    pub async fn close(&self) -> Result<()> {
        let mut writer_lock = self.writer.lock().await;
        if let Some(mut writer) = writer_lock.take() {
            writer.shutdown().await?;
        }
        Ok(())
    }

    pub async fn publish(&self, channel: &str, sub_name: &str, data: Value) -> Result<()> {
        self.send_message(&Message::Publish {
            channel: channel.to_string(),
            sub_name: sub_name.to_string(),
            data,
        })
        .await
    }

    pub async fn add_subscription(&self, channel: &str, sub_name: &str) -> Result<()> {
        let rid = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();

        {
            let mut pending = self.pending_calls.lock().await;
            pending.insert(rid.clone(), tx);
        }

        self.send_message(&Message::AddSubscription {
            request_id: Some(rid),
            channel: channel.to_string(),
            sub_name: sub_name.to_string(),
            return_schema: None,
        })
        .await?;

        match rx.await {
            Ok(Message::Response { data: _, error: _ }) => Ok(()),
            Ok(Message::Error { message, .. }) => Err(crate::error::Error::Call(message)),
            _ => Err(crate::error::Error::Connection(
                "Add subscription failed".into(),
            )),
        }
    }

    pub async fn subscribe(&self, channel: &str, sub_name: &str) -> Result<()> {
        let rid = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();

        {
            let mut pending = self.pending_calls.lock().await;
            pending.insert(rid.clone(), tx);
        }

        self.send_message(&Message::Subscribe {
            channel: channel.to_string(),
            sub_name: sub_name.to_string(),
            request_id: Some(rid),
        })
        .await?;

        match rx.await {
            Ok(Message::Response { data: _, error: _ }) => Ok(()),
            Ok(Message::Error { message, .. }) => Err(crate::error::Error::Call(message)),
            _ => Err(crate::error::Error::Connection("Subscribe failed".into())),
        }
    }

    pub async fn stream(
        &self,
        channel: &str,
        event: &str,
        data: Value,
    ) -> Result<mpsc::Receiver<CommData>> {
        let rid = Uuid::new_v4().to_string();
        let (tx, rx) = mpsc::channel(100);

        {
            let mut streams = self.active_streams.lock().await;
            streams.insert(rid.clone(), tx);
        }

        let mut cd = CommData::new();
        cd.channel = channel.to_string();
        cd.event = event.to_string();
        cd.data = data;
        cd.request_id = Some(rid);
        cd.sender_id = self.client_id.clone();
        if let Some(ref sid) = *self.server_id.read().await {
            cd.server_id = sid.clone();
        }

        self.send_message(&Message::Call { data: cd }).await?;
        Ok(rx)
    }
}
