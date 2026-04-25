use crate::client::{CommIPC, Message};
use crate::comm_data::CommData;
use crate::error::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[async_trait]
pub trait EventHandler: Send + Sync {
    async fn handle(&self, cd: CommData) -> Result<Value>;
}

pub struct EventInfo {
    pub handler: Arc<dyn EventHandler>,
    pub is_stream: bool,
}

pub struct CommIPCChannel {
    pub name: String,
    pub parent: Arc<CommIPC>,
    pub events: RwLock<HashMap<String, EventInfo>>,
    pub listeners: RwLock<HashMap<String, Vec<Arc<dyn EventHandler>>>>,
}

impl CommIPCChannel {
    pub fn new(name: String, parent: Arc<CommIPC>) -> Self {
        Self {
            name,
            parent,
            events: RwLock::new(HashMap::new()),
            listeners: RwLock::new(HashMap::new()),
        }
    }

    pub async fn add_event<H>(&self, name: &str, handler: H) -> Result<()>
    where
        H: EventHandler + 'static,
    {
        self.add_event_internal(name, handler, false, None).await
    }

    pub async fn add_stream<H>(&self, name: &str, handler: H) -> Result<()>
    where
        H: EventHandler + 'static,
    {
        self.add_event_internal(name, handler, true, None).await
    }

    async fn add_event_internal<H>(
        &self,
        name: &str,
        handler: H,
        is_stream: bool,
        group: Option<String>,
    ) -> Result<()>
    where
        H: EventHandler + 'static,
    {
        let mut events = self.events.write().await;
        events.insert(
            name.to_string(),
            EventInfo {
                handler: Arc::new(handler),
                is_stream,
            },
        );

        let is_group = group.is_some();

        // Register with server
        self.parent
            .send_message(&Message::Register {
                request_id: None,
                client_id: "".to_string(),
                channel: self.name.clone(),
                is_provider: true,
                event: Some(name.to_string()),
                is_stream: Some(is_stream),
                is_group: Some(is_group),
                param_schema: None,
                return_schema: None,
                proof: None,
                challenge_id: None,
            })
            .await?;

        Ok(())
    }

    pub async fn event(&self, name: &str, data: Value) -> Result<CommData> {
        self.parent.call(&self.name, name, data).await
    }

    pub async fn subscribe<H>(&self, sub_name: &str, handler: H) -> Result<()>
    where
        H: EventHandler + 'static,
    {
        let mut listeners = self.listeners.write().await;
        let event_name = format!("subscription.{}.data", sub_name);
        listeners
            .entry(event_name)
            .or_insert_with(Vec::new)
            .push(Arc::new(handler));

        self.parent.subscribe(&self.name, sub_name).await
    }

    pub async fn create_subscription(&self, sub_name: &str) -> Result<()> {
        self.parent.add_subscription(&self.name, sub_name).await
    }

    pub(crate) async fn handle_call(&self, cd: CommData) {
        let events = self.events.read().await;
        if let Some(info) = events.get(&cd.event) {
            let rid = cd.request_id.clone();
            let target_id = cd.sender_id.clone();
            let is_stream = info.is_stream;

            match info.handler.handle(cd).await {
                Ok(res) => {
                    let mut resp_cd = CommData::new();
                    resp_cd.channel = self.name.clone();
                    resp_cd.event = "".to_string();
                    resp_cd.data = res;
                    resp_cd.request_id = rid;
                    resp_cd.target_id = Some(target_id);
                    resp_cd.is_stream = is_stream;
                    resp_cd.is_final = !is_stream;

                    let _ = self
                        .parent
                        .send_message(&Message::Response {
                            data: resp_cd,
                            error: None,
                        })
                        .await;
                }
                Err(e) => {
                    let _ = self
                        .parent
                        .send_message(&Message::Error {
                            request_id: rid,
                            message: e.to_string(),
                        })
                        .await;
                }
            }
        }
    }

    pub(crate) async fn handle_receive(&self, cd: CommData) {
        let listeners = self.listeners.read().await;
        if let Some(handlers) = listeners.get(&cd.event) {
            for handler in handlers {
                let _ = handler.handle(cd.clone()).await;
            }
        }
    }

    pub fn group(&self, name: &str) -> CommIPCGroup<'_> {
        CommIPCGroup {
            channel: self,
            name: name.to_string(),
        }
    }
}

pub struct CommIPCGroup<'a> {
    channel: &'a CommIPCChannel,
    name: String,
}

impl<'a> CommIPCGroup<'a> {
    fn get_event_name(&self, event: &str) -> String {
        format!("{}.{}", self.name, event)
    }

    pub async fn provide<H>(&self, event: &str, handler: H) -> Result<()>
    where
        H: EventHandler + 'static,
    {
        let full_name = self.get_event_name(event);
        self.channel
            .add_event_internal(&full_name, handler, false, Some(self.name.clone()))
            .await
    }

    pub async fn event(&self, event: &str, data: Value) -> Result<CommData> {
        let full_name = self.get_event_name(event);
        self.channel.event(&full_name, data).await
    }
}
