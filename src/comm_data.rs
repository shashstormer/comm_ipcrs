use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CommData {
    #[serde(default, deserialize_with = "deserialize_null_as_default")]
    pub sender_id: String,
    #[serde(default, deserialize_with = "deserialize_null_as_default")]
    pub server_id: String,
    #[serde(default, deserialize_with = "deserialize_null_as_default")]
    pub channel: String,
    #[serde(default, deserialize_with = "deserialize_null_as_default")]
    pub event: String,
    #[serde(default)]
    pub data: Value,
    #[serde(default = "default_timestamp")]
    pub timestamp: u64,
    #[serde(default)]
    pub metadata: HashMap<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_id: Option<String>,
    #[serde(default)]
    pub path: Vec<String>,
    #[serde(default)]
    pub is_stream: bool,
    #[serde(default)]
    pub is_final: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin_server_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_name: Option<String>,
    #[serde(default = "default_mode")]
    pub mode: String,
}

fn deserialize_null_as_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Default + serde::Deserialize<'de>,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

fn default_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn default_mode() -> String {
    "client".to_string()
}

impl Default for CommData {
    fn default() -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            sender_id: String::new(),
            server_id: String::new(),
            channel: String::new(),
            event: String::new(),
            data: Value::Null,
            timestamp,
            metadata: HashMap::new(),
            request_id: None,
            target_id: None,
            path: Vec::new(),
            is_stream: false,
            is_final: false,
            signature: None,
            origin_server_id: None,
            sub_name: None,
            mode: "client".to_string(),
        }
    }
}

impl CommData {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn to_value(&self) -> Result<Value, serde_json::Error> {
        serde_json::to_value(self)
    }

    pub fn from_value(v: Value) -> Result<Self, serde_json::Error> {
        serde_json::from_value(v)
    }
}
