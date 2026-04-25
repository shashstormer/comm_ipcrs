use pbkdf2::{
    hmac::{Hmac, KeyInit, Mac},
    pbkdf2,
    sha2::{Digest, Sha256},
};
use serde_json::Value;
use std::collections::HashMap;

pub fn hash_secret(secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn derive_key(secret: &str, salt: &[u8]) -> Vec<u8> {
    let mut key = [0u8; 32];
    pbkdf2::<Hmac<Sha256>>(secret.as_bytes(), salt, 100_000, &mut key)
        .expect("PBKDF2 derivation failed");
    key.to_vec()
}

pub fn compute_signature(key: &[u8], msg: &HashMap<String, Value>) -> String {
    let immutable_keys = [
        "sender_id",
        "channel",
        "event",
        "data",
        "timestamp",
        "request_id",
        "target_id",
        "is_stream",
        "is_final",
        "origin_server_id",
        "challenge",
        "sub_name",
    ];

    let mut parts = Vec::new();
    let mut sorted_keys = immutable_keys.to_vec();
    sorted_keys.sort();

    for k in sorted_keys {
        if let Some(val) = msg.get(k).filter(|v| !v.is_null()) {
            let serialized_val = serialize_json_canonical(val);
            parts.append(&mut format!("{}:{}", k, serialized_val).into_bytes());
            parts.push(b'|');
        }
    }

    if !parts.is_empty() {
        parts.pop(); // Remove trailing '|'
    }

    let mut mac =
        <Hmac<Sha256> as KeyInit>::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(&parts);
    hex::encode(mac.finalize().into_bytes())
}

fn serialize_json_canonical(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("\"{}\"", s),
        Value::Array(arr) => {
            let mut s = String::from("[");
            for (i, v) in arr.iter().enumerate() {
                if i > 0 {
                    s.push(',');
                }
                s.push_str(&serialize_json_canonical(v));
            }
            s.push(']');
            s
        }
        Value::Object(obj) => {
            let mut s = String::from("{");
            let mut keys: Vec<_> = obj.keys().collect();
            keys.sort();
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    s.push(',');
                }
                s.push_str(&format!("\"{}\":{}", k, serialize_json_canonical(&obj[*k])));
            }
            s.push('}');
            s
        }
    }
}

pub fn verify_signature(key: &[u8], msg: &HashMap<String, Value>, signature: &str) -> bool {
    let expected = compute_signature(key, msg);
    expected == signature
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_canonical_json() {
        let msg = json!({
            "z": 1,
            "a": 2,
            "m": [3, 2, 1],
            "x": {"b": 1, "a": 2}
        });
        let serialized = serialize_json_canonical(&msg);
        assert_eq!(
            serialized,
            "{\"a\":2,\"m\":[3,2,1],\"x\":{\"a\":2,\"b\":1},\"z\":1}"
        );
    }
}
