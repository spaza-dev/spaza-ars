use crate::config::OlafConfig;
use anyhow::{anyhow, Result};
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;

#[derive(Clone)]
pub struct OlafEngine {
    client: Client,
    base_url: String,
}

impl OlafEngine {
    pub fn new(config: &OlafConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_seconds))
            .build()?;
        Ok(Self {
            client,
            base_url: config.rest_url.trim_end_matches('/').to_string(),
        })
    }

    pub async fn health(&self) -> bool {
        let url = format!("{}/api/healthz", self.base_url);
        match self.client.get(url).send().await {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    pub async fn store(&self, identifier: &str, bytes: Vec<u8>, force: bool) -> Result<Value> {
        let mut url = format!(
            "{}/api/store?identifier={}",
            self.base_url,
            urlencoding(identifier)
        );
        if force {
            url.push_str("&force=true");
        }
        let response = self
            .client
            .post(url)
            .header("content-type", "application/octet-stream")
            .body(bytes)
            .send()
            .await?;
        let status = response.status();
        let text = response.text().await?;
        if !status.is_success() {
            return Err(anyhow!("Olaf store failed ({}): {}", status, text));
        }
        Ok(serde_json::from_str(&text).unwrap_or_else(|_| serde_json::json!({ "raw": text })))
    }

    pub async fn query(&self, bytes: Vec<u8>, fragmented: bool) -> Result<Vec<Value>> {
        let url = if fragmented {
            format!("{}/api/query?fragmented=true", self.base_url)
        } else {
            format!("{}/api/query", self.base_url)
        };
        let response = self
            .client
            .post(url)
            .header("content-type", "application/octet-stream")
            .body(bytes)
            .send()
            .await?;
        let status = response.status();
        let text = response.text().await?;
        if !status.is_success() {
            return Err(anyhow!("Olaf query failed ({}): {}", status, text));
        }
        let payload: Value = serde_json::from_str(&text)?;
        let mut matches = Vec::new();
        collect_matches(&payload, &mut matches);
        Ok(matches)
    }
}

fn collect_matches(value: &Value, out: &mut Vec<Value>) {
    match value {
        Value::Object(map) => {
            if let Some(Value::Array(items)) = map.get("matches") {
                for item in items {
                    out.push(item.clone());
                }
            }
            for (key, child) in map {
                if key != "matches" {
                    collect_matches(child, out);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_matches(item, out);
            }
        }
        _ => {}
    }
}

fn urlencoding(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{:02X}", byte)),
        }
    }
    out
}
