use crate::config::{Config, WebhookConfig};
use crate::db::{self, Db};
use anyhow::Result;
use hmac::{Hmac, Mac};
use reqwest::Client;
use sha2::Sha256;
use std::time::Duration;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct WebhookSender {
    client: Client,
    config: WebhookConfig,
}

impl WebhookSender {
    pub fn new(config: &Config) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.webhook.timeout_seconds))
            .build()?;
        Ok(Self {
            client,
            config: config.webhook.clone(),
        })
    }

    pub async fn process_outbox(&self, db: &Db) -> Result<()> {
        let items = db::due_webhooks(db, 20)?;
        for (id, callback, event_type, payload, _attempts) in items {
            let subscribed: Vec<String> =
                serde_json::from_str(&callback.event_types).unwrap_or_default();
            if !subscribed.is_empty() && !subscribed.iter().any(|value| value == &event_type) {
                db::mark_webhook_sent(db, &id)?;
                continue;
            }
            let body = payload.clone();
            let mut request = self
                .client
                .post(&callback.url)
                .header("content-type", "application/json")
                .header("x-olaf-event", &event_type)
                .body(body.clone());

            if callback.auth_type == "bearer" || callback.auth_type == "both" {
                if let Some(token) = &callback.token {
                    request = request.header("authorization", format!("Bearer {}", token));
                }
            }

            let secret = callback
                .signing_secret
                .as_deref()
                .filter(|value| !value.is_empty())
                .unwrap_or(&self.config.signing_secret);
            if !secret.is_empty()
                && (callback.auth_type == "hmac"
                    || callback.auth_type == "both"
                    || callback.auth_type == "bearer")
            {
                let signature = sign(secret, body.as_bytes())?;
                request = request.header("x-olaf-signature", format!("sha256={}", signature));
            }

            match request.send().await {
                Ok(response) if response.status().is_success() => {
                    db::mark_webhook_sent(db, &id)?;
                }
                Ok(response) => {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    db::mark_webhook_failed(
                        db,
                        &id,
                        &format!("webhook returned {}: {}", status, text),
                        self.config.max_attempts as i64,
                    )?;
                }
                Err(error) => {
                    db::mark_webhook_failed(
                        db,
                        &id,
                        &error.to_string(),
                        self.config.max_attempts as i64,
                    )?;
                }
            }
        }
        Ok(())
    }
}

fn sign(secret: &str, body: &[u8]) -> Result<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())?;
    mac.update(body);
    Ok(hex::encode(mac.finalize().into_bytes()))
}
