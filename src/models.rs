use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Callback {
    pub id: String,
    pub name: String,
    pub url: String,
    pub auth_type: String,
    pub token: Option<String>,
    pub signing_secret: Option<String>,
    pub event_types: String,
    pub enabled: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stream {
    pub id: String,
    pub name: String,
    pub url: String,
    pub source_type: String,
    pub external_id: Option<String>,
    pub catalogs: Vec<String>,
    pub enabled: i64,
    pub evidence_enabled: i64,
    pub evidence_pre_seconds: i64,
    pub evidence_post_seconds: i64,
    pub callback_id: Option<String>,
    pub chunk_seconds: i64,
    pub overlap_seconds: i64,
    pub aggregation_window_seconds: Option<i64>,
    pub status: String,
    pub last_error: Option<String>,
    pub last_audio_at: Option<String>,
    pub last_match_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateStream {
    pub name: String,
    pub url: String,
    #[serde(default = "default_source_type")]
    pub source_type: String,
    pub external_id: Option<String>,
    #[serde(default)]
    pub catalogs: Vec<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub evidence_enabled: bool,
    pub evidence_pre_seconds: Option<i64>,
    pub evidence_post_seconds: Option<i64>,
    pub callback_id: Option<String>,
    pub chunk_seconds: Option<i64>,
    pub overlap_seconds: Option<i64>,
    pub aggregation_window_seconds: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateStream {
    pub name: Option<String>,
    pub url: Option<String>,
    pub source_type: Option<String>,
    pub external_id: Option<String>,
    pub catalogs: Option<Vec<String>>,
    pub enabled: Option<bool>,
    pub evidence_enabled: Option<bool>,
    pub evidence_pre_seconds: Option<i64>,
    pub evidence_post_seconds: Option<i64>,
    pub callback_id: Option<String>,
    pub chunk_seconds: Option<i64>,
    pub overlap_seconds: Option<i64>,
    pub aggregation_window_seconds: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct CreateCallback {
    pub name: String,
    pub url: String,
    #[serde(default = "default_bearer")]
    pub auth_type: String,
    pub token: Option<String>,
    pub signing_secret: Option<String>,
    pub event_types: Option<Vec<String>>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct UpdateCallback {
    pub name: Option<String>,
    pub url: Option<String>,
    pub auth_type: Option<String>,
    pub token: Option<String>,
    pub signing_secret: Option<String>,
    pub event_types: Option<Vec<String>>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct IndexItemJson {
    pub catalog: Option<String>,
    pub external_id: Option<String>,
    pub name: String,
    #[serde(rename = "type")]
    pub item_type: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub url: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CatalogItem {
    pub id: String,
    pub catalog: String,
    pub external_id: Option<String>,
    pub name: String,
    #[serde(rename = "type")]
    pub item_type: String,
    pub metadata_json: String,
    pub fingerprint_id: Option<String>,
    pub status: String,
    pub indexed_at: Option<String>,
    pub error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Detection {
    pub id: String,
    pub stream_id: String,
    pub catalog_item_id: String,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub confidence: Option<f64>,
    pub match_count: Option<i64>,
    pub evidence_id: Option<String>,
    pub aggregated_at: Option<String>,
    pub raw_json: String,
    pub created_at: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DetectionAggregate {
    pub id: String,
    pub stream_id: String,
    pub window_start: String,
    pub window_end: String,
    pub match_count: i64,
    pub payload_json: String,
    pub status: String,
    pub created_at: String,
}

#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub data: T,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<serde_json::Value>,
}

impl<T> ApiResponse<T> {
    pub fn ok(data: T) -> Self {
        Self { data, meta: None }
    }
}

#[derive(Debug, Serialize)]
pub struct JobAccepted {
    pub job_id: String,
    pub status: String,
    pub status_url: String,
}

#[derive(Debug, Serialize)]
pub struct Health {
    pub ok: bool,
    pub status: String,
    pub version: String,
    pub streams: i64,
    pub pending_jobs: i64,
    pub pending_webhooks: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequest {
    pub url: Option<String>,
    pub audio_url: Option<String>,
    pub catalog: Option<Vec<String>>,
    pub options: Option<serde_json::Value>,
}

fn default_source_type() -> String {
    "radio".into()
}

fn default_true() -> bool {
    true
}

fn default_bearer() -> String {
    "bearer".into()
}
