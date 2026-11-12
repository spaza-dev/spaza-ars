use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
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

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateStream {
    #[schema(example = "Radio One")]
    pub name: String,
    #[schema(example = "https://example.com/live.mp3")]
    pub url: String,
    #[serde(default = "default_source_type")]
    pub source_type: String,
    pub external_id: Option<String>,
    #[serde(default)]
    #[schema(example = json!(["private"]))]
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

#[derive(Debug, Deserialize, ToSchema)]
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

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateCallback {
    #[schema(example = "console-default")]
    pub name: String,
    #[schema(example = "https://console.example.com/api/callback/secret")]
    pub url: String,
    #[serde(default = "default_bearer")]
    pub auth_type: String,
    pub token: Option<String>,
    pub signing_secret: Option<String>,
    pub event_types: Option<Vec<String>>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateCallback {
    pub name: Option<String>,
    pub url: Option<String>,
    pub auth_type: Option<String>,
    pub token: Option<String>,
    pub signing_secret: Option<String>,
    pub event_types: Option<Vec<String>>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct IndexItemJson {
    #[schema(example = "private")]
    pub catalog: Option<String>,
    #[schema(example = "12345")]
    pub external_id: Option<String>,
    #[schema(example = "Song A")]
    pub name: String,
    #[serde(rename = "type")]
    #[schema(example = "song")]
    pub item_type: Option<String>,
    pub metadata: Option<serde_json::Value>,
    #[schema(example = "https://example.com/song.mp3")]
    pub url: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
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

#[derive(Debug, Serialize, Deserialize, ToSchema)]
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

#[derive(Debug, Serialize, Deserialize, ToSchema)]
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

#[derive(Debug, Serialize, ToSchema)]
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

#[derive(Debug, Serialize, ToSchema)]
pub struct JobAccepted {
    pub job_id: String,
    pub status: String,
    pub status_url: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Health {
    pub ok: bool,
    pub status: String,
    pub version: String,
    pub streams: i64,
    pub pending_jobs: i64,
    pub pending_webhooks: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
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

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    #[schema(example = "invalid bearer token")]
    pub error: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DeleteResponse {
    #[schema(example = true)]
    pub deleted: bool,
    pub note: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct StatusResponse {
    #[schema(example = "running")]
    pub status: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct JobStatus {
    pub id: String,
    pub kind: String,
    pub status: String,
    pub attempts: i64,
    pub error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct QueryResponse {
    pub matches: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct TestWebhookRequest {
    #[schema(example = "cb_123")]
    pub callback_id: String,
}

#[derive(Debug, ToSchema)]
pub struct IndexItemMultipart {
    #[schema(format = Binary, value_type = String)]
    pub file: Option<String>,
    #[schema(example = "https://example.com/song.mp3")]
    pub url: Option<String>,
    #[schema(example = "Song A")]
    pub name: String,
    #[schema(example = "private")]
    pub catalog: Option<String>,
    #[schema(example = "12345")]
    pub external_id: Option<String>,
    #[schema(example = "song")]
    pub r#type: Option<String>,
    #[schema(example = "{\"artist\":\"Artist A\"}")]
    pub metadata: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct StreamResponse {
    pub data: Stream,
}
#[derive(Debug, Serialize, ToSchema)]
pub struct StreamListResponse {
    pub data: Vec<Stream>,
}
#[derive(Debug, Serialize, ToSchema)]
pub struct CallbackResponse {
    pub data: Callback,
}
#[derive(Debug, Serialize, ToSchema)]
pub struct CallbackListResponse {
    pub data: Vec<Callback>,
}
#[derive(Debug, Serialize, ToSchema)]
pub struct CatalogItemResponse {
    pub data: CatalogItem,
}
#[derive(Debug, Serialize, ToSchema)]
pub struct CatalogItemListResponse {
    pub data: Vec<CatalogItem>,
}
#[derive(Debug, Serialize, ToSchema)]
pub struct DetectionListResponse {
    pub data: Vec<Detection>,
}
#[derive(Debug, Serialize, ToSchema)]
pub struct AggregateListResponse {
    pub data: Vec<DetectionAggregate>,
}
#[derive(Debug, Serialize, ToSchema)]
pub struct JobAcceptedResponse {
    pub data: JobAccepted,
}
#[derive(Debug, Serialize, ToSchema)]
pub struct JobStatusResponse {
    pub data: JobStatus,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ReadyResponse {
    pub ok: bool,
    pub status: String,
    pub olaf: bool,
    pub streams: i64,
    pub pending_jobs: i64,
    pub pending_webhooks: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct VersionResponse {
    pub version: String,
    pub contract_version: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TestWebhookData {
    pub queued: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TestWebhookResponse {
    pub data: TestWebhookData,
}
