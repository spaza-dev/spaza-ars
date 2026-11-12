use serde::Deserialize;
use std::env;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub olaf: OlafConfig,
    #[serde(default)]
    pub capture: CaptureConfig,
    #[serde(default)]
    pub query: QueryConfig,
    #[serde(default)]
    pub evidence: EvidenceConfig,
    #[serde(default)]
    pub webhook: WebhookConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub bind: String,
    pub public_base_url: String,
    pub api_token: String,
    pub health_public: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OlafConfig {
    pub rest_url: String,
    pub db_path: String,
    pub store_concurrency: usize,
    pub query_concurrency: usize,
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CaptureConfig {
    pub ffmpeg: String,
    pub yt_dlp: String,
    pub temp_dir: String,
    pub max_streams: usize,
    pub chunk_seconds: u64,
    pub overlap_seconds: u64,
    pub restart_backoff_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueryConfig {
    pub min_confidence: f64,
    pub min_match_count: u64,
    pub aggregation_window_seconds: u64,
    pub dedupe_window_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EvidenceConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub prefix: String,
    pub path_style: bool,
    pub pre_seconds: u64,
    pub post_seconds: u64,
    pub content_type: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WebhookConfig {
    pub timeout_seconds: u64,
    pub max_attempts: u32,
    pub signing_secret: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            olaf: OlafConfig::default(),
            capture: CaptureConfig::default(),
            query: QueryConfig::default(),
            evidence: EvidenceConfig::default(),
            webhook: WebhookConfig::default(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:8080".into(),
            public_base_url: "https://olaf.example.com".into(),
            api_token: "change-me".into(),
            health_public: true,
        }
    }
}

impl Default for OlafConfig {
    fn default() -> Self {
        Self {
            rest_url: "http://127.0.0.1:8920".into(),
            db_path: "/var/lib/ars".into(),
            store_concurrency: 1,
            query_concurrency: 4,
            timeout_seconds: 120,
        }
    }
}

impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            ffmpeg: "ffmpeg".into(),
            yt_dlp: "yt-dlp".into(),
            temp_dir: "/var/lib/ars/work".into(),
            max_streams: 50,
            chunk_seconds: 10,
            overlap_seconds: 2,
            restart_backoff_seconds: 5,
        }
    }
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            min_confidence: 0.5,
            min_match_count: 20,
            aggregation_window_seconds: 60,
            dedupe_window_seconds: 30,
        }
    }
}

impl Default for EvidenceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: "".into(),
            region: "auto".into(),
            bucket: "".into(),
            access_key: "".into(),
            secret_key: "".into(),
            prefix: "evidence".into(),
            path_style: true,
            pre_seconds: 10,
            post_seconds: 20,
            content_type: "audio/mpeg".into(),
        }
    }
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            timeout_seconds: 15,
            max_attempts: 10,
            signing_secret: "".into(),
        }
    }
}

fn env_or(name: &str, current: String) -> String {
    env::var(name).unwrap_or(current)
}

impl Config {
    pub fn load(path: Option<&str>) -> anyhow::Result<Self> {
        let mut config = if let Some(path) = path {
            if Path::new(path).exists() {
                let raw = std::fs::read_to_string(path)?;
                toml::from_str(&raw)?
            } else {
                Config::default()
            }
        } else {
            Config::default()
        };

        config.server.bind = env_or("ARS_BIND", config.server.bind);
        config.server.public_base_url =
            env_or("ARS_PUBLIC_BASE_URL", config.server.public_base_url);
        config.server.api_token = env_or("ARS_API_TOKEN", config.server.api_token);
        config.server.health_public = env::var("ARS_HEALTH_PUBLIC")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(config.server.health_public);

        config.olaf.rest_url = env_or("OLAF_REST_URL", config.olaf.rest_url);
        config.olaf.db_path = env_or("ARS_DB_PATH", config.olaf.db_path);
        config.olaf.query_concurrency = env::var("ARS_QUERY_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(config.olaf.query_concurrency);
        config.olaf.timeout_seconds = env::var("OLAF_TIMEOUT_SECONDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(config.olaf.timeout_seconds);

        config.capture.temp_dir = env_or("ARS_TEMP_DIR", config.capture.temp_dir);
        config.capture.max_streams = env::var("ARS_MAX_STREAMS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(config.capture.max_streams);
        config.capture.chunk_seconds = env::var("ARS_CHUNK_SECONDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(config.capture.chunk_seconds);
        config.capture.overlap_seconds = env::var("ARS_OVERLAP_SECONDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(config.capture.overlap_seconds);

        config.query.min_confidence = env::var("ARS_MIN_CONFIDENCE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(config.query.min_confidence);
        config.query.min_match_count = env::var("ARS_MIN_MATCH_COUNT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(config.query.min_match_count);
        config.query.aggregation_window_seconds = env::var("ARS_AGGREGATION_WINDOW_SECONDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(config.query.aggregation_window_seconds);

        config.evidence.enabled = env::var("ARS_EVIDENCE_ENABLED")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(config.evidence.enabled);
        config.evidence.endpoint = env_or("ARS_EVIDENCE_S3_ENDPOINT", config.evidence.endpoint);
        config.evidence.region = env_or("ARS_EVIDENCE_S3_REGION", config.evidence.region);
        config.evidence.bucket = env_or("ARS_EVIDENCE_S3_BUCKET", config.evidence.bucket);
        config.evidence.access_key =
            env_or("ARS_EVIDENCE_S3_ACCESS_KEY", config.evidence.access_key);
        config.evidence.secret_key =
            env_or("ARS_EVIDENCE_S3_SECRET_KEY", config.evidence.secret_key);
        config.evidence.prefix = env_or("ARS_EVIDENCE_S3_PREFIX", config.evidence.prefix);
        config.evidence.path_style = env::var("ARS_EVIDENCE_S3_PATH_STYLE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(config.evidence.path_style);

        config.webhook.signing_secret =
            env_or("ARS_WEBHOOK_SIGNING_SECRET", config.webhook.signing_secret);

        Ok(config)
    }
}
