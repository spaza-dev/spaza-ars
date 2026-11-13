use crate::config::Config;
use crate::db::{self, Db};
use crate::engine::OlafEngine;
use crate::evidence;
use crate::webhook::WebhookSender;
use anyhow::{anyhow, Result};
use serde_json::Value;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};

pub async fn run_query_worker(config: Config, db: Db, engine: OlafEngine) {
    let mut ticker = interval(Duration::from_millis(500));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        loop {
            match db::claim_job(&db, "query") {
                Ok(Some((job_id, payload, _attempts))) => {
                    let result = process_query_job(&config, &db, &engine, &payload).await;
                    match result {
                        Ok(()) => {
                            let _ = db::finish_job(&db, &job_id, None, 10);
                        }
                        Err(error) => {
                            let _ = db::finish_job(&db, &job_id, Some(&error.to_string()), 10);
                        }
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    tracing::warn!(%error, "query job claim failed");
                    break;
                }
            }
        }
    }
}

async fn process_query_job(
    config: &Config,
    db: &Db,
    engine: &OlafEngine,
    payload: &Value,
) -> Result<()> {
    let stream_id = payload
        .get("stream_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing stream_id"))?;
    let path = payload
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing chunk path"))?;

    let stream =
        db::get_stream(db, stream_id)?.ok_or_else(|| anyhow!("stream no longer exists"))?;
    let bytes = tokio::fs::read(path).await?;
    let matches = engine.query(bytes.clone(), true).await?;
    let mut found = false;

    for item in matches {
        let identifier = match_identifier(&item);
        let Some(identifier) = identifier else {
            continue;
        };
        let Some(catalog_item) = db::find_catalog_item_by_fingerprint(db, &identifier)? else {
            continue;
        };
        if !stream.catalogs.is_empty() && !stream.catalogs.contains(&catalog_item.catalog) {
            continue;
        }

        let match_count = item
            .get("match_count")
            .and_then(Value::as_i64)
            .or_else(|| item.get("count").and_then(Value::as_i64))
            .unwrap_or(0);
        if match_count > 0 && match_count < config.query.min_match_count as i64 {
            continue;
        }

        let confidence = item
            .get("confidence")
            .and_then(Value::as_f64)
            .or_else(|| item.get("score").and_then(Value::as_f64).map(|v| v / 100.0));
        if let Some(confidence) = confidence {
            if confidence < config.query.min_confidence {
                continue;
            }
        }

        let started_at = item
            .get("matched_at")
            .and_then(Value::as_str)
            .or_else(|| item.get("timestamp").and_then(Value::as_str))
            .map(str::to_string)
            .unwrap_or_else(db::now);
        let ended_at = item
            .get("ended_at")
            .and_then(Value::as_str)
            .map(str::to_string);

        let evidence_id = if stream.evidence_enabled != 0 && config.evidence.enabled {
            let key = format!(
                "{}/{}/{}/chunk-{}.wav",
                config.evidence.prefix.trim_end_matches('/'),
                stream.id,
                chrono::Utc::now().format("%Y-%m-%d"),
                uuid::Uuid::new_v4().simple()
            );
            match evidence::upload_evidence(
                &config.evidence,
                &key,
                bytes.clone(),
                &config.evidence.content_type,
            )
            .await
            {
                Ok(()) => Some(db::insert_evidence(
                    db,
                    &stream.id,
                    None,
                    &config.evidence.bucket,
                    &key,
                    &config.evidence.content_type,
                    bytes.len() as i64,
                    Some(stream.chunk_seconds as f64),
                )?),
                Err(error) => {
                    tracing::warn!(stream_id = %stream.id, %error, "evidence upload failed");
                    None
                }
            }
        } else {
            None
        };

        db::insert_detection(
            db,
            &stream.id,
            &catalog_item.id,
            &started_at,
            ended_at.as_deref(),
            confidence,
            Some(match_count),
            evidence_id.as_deref(),
            &item,
        )?;
        found = true;
    }

    if found {
        db::mark_stream_match(db, &stream.id)?;
    }
    let _ = tokio::fs::remove_file(path).await;
    Ok(())
}

fn match_identifier(value: &Value) -> Option<String> {
    for key in [
        "match_identifier",
        "identifier",
        "path",
        "match",
        "reference",
        "catalog_item_id",
    ] {
        if let Some(value) = value.get(key) {
            if let Some(text) = value.as_str() {
                return Some(text.to_string());
            }
            if let Some(text) = value.get("path").and_then(Value::as_str) {
                return Some(text.to_string());
            }
            if let Some(text) = value.get("identifier").and_then(Value::as_str) {
                return Some(text.to_string());
            }
        }
    }
    None
}

fn enqueue_index_webhook(
    db: &Db,
    catalog_item_id: &str,
    job_id: &str,
    callback_id: Option<&str>,
    event: &str,
    error: Option<&str>,
) -> Result<()> {
    let callback_id = match callback_id {
        Some(value) => value.to_string(),
        None => match db::first_enabled_callback_id(db)? {
            Some(value) => value,
            None => return Ok(()),
        },
    };
    let item = match db::get_catalog_item(db, catalog_item_id)? {
        Some(item) => item,
        None => return Ok(()),
    };
    let payload = serde_json::json!({
        "event": event,
        "catalog_item_id": item.id,
        "catalog": item.catalog,
        "external_id": item.external_id,
        "fingerprint_id": item.fingerprint_id,
        "status": item.status,
        "indexed_at": item.indexed_at,
        "job_id": job_id,
        "error": error,
    });
    db::enqueue_webhook(
        db,
        &callback_id,
        event,
        &payload,
        &format!("catalog-index:{}:{}", job_id, event),
    )?;
    Ok(())
}

pub async fn run_index_worker(config: Config, db: Db, engine: OlafEngine) {
    let download_client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(config.index.download_timeout_seconds))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            tracing::error!(%error, "failed to build index download client");
            return;
        }
    };
    let mut ticker = interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        loop {
            match db::claim_job(&db, "index") {
                Ok(Some((job_id, payload, _attempts))) => {
                    let result =
                        process_index_job(&config, &db, &engine, &download_client, &payload).await;
                    match result {
                        Ok(()) => {
                            if let Some(item_id) =
                                payload.get("catalog_item_id").and_then(Value::as_str)
                            {
                                let _ = enqueue_index_webhook(
                                    &db,
                                    item_id,
                                    &job_id,
                                    payload.get("callback_id").and_then(Value::as_str),
                                    "catalog.index.completed",
                                    None,
                                );
                            }
                            let _ = db::finish_job(&db, &job_id, None, 1);
                        }
                        Err(error) => {
                            if let Some(item_id) =
                                payload.get("catalog_item_id").and_then(Value::as_str)
                            {
                                let _ =
                                    db::mark_catalog_item_error(&db, item_id, &error.to_string());
                                let _ = enqueue_index_webhook(
                                    &db,
                                    item_id,
                                    &job_id,
                                    payload.get("callback_id").and_then(Value::as_str),
                                    "catalog.index.failed",
                                    Some(&error.to_string()),
                                );
                            }
                            let _ = db::finish_job(&db, &job_id, Some(&error.to_string()), 1);
                        }
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    tracing::warn!(%error, "index job claim failed");
                    break;
                }
            }
        }
    }
}

async fn process_index_job(
    config: &Config,
    db: &Db,
    engine: &OlafEngine,
    client: &reqwest::Client,
    payload: &Value,
) -> Result<()> {
    let item_id = payload
        .get("catalog_item_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing catalog_item_id"))?;
    let item = db::get_catalog_item(db, item_id)?
        .ok_or_else(|| anyhow!("catalog item no longer exists"))?;
    let fingerprint_id = item
        .fingerprint_id
        .clone()
        .unwrap_or_else(|| item.id.clone());

    let (bytes, temp_path) = if let Some(path) = payload.get("path").and_then(Value::as_str) {
        let metadata = tokio::fs::metadata(path).await?;
        if metadata.len() > config.index.max_upload_bytes {
            let _ = tokio::fs::remove_file(path).await;
            return Err(anyhow!(
                "index source exceeds {} bytes",
                config.index.max_upload_bytes
            ));
        }
        match tokio::fs::read(path).await {
            Ok(bytes) => (bytes, Some(path.to_string())),
            Err(error) => {
                let _ = tokio::fs::remove_file(path).await;
                return Err(error.into());
            }
        }
    } else if let Some(key) = payload.get("object_key").and_then(Value::as_str) {
        let requested_bucket = payload.get("object_bucket").and_then(Value::as_str);
        let bucket = config
            .resolve_catalog_bucket(requested_bucket)
            .map_err(|message| anyhow!(message))?;
        let dir = format!("{}/index", config.capture.temp_dir);
        tokio::fs::create_dir_all(&dir).await?;
        let path = format!("{}/{}.bin", dir, item.id);
        if let Err(error) = evidence::download_object(
            client,
            &config.evidence,
            &bucket,
            key,
            &path,
            config.index.max_download_bytes,
        )
        .await
        {
            let _ = tokio::fs::remove_file(&path).await;
            return Err(error);
        }
        match tokio::fs::read(&path).await {
            Ok(bytes) => (bytes, Some(path)),
            Err(error) => {
                let _ = tokio::fs::remove_file(&path).await;
                return Err(error.into());
            }
        }
    } else {
        return Err(anyhow!("index job has no source"));
    };

    let force = payload
        .get("force")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    db::mark_catalog_item_indexing(db, &item.id)?;
    let result = engine.store(&fingerprint_id, bytes, force).await;
    if let Some(path) = temp_path {
        let _ = tokio::fs::remove_file(path).await;
    }
    result?;
    db::mark_catalog_item_indexed(db, &item.id)?;
    Ok(())
}

pub async fn run_aggregator(config: Config, db: Db) {
    let mut ticker = interval(Duration::from_secs(5));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let streams = match db::pending_streams_with_detections(&db) {
            Ok(streams) => streams,
            Err(error) => {
                tracing::warn!(%error, "aggregation stream scan failed");
                continue;
            }
        };
        for stream_id in streams {
            if let Err(error) = aggregate_stream(&config, &db, &stream_id) {
                tracing::warn!(stream_id = %stream_id, %error, "aggregation failed");
            }
        }
    }
}

fn parse_rfc3339(value: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&chrono::Utc))
}

fn match_payload(
    db: &Db,
    catalog_item_id: &str,
    item: Option<&crate::models::CatalogItem>,
    detection: &crate::models::Detection,
    include_end: bool,
) -> Result<Value> {
    let evidence = detection
        .evidence_id
        .as_deref()
        .map(|id| db::get_evidence(db, id))
        .transpose()?
        .flatten();
    let metadata = item
        .and_then(|item| serde_json::from_str::<Value>(&item.metadata_json).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    Ok(serde_json::json!({
        "catalog_item_id": catalog_item_id,
        "external_item_id": item.and_then(|item| item.external_id.clone()),
        "name": item.map(|item| item.name.clone()).unwrap_or_default(),
        "type": item.map(|item| item.item_type.clone()).unwrap_or_else(|| "song".into()),
        "catalog": item.map(|item| item.catalog.clone()).unwrap_or_default(),
        "metadata": metadata,
        "started_at": detection.started_at,
        "ended_at": if include_end { detection.ended_at.clone() } else { None },
        "confidence": detection.confidence,
        "match_count": detection.match_count,
        "evidence": evidence,
    }))
}

fn aggregate_stream(config: &Config, db: &Db, stream_id: &str) -> Result<()> {
    let stream = match db::get_stream(db, stream_id)? {
        Some(stream) => stream,
        None => return Ok(()),
    };
    let detections = db::pending_detections_for_stream(db, stream_id)?;
    if detections.is_empty() {
        return Ok(());
    }
    let window = stream
        .aggregation_window_seconds
        .unwrap_or(config.query.aggregation_window_seconds as i64)
        .max(1);
    if stream.callback_mode == "before" {
        aggregate_before(db, &stream, detections, window)
    } else {
        aggregate_after(db, &stream, detections, window)
    }
}

fn aggregate_after(
    db: &Db,
    stream: &crate::models::Stream,
    detections: Vec<crate::models::Detection>,
    window: i64,
) -> Result<()> {
    let now = chrono::Utc::now();
    let cutoff = now - chrono::Duration::seconds(window);
    let due: Vec<_> = detections
        .into_iter()
        .filter(|detection| {
            parse_rfc3339(&detection.created_at)
                .map(|created| created < cutoff)
                .unwrap_or(true)
        })
        .collect();
    if due.is_empty() {
        return Ok(());
    }

    let mut groups: std::collections::HashMap<String, Vec<crate::models::Detection>> =
        std::collections::HashMap::new();
    for detection in due {
        groups
            .entry(detection.catalog_item_id.clone())
            .or_default()
            .push(detection);
    }

    for (catalog_item_id, detections) in groups {
        let item = db::get_catalog_item(db, &catalog_item_id)?;
        let started = detections
            .iter()
            .map(|detection| detection.started_at.clone())
            .min()
            .unwrap_or_else(db::now);
        let ended = detections
            .iter()
            .filter_map(|detection| {
                detection
                    .ended_at
                    .clone()
                    .or(Some(detection.started_at.clone()))
            })
            .max()
            .unwrap_or_else(db::now);
        let match_count = detections.len() as i64;
        let mut matches = Vec::new();
        for detection in &detections {
            matches.push(match_payload(
                db,
                &catalog_item_id,
                item.as_ref(),
                detection,
                true,
            )?);
        }
        let payload = serde_json::json!({
            "event": "matches.aggregated",
            "callback_mode": stream.callback_mode,
            "stream_id": stream.id,
            "external_stream_id": stream.external_id,
            "catalogs": stream.catalogs,
            "window": { "start": started, "end": ended },
            "matches": matches,
        });
        let aggregate_id =
            db::create_aggregate(db, &stream.id, &started, &ended, match_count, &payload)?;
        if let Some(callback_id) = &stream.callback_id {
            db::enqueue_webhook(
                db,
                callback_id,
                "matches.aggregated",
                &payload,
                &format!("aggregate:{}", aggregate_id),
            )?;
        }
        let ids: Vec<String> = detections
            .into_iter()
            .map(|detection| detection.id)
            .collect();
        db::mark_detections_aggregated(db, &ids)?;
        db::delete_aggregated_detections(db, &ids)?;
    }
    Ok(())
}

fn aggregate_before(
    db: &Db,
    stream: &crate::models::Stream,
    detections: Vec<crate::models::Detection>,
    window: i64,
) -> Result<()> {
    let now = chrono::Utc::now();
    let cutoff = now - chrono::Duration::seconds(window);
    let mut groups: std::collections::HashMap<String, Vec<crate::models::Detection>> =
        std::collections::HashMap::new();
    for detection in detections {
        groups
            .entry(detection.catalog_item_id.clone())
            .or_default()
            .push(detection);
    }

    for (catalog_item_id, mut detections) in groups {
        detections.sort_by(|left, right| left.started_at.cmp(&right.started_at));
        let first = match detections.first() {
            Some(detection) => detection,
            None => continue,
        };
        let last_seen = detections
            .iter()
            .map(|detection| detection.created_at.clone())
            .max()
            .unwrap_or_else(db::now);
        let state = db::get_stream_match_state(db, &stream.id, &catalog_item_id)?;
        let continuing = state
            .as_ref()
            .and_then(|(_, last_seen, _)| parse_rfc3339(last_seen))
            .map(|last_seen| last_seen > cutoff)
            .unwrap_or(false);
        let already_sent = state
            .as_ref()
            .and_then(|(_, _, callback_sent_at)| callback_sent_at.clone());
        let emit = !continuing || already_sent.is_none();
        let started = if continuing {
            state
                .as_ref()
                .map(|(started_at, _, _)| started_at.clone())
                .unwrap_or_else(|| first.started_at.clone())
        } else {
            first.started_at.clone()
        };
        let callback_sent_at = if emit { Some(db::now()) } else { None };
        db::upsert_stream_match_state(
            db,
            &stream.id,
            &catalog_item_id,
            &started,
            &last_seen,
            callback_sent_at.as_deref(),
        )?;

        if emit {
            let item = db::get_catalog_item(db, &catalog_item_id)?;
            let matches = vec![match_payload(
                db,
                &catalog_item_id,
                item.as_ref(),
                first,
                false,
            )?];
            let payload = serde_json::json!({
                "event": "matches.aggregated",
                "callback_mode": "before",
                "stream_id": stream.id,
                "external_stream_id": stream.external_id,
                "catalogs": stream.catalogs,
                "window": { "start": started, "end": first.started_at },
                "matches": matches,
            });
            let aggregate_id =
                db::create_aggregate(db, &stream.id, &started, &first.started_at, 1, &payload)?;
            if let Some(callback_id) = &stream.callback_id {
                db::enqueue_webhook(
                    db,
                    callback_id,
                    "matches.aggregated",
                    &payload,
                    &format!("aggregate:{}", aggregate_id),
                )?;
            }
        }

        let ids: Vec<String> = detections
            .into_iter()
            .map(|detection| detection.id)
            .collect();
        db::mark_detections_aggregated(db, &ids)?;
        db::delete_aggregated_detections(db, &ids)?;
    }
    Ok(())
}

pub async fn run_cleanup(db: Db) {
    let mut ticker = interval(Duration::from_secs(3600));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        match db::cleanup_aggregates(&db) {
            Ok(count) => {
                if count > 0 {
                    tracing::info!(count, "removed expired detection aggregates");
                }
            }
            Err(error) => tracing::warn!(%error, "aggregate cleanup failed"),
        }
        let state_cutoff = (chrono::Utc::now() - chrono::Duration::hours(24)).to_rfc3339();
        match db::cleanup_stream_match_state(&db, &state_cutoff) {
            Ok(count) => {
                if count > 0 {
                    tracing::info!(count, "removed expired stream match state");
                }
            }
            Err(error) => tracing::warn!(%error, "stream match state cleanup failed"),
        }
    }
}

pub async fn run_webhook_worker(config: Config, db: Db) {
    let sender = match WebhookSender::new(&config) {
        Ok(sender) => sender,
        Err(error) => {
            tracing::error!(%error, "failed to create webhook sender");
            return;
        }
    };
    let mut ticker = interval(Duration::from_secs(2));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        if let Err(error) = sender.process_outbox(&db).await {
            tracing::warn!(%error, "webhook outbox processing failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{self, Db};
    use crate::models::{CreateCallback, CreateStream};
    use rusqlite::params;

    fn test_db() -> Db {
        let path = std::env::temp_dir().join(format!("ars-test-{}.db", uuid::Uuid::new_v4()));
        let db = db::open(path.to_str().unwrap()).unwrap();
        db::migrate(&db, include_str!("../migrations/0001_init.sql")).unwrap();
        db::apply_migration(
            &db,
            2,
            include_str!("../migrations/0002_catalog_idempotency.sql"),
        )
        .unwrap();
        db::apply_migration(
            &db,
            3,
            include_str!("../migrations/0003_outbox_updated_at.sql"),
        )
        .unwrap();
        db::apply_migration(
            &db,
            4,
            include_str!("../migrations/0004_stream_callback_mode.sql"),
        )
        .unwrap();
        db
    }

    fn callback(db: &Db) -> crate::models::Callback {
        db::create_callback(
            db,
            CreateCallback {
                name: "test".into(),
                url: "https://console.test/callback".into(),
                auth_type: "bearer".into(),
                token: None,
                signing_secret: None,
                event_types: None,
                enabled: true,
            },
        )
        .unwrap()
    }

    fn stream(db: &Db, callback_id: String, mode: &str) -> crate::models::Stream {
        db::create_stream(
            db,
            CreateStream {
                name: "Stream".into(),
                url: "https://example.test/live".into(),
                source_type: "radio".into(),
                external_id: None,
                catalogs: vec!["private".into()],
                enabled: true,
                evidence_enabled: false,
                evidence_pre_seconds: None,
                evidence_post_seconds: None,
                callback_id: Some(callback_id),
                callback_mode: mode.into(),
                chunk_seconds: None,
                overlap_seconds: None,
                aggregation_window_seconds: Some(300),
            },
        )
        .unwrap()
    }

    fn payloads(db: &Db) -> Vec<serde_json::Value> {
        let conn = db.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT payload_json FROM webhook_outbox ORDER BY created_at ASC")
            .unwrap();
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .map(|row| serde_json::from_str(&row.unwrap()).unwrap())
            .collect();
        rows
    }

    #[test]
    fn streams_attach_the_enabled_callback_automatically() {
        let db = test_db();
        let cb = callback(&db);
        let created = db::create_stream(
            &db,
            CreateStream {
                name: "Stream".into(),
                url: "https://example.test/live".into(),
                source_type: "radio".into(),
                external_id: None,
                catalogs: vec![],
                enabled: true,
                evidence_enabled: false,
                evidence_pre_seconds: None,
                evidence_post_seconds: None,
                callback_id: None,
                callback_mode: "after".into(),
                chunk_seconds: None,
                overlap_seconds: None,
                aggregation_window_seconds: None,
            },
        )
        .unwrap();
        assert_eq!(created.callback_id.as_deref(), Some(cb.id.as_str()));
        let updated = db::update_stream(
            &db,
            &created.id,
            crate::models::UpdateStream {
                name: None,
                url: None,
                source_type: None,
                external_id: None,
                catalogs: None,
                enabled: None,
                evidence_enabled: None,
                evidence_pre_seconds: None,
                evidence_post_seconds: None,
                callback_id: None,
                callback_mode: None,
                chunk_seconds: None,
                overlap_seconds: None,
                aggregation_window_seconds: None,
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(updated.callback_id.as_deref(), Some(cb.id.as_str()));
    }

    #[test]
    fn stream_status_changes_are_queued_for_the_callback() {
        let db = test_db();
        let cb = callback(&db);
        let stream = stream(&db, cb.id, "after");
        db::set_stream_status(&db, &stream.id, "running", None).unwrap();
        let queued = payloads(&db);
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0]["event"], "stream.status");
        assert_eq!(queued[0]["status"], "running");
        db::set_stream_status(&db, &stream.id, "running", None).unwrap();
        assert_eq!(payloads(&db).len(), 1);
    }

    #[test]
    fn before_mode_emits_once_per_airing() {
        let db = test_db();
        let cb = callback(&db);
        let stream = stream(&db, cb.id, "before");
        let item = db::create_catalog_item(
            &db,
            "private",
            Some("track-1"),
            "Song A",
            "song",
            &serde_json::json!({"artist": "Artist A"}),
            "fp-1",
        )
        .unwrap();
        let now = chrono::Utc::now().to_rfc3339();
        db::insert_detection(
            &db,
            &stream.id,
            &item.id,
            &now,
            None,
            Some(0.9),
            Some(1),
            None,
            &serde_json::json!({}),
        )
        .unwrap();
        let pending = db::pending_detections_for_stream(&db, &stream.id).unwrap();
        aggregate_before(&db, &stream, pending, 300).unwrap();
        let first = payloads(&db);
        assert_eq!(first.len(), 1);
        assert_eq!(first[0]["callback_mode"], "before");
        assert_eq!(first[0]["matches"][0]["external_item_id"], "track-1");
        assert_eq!(first[0]["matches"][0]["metadata"]["artist"], "Artist A");

        db::insert_detection(
            &db,
            &stream.id,
            &item.id,
            &now,
            None,
            Some(0.9),
            Some(1),
            None,
            &serde_json::json!({}),
        )
        .unwrap();
        let pending = db::pending_detections_for_stream(&db, &stream.id).unwrap();
        aggregate_before(&db, &stream, pending, 300).unwrap();
        assert_eq!(payloads(&db).len(), 1);

        let old = (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
        db.lock()
            .unwrap()
            .execute(
                "UPDATE stream_match_state SET last_seen_at = ? WHERE stream_id = ?",
                params![old, stream.id],
            )
            .unwrap();
        db::insert_detection(
            &db,
            &stream.id,
            &item.id,
            &now,
            None,
            Some(0.9),
            Some(1),
            None,
            &serde_json::json!({}),
        )
        .unwrap();
        let pending = db::pending_detections_for_stream(&db, &stream.id).unwrap();
        aggregate_before(&db, &stream, pending, 300).unwrap();
        assert_eq!(payloads(&db).len(), 2);
    }

    #[test]
    fn after_mode_includes_catalog_metadata() {
        let db = test_db();
        let cb = callback(&db);
        let stream = stream(&db, cb.id, "after");
        let item = db::create_catalog_item(
            &db,
            "public",
            Some("isrc-1"),
            "Song B",
            "jingle",
            &serde_json::json!({"artist": "Artist B"}),
            "fp-2",
        )
        .unwrap();
        let detection_id = db::insert_detection(
            &db,
            &stream.id,
            &item.id,
            &chrono::Utc::now().to_rfc3339(),
            None,
            Some(0.8),
            Some(2),
            None,
            &serde_json::json!({}),
        )
        .unwrap();
        let old = (chrono::Utc::now() - chrono::Duration::minutes(10)).to_rfc3339();
        db.lock()
            .unwrap()
            .execute(
                "UPDATE detections SET created_at = ? WHERE id = ?",
                params![old, detection_id],
            )
            .unwrap();
        let pending = db::pending_detections_for_stream(&db, &stream.id).unwrap();
        aggregate_after(&db, &stream, pending, 60).unwrap();
        let payloads = payloads(&db);
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0]["callback_mode"], "after");
        assert_eq!(payloads[0]["matches"][0]["external_item_id"], "isrc-1");
        assert_eq!(payloads[0]["matches"][0]["catalog"], "public");
        assert_eq!(payloads[0]["matches"][0]["type"], "jingle");
        assert_eq!(payloads[0]["matches"][0]["metadata"]["artist"], "Artist B");
    }
}
