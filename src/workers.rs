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
    let now = chrono::Utc::now();
    let cutoff = now - chrono::Duration::seconds(window);
    let due: Vec<_> = detections
        .into_iter()
        .filter(|detection| {
            chrono::DateTime::parse_from_rfc3339(&detection.created_at)
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
            .map(|d| d.started_at.clone())
            .min()
            .unwrap_or_else(db::now);
        let ended = detections
            .iter()
            .filter_map(|d| d.ended_at.clone().or(Some(d.started_at.clone())))
            .max()
            .unwrap_or_else(db::now);
        let match_count = detections.len() as i64;
        let mut matches = Vec::new();
        for detection in &detections {
            let evidence = detection
                .evidence_id
                .as_deref()
                .map(|id| db::get_evidence(db, id))
                .transpose()?
                .flatten();
            matches.push(serde_json::json!({
                "catalog_item_id": catalog_item_id,
                "external_item_id": item.as_ref().and_then(|i| i.external_id.clone()),
                "name": item.as_ref().map(|i| i.name.clone()).unwrap_or_default(),
                "type": item.as_ref().map(|i| i.item_type.clone()).unwrap_or_else(|| "song".into()),
                "catalog": item.as_ref().map(|i| i.catalog.clone()).unwrap_or_default(),
                "started_at": detection.started_at,
                "ended_at": detection.ended_at,
                "confidence": detection.confidence,
                "match_count": detection.match_count,
                "evidence": evidence,
            }));
        }
        let payload = serde_json::json!({
            "event": "matches.aggregated",
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
        let ids: Vec<String> = detections.into_iter().map(|d| d.id).collect();
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
