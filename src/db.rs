use crate::models::*;
use anyhow::{anyhow, Result};
use chrono::{Duration, Utc};
use rusqlite::{params, Connection, OptionalExtension, Row};
use std::path::Path;
use std::sync::{Arc, Mutex};

pub type Db = Arc<Mutex<Connection>>;

pub fn now() -> String {
    Utc::now().to_rfc3339()
}

pub fn open(path: &str) -> Result<Db> {
    if let Some(parent) = Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(path)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.busy_timeout(std::time::Duration::from_secs(10))?;
    Ok(Arc::new(Mutex::new(conn)))
}

pub fn migrate(db: &Db, sql: &str) -> Result<()> {
    let conn = db.lock().unwrap();
    conn.execute_batch(sql)?;
    Ok(())
}

fn catalogs_from_json(raw: &str) -> Vec<String> {
    serde_json::from_str(raw).unwrap_or_default()
}

fn stream_from_row(row: &Row<'_>) -> rusqlite::Result<Stream> {
    let catalogs_raw: String = row.get("catalogs")?;
    Ok(Stream {
        id: row.get("id")?,
        name: row.get("name")?,
        url: row.get("url")?,
        source_type: row.get("source_type")?,
        external_id: row.get("external_id")?,
        catalogs: catalogs_from_json(&catalogs_raw),
        enabled: row.get("enabled")?,
        evidence_enabled: row.get("evidence_enabled")?,
        evidence_pre_seconds: row.get("evidence_pre_seconds")?,
        evidence_post_seconds: row.get("evidence_post_seconds")?,
        callback_id: row.get("callback_id")?,
        chunk_seconds: row.get("chunk_seconds")?,
        overlap_seconds: row.get("overlap_seconds")?,
        aggregation_window_seconds: row.get("aggregation_window_seconds")?,
        status: row.get("status")?,
        last_error: row.get("last_error")?,
        last_audio_at: row.get("last_audio_at")?,
        last_match_at: row.get("last_match_at")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
    })
}

fn callback_from_row(row: &Row<'_>) -> rusqlite::Result<Callback> {
    Ok(Callback {
        id: row.get("id")?,
        name: row.get("name")?,
        url: row.get("url")?,
        auth_type: row.get("auth_type")?,
        token: row.get("token")?,
        signing_secret: row.get("signing_secret")?,
        event_types: row.get("event_types")?,
        enabled: row.get("enabled")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
    })
}

fn catalog_item_from_row(row: &Row<'_>) -> rusqlite::Result<CatalogItem> {
    Ok(CatalogItem {
        id: row.get("id")?,
        catalog: row.get("catalog")?,
        external_id: row.get("external_id")?,
        name: row.get("name")?,
        item_type: row.get("type")?,
        metadata_json: row.get("metadata_json")?,
        fingerprint_id: row.get("fingerprint_id")?,
        status: row.get("status")?,
        indexed_at: row.get("indexed_at")?,
        error: row.get("error")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
    })
}

fn detection_from_row(row: &Row<'_>) -> rusqlite::Result<Detection> {
    Ok(Detection {
        id: row.get("id")?,
        stream_id: row.get("stream_id")?,
        catalog_item_id: row.get("catalog_item_id")?,
        started_at: row.get("started_at")?,
        ended_at: row.get("ended_at")?,
        confidence: row.get("confidence")?,
        match_count: row.get("match_count")?,
        evidence_id: row.get("evidence_id")?,
        aggregated_at: row.get("aggregated_at")?,
        raw_json: row.get("raw_json")?,
        created_at: row.get("created_at")?,
    })
}

pub fn list_streams(db: &Db) -> Result<Vec<Stream>> {
    let conn = db.lock().unwrap();
    let mut stmt = conn.prepare("SELECT * FROM streams ORDER BY name ASC")?;
    let rows = stmt.query_map([], stream_from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn get_stream(db: &Db, id: &str) -> Result<Option<Stream>> {
    let conn = db.lock().unwrap();
    Ok(conn
        .query_row(
            "SELECT * FROM streams WHERE id = ?",
            params![id],
            stream_from_row,
        )
        .optional()?)
}

pub fn enabled_streams(db: &Db) -> Result<Vec<Stream>> {
    let conn = db.lock().unwrap();
    let mut stmt = conn.prepare("SELECT * FROM streams WHERE enabled = 1 ORDER BY name ASC")?;
    let rows = stmt.query_map([], stream_from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn create_stream(db: &Db, input: CreateStream) -> Result<Stream> {
    let id = format!("str_{}", uuid::Uuid::new_v4().simple());
    let now = now();
    let catalogs = serde_json::to_string(&input.catalogs)?;
    {
        let conn = db.lock().unwrap();
        conn.execute(
            "INSERT INTO streams
             (id,name,url,source_type,external_id,catalogs,enabled,evidence_enabled,
              evidence_pre_seconds,evidence_post_seconds,callback_id,chunk_seconds,
              overlap_seconds,aggregation_window_seconds,status,created_at,updated_at)
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            params![
                id,
                input.name,
                input.url,
                input.source_type,
                input.external_id,
                catalogs,
                if input.enabled { 1 } else { 0 },
                if input.evidence_enabled { 1 } else { 0 },
                input.evidence_pre_seconds.unwrap_or(10),
                input.evidence_post_seconds.unwrap_or(20),
                input.callback_id,
                input.chunk_seconds.unwrap_or(10),
                input.overlap_seconds.unwrap_or(2),
                input.aggregation_window_seconds,
                if input.enabled { "starting" } else { "stopped" },
                now,
                now
            ],
        )?;
    }
    get_stream(db, &id)?.ok_or_else(|| anyhow!("stream not found after insert"))
}

pub fn update_stream(db: &Db, id: &str, input: UpdateStream) -> Result<Option<Stream>> {
    let existing = match get_stream(db, id)? {
        Some(value) => value,
        None => return Ok(None),
    };
    let now = now();
    let catalogs = input
        .catalogs
        .map(|value| serde_json::to_string(&value))
        .transpose()?
        .unwrap_or_else(|| {
            serde_json::to_string(&existing.catalogs).unwrap_or_else(|_| "[]".into())
        });
    let enabled = input
        .enabled
        .map(|v| if v { 1 } else { 0 })
        .unwrap_or(existing.enabled);
    {
        let conn = db.lock().unwrap();
        conn.execute(
            "UPDATE streams SET
               name = ?, url = ?, source_type = ?, external_id = ?, catalogs = ?,
               enabled = ?, evidence_enabled = ?, evidence_pre_seconds = ?,
               evidence_post_seconds = ?, callback_id = ?, chunk_seconds = ?,
               overlap_seconds = ?, aggregation_window_seconds = ?, status = ?,
               updated_at = ?
             WHERE id = ?",
            params![
                input.name.unwrap_or(existing.name),
                input.url.unwrap_or(existing.url),
                input.source_type.unwrap_or(existing.source_type),
                input.external_id.or(existing.external_id),
                catalogs,
                enabled,
                input
                    .evidence_enabled
                    .map(|v| if v { 1 } else { 0 })
                    .unwrap_or(existing.evidence_enabled),
                input
                    .evidence_pre_seconds
                    .unwrap_or(existing.evidence_pre_seconds),
                input
                    .evidence_post_seconds
                    .unwrap_or(existing.evidence_post_seconds),
                input.callback_id.or(existing.callback_id),
                input.chunk_seconds.unwrap_or(existing.chunk_seconds),
                input.overlap_seconds.unwrap_or(existing.overlap_seconds),
                input
                    .aggregation_window_seconds
                    .or(existing.aggregation_window_seconds),
                if enabled == 1 { "starting" } else { "stopped" },
                now,
                id
            ],
        )?;
    }
    get_stream(db, id)
}

pub fn delete_stream(db: &Db, id: &str) -> Result<bool> {
    let conn = db.lock().unwrap();
    Ok(conn.execute("DELETE FROM streams WHERE id = ?", params![id])? > 0)
}

pub fn set_stream_status(db: &Db, id: &str, status: &str, error: Option<&str>) -> Result<()> {
    let conn = db.lock().unwrap();
    conn.execute(
        "UPDATE streams SET status = ?, last_error = ?, updated_at = ? WHERE id = ?",
        params![status, error, now(), id],
    )?;
    Ok(())
}

pub fn mark_stream_audio(db: &Db, id: &str) -> Result<()> {
    let conn = db.lock().unwrap();
    conn.execute(
        "UPDATE streams SET last_audio_at = ?, updated_at = ? WHERE id = ?",
        params![now(), now(), id],
    )?;
    Ok(())
}

pub fn mark_stream_match(db: &Db, id: &str) -> Result<()> {
    let conn = db.lock().unwrap();
    conn.execute(
        "UPDATE streams SET last_match_at = ?, updated_at = ? WHERE id = ?",
        params![now(), now(), id],
    )?;
    Ok(())
}

pub fn list_callbacks(db: &Db) -> Result<Vec<Callback>> {
    let conn = db.lock().unwrap();
    let mut stmt = conn.prepare("SELECT * FROM callbacks ORDER BY name ASC")?;
    let rows = stmt.query_map([], callback_from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn get_callback(db: &Db, id: &str) -> Result<Option<Callback>> {
    let conn = db.lock().unwrap();
    Ok(conn
        .query_row(
            "SELECT * FROM callbacks WHERE id = ?",
            params![id],
            callback_from_row,
        )
        .optional()?)
}

pub fn create_callback(db: &Db, input: CreateCallback) -> Result<Callback> {
    let id = format!("cb_{}", uuid::Uuid::new_v4().simple());
    let now = now();
    let events = serde_json::to_string(
        &input
            .event_types
            .unwrap_or_else(|| vec!["matches.aggregated".into(), "stream.status".into()]),
    )?;
    {
        let conn = db.lock().unwrap();
        conn.execute(
            "INSERT INTO callbacks
             (id,name,url,auth_type,token,signing_secret,event_types,enabled,created_at,updated_at)
             VALUES (?,?,?,?,?,?,?,?,?,?)",
            params![
                id,
                input.name,
                input.url,
                input.auth_type,
                input.token,
                input.signing_secret,
                events,
                if input.enabled { 1 } else { 0 },
                now,
                now
            ],
        )?;
    }
    get_callback(db, &id)?.ok_or_else(|| anyhow!("callback not found after insert"))
}

pub fn update_callback(db: &Db, id: &str, input: UpdateCallback) -> Result<Option<Callback>> {
    let existing = match get_callback(db, id)? {
        Some(value) => value,
        None => return Ok(None),
    };
    let events = input
        .event_types
        .map(|value| serde_json::to_string(&value))
        .transpose()?
        .unwrap_or(existing.event_types);
    {
        let conn = db.lock().unwrap();
        conn.execute(
            "UPDATE callbacks SET name = ?, url = ?, auth_type = ?, token = ?,
             signing_secret = ?, event_types = ?, enabled = ?, updated_at = ? WHERE id = ?",
            params![
                input.name.unwrap_or(existing.name),
                input.url.unwrap_or(existing.url),
                input.auth_type.unwrap_or(existing.auth_type),
                input.token.or(existing.token),
                input.signing_secret.or(existing.signing_secret),
                events,
                input
                    .enabled
                    .map(|v| if v { 1 } else { 0 })
                    .unwrap_or(existing.enabled),
                now(),
                id
            ],
        )?;
    }
    get_callback(db, id)
}

pub fn delete_callback(db: &Db, id: &str) -> Result<bool> {
    let conn = db.lock().unwrap();
    Ok(conn.execute("DELETE FROM callbacks WHERE id = ?", params![id])? > 0)
}

pub fn create_catalog_item(
    db: &Db,
    catalog: &str,
    external_id: Option<&str>,
    name: &str,
    item_type: &str,
    metadata: &serde_json::Value,
    fingerprint_id: &str,
) -> Result<CatalogItem> {
    let id = format!("cat_{}", uuid::Uuid::new_v4().simple());
    let now = now();
    {
        let conn = db.lock().unwrap();
        conn.execute(
            "INSERT INTO catalog_items
             (id,catalog,external_id,name,type,metadata_json,fingerprint_id,status,created_at,updated_at)
             VALUES (?,?,?,?,?,?,?,?,?,?)",
            params![
                id,
                catalog,
                external_id,
                name,
                item_type,
                metadata.to_string(),
                fingerprint_id,
                "pending",
                now,
                now
            ],
        )?;
    }
    get_catalog_item(db, &id)?.ok_or_else(|| anyhow!("catalog item not found after insert"))
}

pub fn get_catalog_item(db: &Db, id: &str) -> Result<Option<CatalogItem>> {
    let conn = db.lock().unwrap();
    Ok(conn
        .query_row(
            "SELECT * FROM catalog_items WHERE id = ?",
            params![id],
            catalog_item_from_row,
        )
        .optional()?)
}

pub fn find_catalog_item_by_fingerprint(db: &Db, fingerprint: &str) -> Result<Option<CatalogItem>> {
    let conn = db.lock().unwrap();
    Ok(conn
        .query_row(
            "SELECT * FROM catalog_items WHERE fingerprint_id = ? LIMIT 1",
            params![fingerprint],
            catalog_item_from_row,
        )
        .optional()?)
}

pub fn list_catalog_items(db: &Db, catalog: Option<&str>) -> Result<Vec<CatalogItem>> {
    let conn = db.lock().unwrap();
    if let Some(catalog) = catalog {
        let mut stmt =
            conn.prepare("SELECT * FROM catalog_items WHERE catalog = ? ORDER BY created_at DESC")?;
        let rows = stmt.query_map(params![catalog], catalog_item_from_row)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    } else {
        let mut stmt = conn.prepare("SELECT * FROM catalog_items ORDER BY created_at DESC")?;
        let rows = stmt.query_map([], catalog_item_from_row)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }
}

pub fn mark_catalog_item_indexed(db: &Db, id: &str) -> Result<()> {
    let conn = db.lock().unwrap();
    conn.execute(
        "UPDATE catalog_items SET status = 'indexed', indexed_at = ?, error = NULL, updated_at = ? WHERE id = ?",
        params![now(), now(), id],
    )?;
    Ok(())
}

pub fn mark_catalog_item_error(db: &Db, id: &str, error: &str) -> Result<()> {
    let conn = db.lock().unwrap();
    conn.execute(
        "UPDATE catalog_items SET status = 'error', error = ?, updated_at = ? WHERE id = ?",
        params![error, now(), id],
    )?;
    Ok(())
}

pub fn enqueue_job(
    db: &Db,
    kind: &str,
    payload: &serde_json::Value,
    priority: i64,
) -> Result<String> {
    let id = format!("job_{}", uuid::Uuid::new_v4().simple());
    let now = now();
    let conn = db.lock().unwrap();
    conn.execute(
        "INSERT INTO jobs(id,kind,status,priority,payload_json,created_at,updated_at)
         VALUES (?,?, 'pending', ?, ?, ?, ?)",
        params![id, kind, priority, payload.to_string(), now, now],
    )?;
    Ok(id)
}

pub fn claim_job(db: &Db, kind: &str) -> Result<Option<(String, serde_json::Value, i64)>> {
    let now = now();
    let conn = db.lock().unwrap();
    let row = conn
        .query_row(
            "SELECT id,payload_json,attempts FROM jobs
             WHERE kind = ? AND status = 'pending'
               AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
             ORDER BY priority DESC, created_at ASC LIMIT 1",
            params![kind, now],
            |row| {
                let id: String = row.get(0)?;
                let payload: String = row.get(1)?;
                let attempts: i64 = row.get(2)?;
                Ok((id, payload, attempts))
            },
        )
        .optional()?;
    if let Some((id, payload, attempts)) = row {
        conn.execute(
            "UPDATE jobs SET status = 'running', attempts = ?, locked_at = ?, updated_at = ? WHERE id = ?",
            params![attempts + 1, now, now, id],
        )?;
        Ok(Some((id, serde_json::from_str(&payload)?, attempts)))
    } else {
        Ok(None)
    }
}

pub fn finish_job(db: &Db, id: &str, error: Option<&str>, max_attempts: i64) -> Result<()> {
    let conn = db.lock().unwrap();
    if let Some(error) = error {
        let attempts: i64 = conn.query_row(
            "SELECT attempts FROM jobs WHERE id = ?",
            params![id],
            |row| row.get(0),
        )?;
        if attempts >= max_attempts {
            conn.execute(
                "UPDATE jobs SET status = 'failed', error = ?, updated_at = ? WHERE id = ?",
                params![error, now(), id],
            )?;
        } else {
            let delay = std::cmp::min(3600, 30 * 2_i64.pow(std::cmp::min(attempts as u32, 7)));
            let next = (Utc::now() + Duration::seconds(delay)).to_rfc3339();
            conn.execute(
                "UPDATE jobs SET status = 'pending', error = ?, next_attempt_at = ?, updated_at = ? WHERE id = ?",
                params![error, next, now(), id],
            )?;
        }
    } else {
        conn.execute(
            "UPDATE jobs SET status = 'done', error = NULL, updated_at = ? WHERE id = ?",
            params![now(), id],
        )?;
    }
    Ok(())
}

pub fn insert_detection(
    db: &Db,
    stream_id: &str,
    catalog_item_id: &str,
    started_at: &str,
    ended_at: Option<&str>,
    confidence: Option<f64>,
    match_count: Option<i64>,
    evidence_id: Option<&str>,
    raw: &serde_json::Value,
) -> Result<String> {
    let id = format!("det_{}", uuid::Uuid::new_v4().simple());
    let conn = db.lock().unwrap();
    conn.execute(
        "INSERT INTO detections
         (id,stream_id,catalog_item_id,started_at,ended_at,confidence,match_count,evidence_id,raw_json,created_at)
         VALUES (?,?,?,?,?,?,?,?,?,?)",
        params![
            id,
            stream_id,
            catalog_item_id,
            started_at,
            ended_at,
            confidence,
            match_count,
            evidence_id,
            raw.to_string(),
            now()
        ],
    )?;
    Ok(id)
}

pub fn list_detections(db: &Db, stream_id: Option<&str>) -> Result<Vec<Detection>> {
    let conn = db.lock().unwrap();
    if let Some(stream_id) = stream_id {
        let mut stmt = conn.prepare(
            "SELECT * FROM detections WHERE stream_id = ? ORDER BY created_at DESC LIMIT 500",
        )?;
        let rows = stmt.query_map(params![stream_id], detection_from_row)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    } else {
        let mut stmt =
            conn.prepare("SELECT * FROM detections ORDER BY created_at DESC LIMIT 500")?;
        let rows = stmt.query_map([], detection_from_row)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }
}

pub fn pending_detections_for_stream(db: &Db, stream_id: &str) -> Result<Vec<Detection>> {
    let conn = db.lock().unwrap();
    let mut stmt = conn.prepare(
        "SELECT * FROM detections WHERE stream_id = ? AND aggregated_at IS NULL ORDER BY started_at ASC",
    )?;
    let rows = stmt.query_map(params![stream_id], detection_from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn pending_streams_with_detections(db: &Db) -> Result<Vec<String>> {
    let conn = db.lock().unwrap();
    let mut stmt =
        conn.prepare("SELECT DISTINCT stream_id FROM detections WHERE aggregated_at IS NULL")?;
    let rows = stmt.query_map([], |row| row.get(0))?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn create_aggregate(
    db: &Db,
    stream_id: &str,
    window_start: &str,
    window_end: &str,
    match_count: i64,
    payload: &serde_json::Value,
) -> Result<String> {
    let id = format!("agg_{}", uuid::Uuid::new_v4().simple());
    let conn = db.lock().unwrap();
    conn.execute(
        "INSERT INTO detection_aggregates
         (id,stream_id,window_start,window_end,match_count,payload_json,status,created_at)
         VALUES (?,?,?,?,?,?, 'created', ?)",
        params![
            id,
            stream_id,
            window_start,
            window_end,
            match_count,
            payload.to_string(),
            now()
        ],
    )?;
    Ok(id)
}

pub fn mark_detections_aggregated(db: &Db, ids: &[String]) -> Result<()> {
    let conn = db.lock().unwrap();
    let now = now();
    for id in ids {
        conn.execute(
            "UPDATE detections SET aggregated_at = ? WHERE id = ?",
            params![now, id],
        )?;
    }
    Ok(())
}

pub fn delete_aggregated_detections(db: &Db, ids: &[String]) -> Result<()> {
    let conn = db.lock().unwrap();
    for id in ids {
        conn.execute("DELETE FROM detections WHERE id = ?", params![id])?;
    }
    Ok(())
}

pub fn list_aggregates(db: &Db, stream_id: Option<&str>) -> Result<Vec<DetectionAggregate>> {
    let conn = db.lock().unwrap();
    let mapper = |row: &Row<'_>| {
        Ok(DetectionAggregate {
            id: row.get("id")?,
            stream_id: row.get("stream_id")?,
            window_start: row.get("window_start")?,
            window_end: row.get("window_end")?,
            match_count: row.get("match_count")?,
            payload_json: row.get("payload_json")?,
            status: row.get("status")?,
            created_at: row.get("created_at")?,
        })
    };
    if let Some(stream_id) = stream_id {
        let mut stmt = conn.prepare(
            "SELECT * FROM detection_aggregates WHERE stream_id = ? ORDER BY created_at DESC LIMIT 200",
        )?;
        let rows = stmt.query_map(params![stream_id], mapper)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    } else {
        let mut stmt =
            conn.prepare("SELECT * FROM detection_aggregates ORDER BY created_at DESC LIMIT 200")?;
        let rows = stmt.query_map([], mapper)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }
}

pub fn cleanup_aggregates(db: &Db) -> Result<usize> {
    let cutoff = (Utc::now() - Duration::hours(24)).to_rfc3339();
    let conn = db.lock().unwrap();
    Ok(conn.execute(
        "DELETE FROM detection_aggregates WHERE created_at < ?",
        params![cutoff],
    )?)
}

pub fn enqueue_webhook(
    db: &Db,
    callback_id: &str,
    event_type: &str,
    payload: &serde_json::Value,
    idempotency_key: &str,
) -> Result<String> {
    let id = format!("wh_{}", uuid::Uuid::new_v4().simple());
    let now = now();
    let conn = db.lock().unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO webhook_outbox
         (id,callback_id,event_type,payload_json,idempotency_key,status,created_at)
         VALUES (?,?,?,?,?, 'pending', ?)",
        params![
            id,
            callback_id,
            event_type,
            payload.to_string(),
            idempotency_key,
            now
        ],
    )?;
    Ok(id)
}

pub fn due_webhooks(db: &Db, limit: usize) -> Result<Vec<(String, Callback, String, String, i64)>> {
    let now = now();
    let conn = db.lock().unwrap();
    let mut stmt = conn.prepare(
        "SELECT w.id,w.callback_id,w.event_type,w.payload_json,w.attempts
         FROM webhook_outbox w
         WHERE w.status = 'pending' AND (w.next_attempt_at IS NULL OR w.next_attempt_at <= ?)
         ORDER BY w.created_at ASC LIMIT ?",
    )?;
    let rows = stmt.query_map(params![now, limit as i64], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)?,
        ))
    })?;
    let mut raw = Vec::new();
    for row in rows {
        raw.push(row?);
    }
    drop(stmt);
    drop(conn);
    let mut result = Vec::new();
    for (id, callback_id, event_type, payload, attempts) in raw {
        if let Some(callback) = get_callback(db, &callback_id)? {
            result.push((id, callback, event_type, payload, attempts));
        }
    }
    Ok(result)
}

pub fn mark_webhook_sent(db: &Db, id: &str) -> Result<()> {
    let conn = db.lock().unwrap();
    conn.execute(
        "UPDATE webhook_outbox SET status = 'sent', sent_at = ?, updated_at = ? WHERE id = ?",
        params![now(), now(), id],
    )?;
    Ok(())
}

pub fn mark_webhook_failed(db: &Db, id: &str, error: &str, max_attempts: i64) -> Result<()> {
    let conn = db.lock().unwrap();
    let attempts: i64 = conn.query_row(
        "SELECT attempts FROM webhook_outbox WHERE id = ?",
        params![id],
        |row| row.get(0),
    )?;
    if attempts + 1 >= max_attempts {
        conn.execute(
            "UPDATE webhook_outbox SET status = 'failed', attempts = attempts + 1, last_error = ? WHERE id = ?",
            params![error, id],
        )?;
    } else {
        let delay = std::cmp::min(
            3600,
            15 * 2_i64.pow(std::cmp::min((attempts + 1) as u32, 7)),
        );
        let next = (Utc::now() + Duration::seconds(delay)).to_rfc3339();
        conn.execute(
            "UPDATE webhook_outbox SET attempts = attempts + 1, last_error = ?, next_attempt_at = ? WHERE id = ?",
            params![error, next, id],
        )?;
    }
    Ok(())
}

pub fn insert_evidence(
    db: &Db,
    stream_id: &str,
    detection_id: Option<&str>,
    bucket: &str,
    object_key: &str,
    content_type: &str,
    size_bytes: i64,
    duration_seconds: Option<f64>,
) -> Result<String> {
    let id = format!("ev_{}", uuid::Uuid::new_v4().simple());
    let conn = db.lock().unwrap();
    conn.execute(
        "INSERT INTO evidence
         (id,stream_id,detection_id,storage,bucket,object_key,content_type,size_bytes,duration_seconds,status,created_at)
         VALUES (?,?,?, 's3', ?,?,?,?,?, 'uploaded', ?)",
        params![
            id,
            stream_id,
            detection_id,
            bucket,
            object_key,
            content_type,
            size_bytes,
            duration_seconds,
            now()
        ],
    )?;
    Ok(id)
}

pub fn get_evidence(db: &Db, id: &str) -> Result<Option<serde_json::Value>> {
    let conn = db.lock().unwrap();
    Ok(conn
        .query_row(
            "SELECT id,bucket,object_key,content_type,size_bytes,duration_seconds FROM evidence WHERE id = ?",
            params![id],
            |row| {
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "bucket": row.get::<_, String>(1)?,
                    "key": row.get::<_, String>(2)?,
                    "content_type": row.get::<_, String>(3)?,
                    "size_bytes": row.get::<_, Option<i64>>(4)?,
                    "duration_seconds": row.get::<_, Option<f64>>(5)?,
                }))
            },
        )
        .optional()?)
}

pub fn get_job(db: &Db, id: &str) -> Result<Option<serde_json::Value>> {
    let conn = db.lock().unwrap();
    Ok(conn
        .query_row(
            "SELECT id,kind,status,attempts,error,created_at,updated_at FROM jobs WHERE id = ?",
            params![id],
            |row| {
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "kind": row.get::<_, String>(1)?,
                    "status": row.get::<_, String>(2)?,
                    "attempts": row.get::<_, i64>(3)?,
                    "error": row.get::<_, Option<String>>(4)?,
                    "created_at": row.get::<_, String>(5)?,
                    "updated_at": row.get::<_, String>(6)?,
                }))
            },
        )
        .optional()?)
}

pub fn delete_catalog_item(db: &Db, id: &str) -> Result<bool> {
    let conn = db.lock().unwrap();
    Ok(conn.execute("DELETE FROM catalog_items WHERE id = ?", params![id])? > 0)
}

pub fn counts(db: &Db) -> Result<(i64, i64, i64)> {
    let conn = db.lock().unwrap();
    let streams: i64 = conn.query_row("SELECT COUNT(*) FROM streams", [], |row| row.get(0))?;
    let jobs: i64 = conn.query_row(
        "SELECT COUNT(*) FROM jobs WHERE status IN ('pending','running')",
        [],
        |row| row.get(0),
    )?;
    let webhooks: i64 = conn.query_row(
        "SELECT COUNT(*) FROM webhook_outbox WHERE status = 'pending'",
        [],
        |row| row.get(0),
    )?;
    Ok((streams, jobs, webhooks))
}
