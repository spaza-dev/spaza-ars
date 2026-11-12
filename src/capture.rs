use crate::config::Config;
use crate::db::{self, Db};
use crate::models::Stream;
use anyhow::{anyhow, Result};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::process::Command;
use tokio::time::{interval, sleep, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

pub type RunningStreams = Arc<Mutex<HashMap<String, CancellationToken>>>;

pub async fn run_supervisor(config: Config, db: Db) -> Result<()> {
    let running: RunningStreams = Arc::new(Mutex::new(HashMap::new()));
    let mut ticker = interval(Duration::from_secs(5));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        let streams = match db::enabled_streams(&db) {
            Ok(streams) => streams,
            Err(error) => {
                tracing::warn!(%error, "failed to load enabled streams");
                continue;
            }
        };

        if streams.len() > config.capture.max_streams {
            tracing::warn!(
                enabled = streams.len(),
                max = config.capture.max_streams,
                "enabled streams exceed max_streams; extra streams will not be started"
            );
        }

        let allowed: HashSet<String> = streams
            .iter()
            .take(config.capture.max_streams)
            .map(|stream| stream.id.clone())
            .collect();

        {
            let mut map = running.lock().unwrap();
            let stale: Vec<String> = map
                .keys()
                .filter(|id| !allowed.contains(*id))
                .cloned()
                .collect();
            for id in stale {
                if let Some(token) = map.remove(&id) {
                    token.cancel();
                }
            }
        }

        for stream in streams.into_iter().take(config.capture.max_streams) {
            let should_start = {
                let map = running.lock().unwrap();
                !map.contains_key(&stream.id)
            };
            if !should_start {
                continue;
            }

            let token = CancellationToken::new();
            {
                let mut map = running.lock().unwrap();
                map.insert(stream.id.clone(), token.clone());
            }

            let task_config = config.clone();
            let task_db = db.clone();
            let task_stream = stream.clone();
            let task_running = running.clone();
            tokio::spawn(async move {
                let result = capture_loop(
                    task_config,
                    task_db.clone(),
                    task_stream.clone(),
                    token.clone(),
                )
                .await;
                if let Err(error) = &result {
                    tracing::error!(stream_id = %task_stream.id, %error, "capture loop failed");
                    let _ = db::set_stream_status(
                        &task_db,
                        &task_stream.id,
                        "error",
                        Some(&error.to_string()),
                    );
                }
                let mut map = task_running.lock().unwrap();
                map.remove(&task_stream.id);
            });
        }
    }
}

async fn capture_loop(
    config: Config,
    db: Db,
    stream: Stream,
    token: CancellationToken,
) -> Result<()> {
    let stream_dir = format!("{}/{}", config.capture.temp_dir, stream.id);
    tokio::fs::create_dir_all(&stream_dir).await?;

    loop {
        if token.is_cancelled() {
            let _ = db::set_stream_status(&db, &stream.id, "stopped", None);
            return Ok(());
        }

        let current = match db::get_stream(&db, &stream.id)? {
            Some(value) => value,
            None => return Ok(()),
        };
        if current.enabled == 0 {
            let _ = db::set_stream_status(&db, &stream.id, "stopped", None);
            return Ok(());
        }

        let source = match resolve_source(&config, &current).await {
            Ok(source) => source,
            Err(error) => {
                let message = format!("source resolve failed: {}", error);
                db::set_stream_status(&db, &stream.id, "error", Some(&message))?;
                sleep(Duration::from_secs(config.capture.restart_backoff_seconds)).await;
                continue;
            }
        };

        db::set_stream_status(&db, &stream.id, "running", None)?;
        tracing::info!(stream_id = %stream.id, url = %current.url, "starting capture");

        let pattern = format!("{}/chunk-%Y%m%d-%H%M%S.wav", stream_dir);
        let mut command = Command::new(&config.capture.ffmpeg);
        command
            .arg("-hide_banner")
            .arg("-loglevel")
            .arg("warning")
            .arg("-i")
            .arg(&source)
            .arg("-vn")
            .arg("-ac")
            .arg("1")
            .arg("-ar")
            .arg("16000")
            .arg("-f")
            .arg("segment")
            .arg("-segment_time")
            .arg(current.chunk_seconds.max(3).to_string())
            .arg("-reset_timestamps")
            .arg("1")
            .arg("-strftime")
            .arg("1")
            .arg(&pattern)
            .kill_on_drop(true);

        let mut child = command.spawn()?;
        let mut monitor = interval(Duration::from_secs(1));
        monitor.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut queued: HashSet<String> = HashSet::new();

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    let _ = child.kill().await;
                    let _ = db::set_stream_status(&db, &stream.id, "stopped", None);
                    return Ok(());
                }
                status = child.wait() => {
                    let message = match status {
                        Ok(status) => format!("ffmpeg exited with {}", status),
                        Err(error) => format!("ffmpeg wait failed: {}", error),
                    };
                    let _ = db::set_stream_status(&db, &stream.id, "error", Some(&message));
                    sleep(Duration::from_secs(config.capture.restart_backoff_seconds)).await;
                    break;
                }
                _ = monitor.tick() => {
                    queue_completed_chunks(&config, &db, &current, &stream_dir, &mut queued).await?;
                }
            }
        }
    }
}

async fn queue_completed_chunks(
    config: &Config,
    db: &Db,
    stream: &Stream,
    dir: &str,
    queued: &mut HashSet<String>,
) -> Result<()> {
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(_) => return Ok(()),
    };
    let now = std::time::SystemTime::now();
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("wav") {
            continue;
        }
        let key = path.to_string_lossy().to_string();
        if queued.contains(&key) {
            continue;
        }
        let metadata = match entry.metadata().await {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };
        let age = metadata
            .modified()
            .ok()
            .and_then(|modified| now.duration_since(modified).ok())
            .unwrap_or_default();
        if age < Duration::from_secs(2) {
            continue;
        }
        queued.insert(key.clone());
        let payload = serde_json::json!({
            "stream_id": stream.id,
            "path": key,
            "catalogs": stream.catalogs,
            "evidence_enabled": stream.evidence_enabled != 0,
            "evidence_pre_seconds": stream.evidence_pre_seconds,
            "evidence_post_seconds": stream.evidence_post_seconds,
        });
        db::enqueue_job(db, "query", &payload, 0)?;
        db::mark_stream_audio(db, &stream.id)?;
        tracing::debug!(stream_id = %stream.id, file = %path.display(), "queued chunk query");
    }
    let _ = config;
    Ok(())
}

async fn resolve_source(config: &Config, stream: &Stream) -> Result<String> {
    let kind = stream.source_type.to_lowercase();
    if kind == "youtube" || kind == "youtube_ch" || kind == "twitch" {
        let output = Command::new(&config.capture.yt_dlp)
            .arg("--no-playlist")
            .arg("-f")
            .arg("bestaudio/best")
            .arg("-g")
            .arg(&stream.url)
            .output()
            .await?;
        if !output.status.success() {
            return Err(anyhow!(
                "yt-dlp failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        let url = String::from_utf8_lossy(&output.stdout)
            .lines()
            .next()
            .unwrap_or("")
            .trim()
            .to_string();
        if url.is_empty() {
            return Err(anyhow!("yt-dlp returned no stream URL"));
        }
        Ok(url)
    } else {
        Ok(stream.url.clone())
    }
}
