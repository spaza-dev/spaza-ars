mod api;
mod auth;
mod capture;
mod config;
mod db;
mod engine;
mod evidence;
mod models;
mod webhook;
mod workers;

use anyhow::Result;
use config::Config;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    let config_path = std::env::args()
        .position(|arg| arg == "--config")
        .and_then(|index| std::env::args().nth(index + 1));
    let config = Config::load(config_path.as_deref())?;

    let db = db::open(&format!("{}/ars.db", config.olaf.db_path))?;
    db::migrate(&db, include_str!("../migrations/0001_init.sql"))?;
    db::apply_migration(
        &db,
        2,
        include_str!("../migrations/0002_catalog_idempotency.sql"),
    )?;
    db::apply_migration(
        &db,
        3,
        include_str!("../migrations/0003_outbox_updated_at.sql"),
    )?;
    db::apply_migration(
        &db,
        4,
        include_str!("../migrations/0004_stream_callback_mode.sql"),
    )?;
    db::apply_migration(
        &db,
        5,
        include_str!("../migrations/0005_query_scheduling.sql"),
    )?;

    let reset_queries = db::reset_running_jobs(&db, "query")?;
    if reset_queries > 0 {
        tracing::info!(count = reset_queries, "requeued stale running query jobs");
    }
    let reset_index = db::reset_running_jobs(&db, "index")?;
    if reset_index > 0 {
        tracing::info!(count = reset_index, "requeued stale running index jobs");
    }

    let engine = engine::OlafEngine::new(&config.olaf)?;
    let state = api::AppState {
        config: config.clone(),
        db: db.clone(),
        engine: engine.clone(),
    };

    tokio::spawn(capture::run_supervisor(config.clone(), db.clone()));
    tokio::spawn(workers::run_query_supervisor(
        config.clone(),
        db.clone(),
        engine.clone(),
    ));
    for _ in 0..config.olaf.store_concurrency.max(1) {
        tokio::spawn(workers::run_index_worker(
            config.clone(),
            db.clone(),
            engine.clone(),
        ));
    }
    tokio::spawn(workers::run_aggregator(config.clone(), db.clone()));
    tokio::spawn(workers::run_cleanup(db.clone()));
    tokio::spawn(workers::run_webhook_worker(config.clone(), db.clone()));

    let app = api::router(state);
    let listener = tokio::net::TcpListener::bind(&config.server.bind).await?;
    tracing::info!(bind = %config.server.bind, "ars listening");
    axum::serve(listener, app).await?;
    Ok(())
}
