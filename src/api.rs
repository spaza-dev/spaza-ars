use crate::auth;
use crate::config::Config;
use crate::db::{self, Db};
use crate::engine::OlafEngine;
use crate::models::*;
use axum::extract::{Multipart, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub db: Db,
    pub engine: OlafEngine,
}

#[derive(OpenApi)]
#[openapi(info(title = "Airplay Recognition Service (ARS)", version = "0.1.0"))]
struct ApiDoc;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route("/version", get(version))
        .route("/v1/streams", get(list_streams).post(create_stream))
        .route(
            "/v1/streams/{id}",
            get(get_stream).patch(update_stream).delete(delete_stream),
        )
        .route("/v1/streams/{id}/start", post(start_stream))
        .route("/v1/streams/{id}/stop", post(stop_stream))
        .route("/v1/callbacks", get(list_callbacks).post(create_callback))
        .route(
            "/v1/callbacks/{id}",
            get(get_callback)
                .patch(update_callback)
                .delete(delete_callback),
        )
        .route(
            "/v1/catalog/items",
            get(list_catalog_items).post(index_catalog_item),
        )
        .route("/v1/catalog/items/json", post(index_catalog_item_json))
        .route(
            "/v1/catalog/items/{id}",
            get(get_catalog_item).delete(delete_catalog_item),
        )
        .route("/v1/query", post(query_audio))
        .route("/v1/jobs/{id}", get(get_job))
        .route("/v1/detections", get(list_detections))
        .route("/v1/detections/aggregate", get(list_aggregates))
        .route("/v1/webhooks/test", post(test_webhook))
        .merge(SwaggerUi::new("/docs").url("/openapi.json", ApiDoc::openapi()))
        .with_state(state)
}

type ApiError = (StatusCode, Json<Value>);

fn err(status: StatusCode, message: impl Into<String>) -> ApiError {
    (status, Json(json!({ "error": message.into() })))
}

fn authorize(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    if auth::is_authorized(headers, &state.config.server.api_token) {
        Ok(())
    } else {
        Err(err(StatusCode::UNAUTHORIZED, "invalid bearer token"))
    }
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let (streams, jobs, webhooks) = db::counts(&state.db).unwrap_or((0, 0, 0));
    Json(Health {
        ok: true,
        status: "ok".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        streams,
        pending_jobs: jobs,
        pending_webhooks: webhooks,
    })
}

async fn ready(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    if !state.config.server.health_public {
        authorize(&state, &headers)?;
    }
    let olaf_ok = state.engine.health().await;
    let (streams, jobs, webhooks) = db::counts(&state.db).unwrap_or((0, 0, 0));
    let status = if olaf_ok { "ready" } else { "degraded" };
    Ok(Json(json!({
        "ok": olaf_ok,
        "status": status,
        "olaf": olaf_ok,
        "streams": streams,
        "pending_jobs": jobs,
        "pending_webhooks": webhooks,
    })))
}

async fn version() -> impl IntoResponse {
    Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "contract_version": "1",
    }))
}

async fn list_streams(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse<Vec<Stream>>>, ApiError> {
    authorize(&state, &headers)?;
    let streams = db::list_streams(&state.db)
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ApiResponse::ok(streams)))
}

async fn create_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<CreateStream>,
) -> Result<(StatusCode, Json<ApiResponse<Stream>>), ApiError> {
    authorize(&state, &headers)?;
    let stream = db::create_stream(&state.db, input)
        .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok((StatusCode::CREATED, Json(ApiResponse::ok(stream))))
}

async fn get_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<Stream>>, ApiError> {
    authorize(&state, &headers)?;
    match db::get_stream(&state.db, &id)
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    {
        Some(stream) => Ok(Json(ApiResponse::ok(stream))),
        None => Err(err(StatusCode::NOT_FOUND, "stream not found")),
    }
}

async fn update_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<UpdateStream>,
) -> Result<Json<ApiResponse<Stream>>, ApiError> {
    authorize(&state, &headers)?;
    match db::update_stream(&state.db, &id, input)
        .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?
    {
        Some(stream) => Ok(Json(ApiResponse::ok(stream))),
        None => Err(err(StatusCode::NOT_FOUND, "stream not found")),
    }
}

async fn delete_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    authorize(&state, &headers)?;
    let deleted = db::delete_stream(&state.db, &id)
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if deleted {
        Ok(Json(ApiResponse::ok(json!({ "deleted": true }))))
    } else {
        Err(err(StatusCode::NOT_FOUND, "stream not found"))
    }
}

async fn start_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    authorize(&state, &headers)?;
    db::set_stream_status(&state.db, &id, "starting", None)
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ApiResponse::ok(json!({ "status": "starting" }))))
}

async fn stop_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    authorize(&state, &headers)?;
    let input = UpdateStream {
        name: None,
        url: None,
        source_type: None,
        external_id: None,
        catalogs: None,
        enabled: Some(false),
        evidence_enabled: None,
        evidence_pre_seconds: None,
        evidence_post_seconds: None,
        callback_id: None,
        chunk_seconds: None,
        overlap_seconds: None,
        aggregation_window_seconds: None,
    };
    db::update_stream(&state.db, &id, input)
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ApiResponse::ok(json!({ "status": "stopped" }))))
}

async fn list_callbacks(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse<Vec<Callback>>>, ApiError> {
    authorize(&state, &headers)?;
    let callbacks = db::list_callbacks(&state.db)
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ApiResponse::ok(callbacks)))
}

async fn create_callback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<CreateCallback>,
) -> Result<(StatusCode, Json<ApiResponse<Callback>>), ApiError> {
    authorize(&state, &headers)?;
    let callback = db::create_callback(&state.db, input)
        .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok((StatusCode::CREATED, Json(ApiResponse::ok(callback))))
}

async fn get_callback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<Callback>>, ApiError> {
    authorize(&state, &headers)?;
    match db::get_callback(&state.db, &id)
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    {
        Some(callback) => Ok(Json(ApiResponse::ok(callback))),
        None => Err(err(StatusCode::NOT_FOUND, "callback not found")),
    }
}

async fn update_callback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<UpdateCallback>,
) -> Result<Json<ApiResponse<Callback>>, ApiError> {
    authorize(&state, &headers)?;
    match db::update_callback(&state.db, &id, input)
        .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?
    {
        Some(callback) => Ok(Json(ApiResponse::ok(callback))),
        None => Err(err(StatusCode::NOT_FOUND, "callback not found")),
    }
}

async fn delete_callback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    authorize(&state, &headers)?;
    let deleted = db::delete_callback(&state.db, &id)
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if deleted {
        Ok(Json(ApiResponse::ok(json!({ "deleted": true }))))
    } else {
        Err(err(StatusCode::NOT_FOUND, "callback not found"))
    }
}

#[derive(Deserialize)]
struct CatalogQuery {
    catalog: Option<String>,
}

async fn list_catalog_items(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<CatalogQuery>,
) -> Result<Json<ApiResponse<Vec<CatalogItem>>>, ApiError> {
    authorize(&state, &headers)?;
    let items = db::list_catalog_items(&state.db, query.catalog.as_deref())
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ApiResponse::ok(items)))
}

async fn get_catalog_item(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<CatalogItem>>, ApiError> {
    authorize(&state, &headers)?;
    match db::get_catalog_item(&state.db, &id)
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    {
        Some(item) => Ok(Json(ApiResponse::ok(item))),
        None => Err(err(StatusCode::NOT_FOUND, "catalog item not found")),
    }
}

async fn index_catalog_item(
    State(state): State<AppState>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<ApiResponse<JobAccepted>>), ApiError> {
    authorize(&state, &headers)?;
    let mut catalog = "private".to_string();
    let mut external_id: Option<String> = None;
    let mut name = String::new();
    let mut item_type = "song".to_string();
    let mut metadata = json!({});
    let mut file_bytes: Option<Vec<u8>> = None;
    let mut url: Option<String> = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?
    {
        let field_name = field.name().unwrap_or("").to_string();
        match field_name.as_str() {
            "file" => {
                file_bytes = Some(
                    field
                        .bytes()
                        .await
                        .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?
                        .to_vec(),
                );
            }
            "url" => {
                url = Some(
                    field
                        .text()
                        .await
                        .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?,
                )
            }
            "catalog" => catalog = field.text().await.unwrap_or_else(|_| "private".into()),
            "external_id" => external_id = Some(field.text().await.unwrap_or_default()),
            "name" => name = field.text().await.unwrap_or_default(),
            "type" => item_type = field.text().await.unwrap_or_else(|_| "song".into()),
            "metadata" => {
                let text = field.text().await.unwrap_or_else(|_| "{}".into());
                metadata = serde_json::from_str(&text).unwrap_or_else(|_| json!({}));
            }
            _ => {}
        }
    }

    if name.is_empty() {
        return Err(err(StatusCode::BAD_REQUEST, "name is required"));
    }
    if file_bytes.is_none() && url.is_none() {
        return Err(err(StatusCode::BAD_REQUEST, "provide a file or url"));
    }

    let fingerprint_id = format!("cat_{}", uuid::Uuid::new_v4().simple());
    let item = db::create_catalog_item(
        &state.db,
        &catalog,
        external_id.as_deref(),
        &name,
        &item_type,
        &metadata,
        &fingerprint_id,
    )
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let payload = if let Some(bytes) = file_bytes {
        let dir = format!("{}/index", state.config.capture.temp_dir);
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let path = format!("{}/{}.bin", dir, item.id);
        tokio::fs::write(&path, bytes)
            .await
            .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        json!({ "catalog_item_id": item.id, "path": path })
    } else {
        json!({ "catalog_item_id": item.id, "url": url })
    };
    let job_id = db::enqueue_job(&state.db, "index", &payload, 0)
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let status_url = format!("{}/v1/jobs/{}", state.config.server.public_base_url, job_id);
    Ok((
        StatusCode::ACCEPTED,
        Json(ApiResponse::ok(JobAccepted {
            job_id,
            status: "accepted".into(),
            status_url,
        })),
    ))
}

async fn index_catalog_item_json(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<IndexItemJson>,
) -> Result<(StatusCode, Json<ApiResponse<JobAccepted>>), ApiError> {
    authorize(&state, &headers)?;
    let fingerprint_id = format!("cat_{}", uuid::Uuid::new_v4().simple());
    let metadata = input.metadata.unwrap_or_else(|| json!({}));
    let item = db::create_catalog_item(
        &state.db,
        input.catalog.as_deref().unwrap_or("private"),
        input.external_id.as_deref(),
        &input.name,
        input.item_type.as_deref().unwrap_or("song"),
        &metadata,
        &fingerprint_id,
    )
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let job_id = db::enqueue_job(
        &state.db,
        "index",
        &json!({ "catalog_item_id": item.id, "url": input.url }),
        0,
    )
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let status_url = format!("{}/v1/jobs/{}", state.config.server.public_base_url, job_id);
    Ok((
        StatusCode::ACCEPTED,
        Json(ApiResponse::ok(JobAccepted {
            job_id,
            status: "accepted".into(),
            status_url,
        })),
    ))
}

async fn delete_catalog_item(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    authorize(&state, &headers)?;
    let deleted = db::delete_catalog_item(&state.db, &id)
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if deleted {
        Ok(Json(ApiResponse::ok(json!({
            "deleted": true,
            "note": "Olaf fingerprint deletion is not performed by this build"
        }))))
    } else {
        Err(err(StatusCode::NOT_FOUND, "catalog item not found"))
    }
}

async fn query_audio(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<QueryRequest>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    authorize(&state, &headers)?;
    let url = input
        .url
        .or(input.audio_url)
        .ok_or_else(|| err(StatusCode::BAD_REQUEST, "url is required"))?;
    let response = reqwest::get(&url)
        .await
        .map_err(|e| err(StatusCode::BAD_GATEWAY, e.to_string()))?;
    if !response.status().is_success() {
        return Err(err(
            StatusCode::BAD_GATEWAY,
            format!("download failed: {}", response.status()),
        ));
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|e| err(StatusCode::BAD_GATEWAY, e.to_string()))?
        .to_vec();
    let matches = state
        .engine
        .query(bytes, true)
        .await
        .map_err(|e| err(StatusCode::BAD_GATEWAY, e.to_string()))?;
    Ok(Json(ApiResponse::ok(json!({ "matches": matches }))))
}

async fn get_job(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    authorize(&state, &headers)?;
    match db::get_job(&state.db, &id)
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    {
        Some(job) => Ok(Json(ApiResponse::ok(job))),
        None => Err(err(StatusCode::NOT_FOUND, "job not found")),
    }
}

#[derive(Deserialize)]
struct DetectionQuery {
    stream_id: Option<String>,
}

async fn list_detections(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<DetectionQuery>,
) -> Result<Json<ApiResponse<Vec<Detection>>>, ApiError> {
    authorize(&state, &headers)?;
    let detections = db::list_detections(&state.db, query.stream_id.as_deref())
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ApiResponse::ok(detections)))
}

async fn list_aggregates(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<CatalogQuery>,
) -> Result<Json<ApiResponse<Vec<DetectionAggregate>>>, ApiError> {
    authorize(&state, &headers)?;
    let aggregates = db::list_aggregates(&state.db, query.catalog.as_deref())
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ApiResponse::ok(aggregates)))
}

async fn test_webhook(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> Result<Json<ApiResponse<Value>>, ApiError> {
    authorize(&state, &headers)?;
    let callback_id = input
        .get("callback_id")
        .and_then(Value::as_str)
        .ok_or_else(|| err(StatusCode::BAD_REQUEST, "callback_id is required"))?;
    let payload = json!({
        "event": "webhook.test",
        "time": db::now(),
        "message": "ARS test webhook"
    });
    db::enqueue_webhook(
        &state.db,
        callback_id,
        "webhook.test",
        &payload,
        &format!("test:{}", uuid::Uuid::new_v4()),
    )
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ApiResponse::ok(json!({ "queued": true }))))
}
