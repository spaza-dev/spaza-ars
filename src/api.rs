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
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::openapi::Server;
use utoipa::OpenApi;
use utoipa_swagger_ui::{Config as SwaggerConfig, SwaggerUi};

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub db: Db,
    pub engine: OlafEngine,
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Airplay Recognition Service (ARS)",
        version = env!("CARGO_PKG_VERSION"),
        description = "Stream monitoring, Olaf catalog indexing, detection aggregation, evidence and authenticated webhooks."
    ),
    tags(
        (name = "Health", description = "Liveness and readiness"),
        (name = "Streams", description = "Stream lifecycle and capture control"),
        (name = "Callbacks", description = "Webhook callback configuration"),
        (name = "Catalog", description = "Catalog indexing"),
        (name = "Query", description = "Ad hoc audio query"),
        (name = "Jobs", description = "Async job status"),
        (name = "Detections", description = "Detections and aggregates"),
        (name = "Webhooks", description = "Webhook testing")
    ),
    components(schemas(
        Stream, CreateStream, UpdateStream,
        Callback, CreateCallback, UpdateCallback,
        CatalogItem, IndexItemJson, IndexItemMultipart,
        Detection, DetectionAggregate,
        QueryRequest, QueryResponse,
        JobAccepted, JobStatus,
        Health, ReadyResponse, VersionResponse,
        ErrorResponse, DeleteResponse, StatusResponse,
        TestWebhookRequest, TestWebhookResponse, TestWebhookData,
        StreamResponse, StreamListResponse,
        CallbackResponse, CallbackListResponse,
        CatalogItemResponse, CatalogItemListResponse,
        DetectionListResponse, AggregateListResponse,
        JobAcceptedResponse, JobStatusResponse
    )),
    paths(
        health, ready, version,
        list_streams, create_stream, get_stream, update_stream, delete_stream,
        start_stream, stop_stream,
        list_callbacks, create_callback, get_callback, update_callback, delete_callback,
        list_catalog_items, get_catalog_item, index_catalog_item,
        index_catalog_item_json, delete_catalog_item,
        query_audio, get_job, list_detections, list_aggregates, test_webhook
    )
)]
struct ApiDoc;

pub fn build_openapi(config: &Config) -> utoipa::openapi::OpenApi {
    let mut openapi = ApiDoc::openapi();
    openapi.servers = Some(vec![Server::new(config.server.public_base_url.clone())]);
    if let Some(components) = openapi.components.as_mut() {
        components.security_schemes.insert(
            "bearer_auth".into(),
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("opaque")
                    .build(),
            ),
        );
    }
    openapi
}

pub fn router(state: AppState) -> Router {
    let openapi = build_openapi(&state.config);
    let swagger = SwaggerUi::new("/docs")
        .url("/openapi.json", openapi)
        .config(
            SwaggerConfig::default()
                .persist_authorization(true)
                .try_it_out_enabled(true)
                .display_request_duration(true)
                .filter(true),
        );

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
        .merge(swagger)
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

#[utoipa::path(
    get,
    path = "/healthz",
    tag = "Health",
    responses((status = 200, description = "Service health", body = Health))
)]
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

#[utoipa::path(
    get,
    path = "/readyz",
    tag = "Health",
    responses(
        (status = 200, description = "Readiness status", body = ReadyResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    get,
    path = "/version",
    tag = "Health",
    responses((status = 200, description = "Version information", body = VersionResponse))
)]
async fn version() -> impl IntoResponse {
    Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "contract_version": "1",
    }))
}

#[utoipa::path(
    get,
    path = "/v1/streams",
    tag = "Streams",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Streams returned", body = StreamListResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse)
    )
)]
async fn list_streams(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse<Vec<Stream>>>, ApiError> {
    authorize(&state, &headers)?;
    let streams = db::list_streams(&state.db)
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ApiResponse::ok(streams)))
}

#[utoipa::path(
    post,
    path = "/v1/streams",
    tag = "Streams",
    security(("bearer_auth" = [])),
    request_body(
        content = CreateStream,
        description = "Create a monitored stream.",
        example = json!({
            "name": "Radio One",
            "url": "https://example.com/live.mp3",
            "source_type": "radio",
            "external_id": "12345",
            "catalogs": ["private"],
            "enabled": true,
            "evidence_enabled": false,
            "chunk_seconds": 10,
            "overlap_seconds": 2,
            "aggregation_window_seconds": 60
        })
    ),
    responses(
        (status = 201, description = "Stream created", body = StreamResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    get,
    path = "/v1/streams/{id}",
    tag = "Streams",
    security(("bearer_auth" = [])),
    params(("id" = String, Path, description = "Stream ID", example = "str_123")),
    responses(
        (status = 200, description = "Stream returned", body = StreamResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Stream not found", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    patch,
    path = "/v1/streams/{id}",
    tag = "Streams",
    security(("bearer_auth" = [])),
    params(("id" = String, Path, description = "Stream ID", example = "str_123")),
    request_body(
        content = UpdateStream,
        description = "Update stream fields. Only provided fields are changed.",
        example = json!({
            "url": "https://example.com/new-stream.mp3",
            "catalogs": ["private"],
            "enabled": true,
            "aggregation_window_seconds": 30
        })
    ),
    responses(
        (status = 200, description = "Stream updated", body = StreamResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Stream not found", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    delete,
    path = "/v1/streams/{id}",
    tag = "Streams",
    security(("bearer_auth" = [])),
    params(("id" = String, Path, description = "Stream ID", example = "str_123")),
    responses(
        (status = 200, description = "Stream deleted", body = DeleteResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Stream not found", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    post,
    path = "/v1/streams/{id}/start",
    tag = "Streams",
    security(("bearer_auth" = [])),
    params(("id" = String, Path, description = "Stream ID", example = "str_123")),
    responses(
        (status = 200, description = "Stream starting", body = StatusResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    post,
    path = "/v1/streams/{id}/stop",
    tag = "Streams",
    security(("bearer_auth" = [])),
    params(("id" = String, Path, description = "Stream ID", example = "str_123")),
    responses(
        (status = 200, description = "Stream stopped", body = StatusResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    get,
    path = "/v1/callbacks",
    tag = "Callbacks",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Callbacks returned", body = CallbackListResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse)
    )
)]
async fn list_callbacks(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse<Vec<Callback>>>, ApiError> {
    authorize(&state, &headers)?;
    let callbacks = db::list_callbacks(&state.db)
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ApiResponse::ok(callbacks)))
}

#[utoipa::path(
    post,
    path = "/v1/callbacks",
    tag = "Callbacks",
    security(("bearer_auth" = [])),
    request_body(
        content = CreateCallback,
        description = "Register an authenticated webhook callback.",
        example = json!({
            "name": "console-default",
            "url": "https://console.example.com/api/callback/secret",
            "auth_type": "both",
            "token": "callback-bearer-token",
            "signing_secret": "callback-hmac-secret",
            "event_types": ["matches.aggregated", "stream.status"],
            "enabled": true
        })
    ),
    responses(
        (status = 201, description = "Callback created", body = CallbackResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    get,
    path = "/v1/callbacks/{id}",
    tag = "Callbacks",
    security(("bearer_auth" = [])),
    params(("id" = String, Path, description = "Callback ID", example = "cb_123")),
    responses(
        (status = 200, description = "Callback returned", body = CallbackResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Callback not found", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    patch,
    path = "/v1/callbacks/{id}",
    tag = "Callbacks",
    security(("bearer_auth" = [])),
    params(("id" = String, Path, description = "Callback ID", example = "cb_123")),
    request_body(
        content = UpdateCallback,
        description = "Update callback fields. Only provided fields are changed.",
        example = json!({
            "url": "https://console.example.com/api/callback/new-secret",
            "enabled": true
        })
    ),
    responses(
        (status = 200, description = "Callback updated", body = CallbackResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Callback not found", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    delete,
    path = "/v1/callbacks/{id}",
    tag = "Callbacks",
    security(("bearer_auth" = [])),
    params(("id" = String, Path, description = "Callback ID", example = "cb_123")),
    responses(
        (status = 200, description = "Callback deleted", body = DeleteResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Callback not found", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    get,
    path = "/v1/catalog/items",
    tag = "Catalog",
    security(("bearer_auth" = [])),
    params(("catalog" = Option<String>, Query, description = "Filter by catalog", example = "private")),
    responses(
        (status = 200, description = "Catalog items returned", body = CatalogItemListResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    get,
    path = "/v1/catalog/items/{id}",
    tag = "Catalog",
    security(("bearer_auth" = [])),
    params(("id" = String, Path, description = "Catalog item ID", example = "cat_123")),
    responses(
        (status = 200, description = "Catalog item returned", body = CatalogItemResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Catalog item not found", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    post,
    path = "/v1/catalog/items",
    tag = "Catalog",
    security(("bearer_auth" = [])),
    request_body(
        content = IndexItemMultipart,
        content_type = "multipart/form-data",
        description = "Provide either a file or a URL, plus name and optional catalog metadata.",
        example = json!({
            "name": "Song A",
            "catalog": "private",
            "external_id": "12345",
            "type": "song",
            "metadata": "{\"artist\":\"Artist A\",\"album\":\"Album A\"}",
            "url": "https://example.com/song.mp3"
        })
    ),
    responses(
        (status = 202, description = "Indexing accepted", body = JobAcceptedResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    post,
    path = "/v1/catalog/items/json",
    tag = "Catalog",
    security(("bearer_auth" = [])),
    request_body(
        content = IndexItemJson,
        description = "Index an audio item from a URL.",
        example = json!({
            "catalog": "private",
            "external_id": "12345",
            "name": "Song A",
            "type": "song",
            "metadata": {"artist": "Artist A", "album": "Album A"},
            "url": "https://example.com/song.mp3"
        })
    ),
    responses(
        (status = 202, description = "Indexing accepted", body = JobAcceptedResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    delete,
    path = "/v1/catalog/items/{id}",
    tag = "Catalog",
    security(("bearer_auth" = [])),
    params(("id" = String, Path, description = "Catalog item ID", example = "cat_123")),
    responses(
        (status = 200, description = "Catalog item deleted locally", body = DeleteResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Catalog item not found", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    post,
    path = "/v1/query",
    tag = "Query",
    security(("bearer_auth" = [])),
    request_body(
        content = QueryRequest,
        description = "Query a URL or audio URL against the Olaf catalog.",
        example = json!({
            "url": "https://example.com/fragment.mp3",
            "catalog": ["private"],
            "options": {"minimum_confidence": 0.5}
        })
    ),
    responses(
        (status = 200, description = "Query matches", body = QueryResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse),
        (status = 502, description = "Olaf query failed", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    get,
    path = "/v1/jobs/{id}",
    tag = "Jobs",
    security(("bearer_auth" = [])),
    params(("id" = String, Path, description = "Job ID", example = "job_123")),
    responses(
        (status = 200, description = "Job status returned", body = JobStatusResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Job not found", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    get,
    path = "/v1/detections",
    tag = "Detections",
    security(("bearer_auth" = [])),
    params(("stream_id" = Option<String>, Query, description = "Filter by stream ID", example = "str_123")),
    responses(
        (status = 200, description = "Detections returned", body = DetectionListResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    get,
    path = "/v1/detections/aggregate",
    tag = "Detections",
    security(("bearer_auth" = [])),
    params(("stream_id" = Option<String>, Query, description = "Filter by stream ID", example = "str_123")),
    responses(
        (status = 200, description = "Detection aggregates returned", body = AggregateListResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse)
    )
)]
async fn list_aggregates(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<DetectionQuery>,
) -> Result<Json<ApiResponse<Vec<DetectionAggregate>>>, ApiError> {
    authorize(&state, &headers)?;
    let aggregates = db::list_aggregates(&state.db, query.stream_id.as_deref())
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ApiResponse::ok(aggregates)))
}

#[utoipa::path(
    post,
    path = "/v1/webhooks/test",
    tag = "Webhooks",
    security(("bearer_auth" = [])),
    request_body(
        content = TestWebhookRequest,
        description = "Queue a test webhook for a callback.",
        example = json!({"callback_id": "cb_123"})
    ),
    responses(
        (status = 200, description = "Test webhook queued", body = TestWebhookResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Invalid bearer token", body = ErrorResponse)
    )
)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openapi_documents_routes_security_and_examples() {
        let doc = build_openapi(&Config::default());
        let value = serde_json::to_value(&doc).expect("openapi serializes");

        assert_eq!(
            value["servers"][0]["url"],
            "https://airplay-ars.spaza.co.ke"
        );
        assert_eq!(
            value["components"]["securitySchemes"]["bearer_auth"]["scheme"],
            "bearer"
        );
        assert!(value["paths"]["/v1/streams"]["post"].is_object());
        assert!(value["paths"]["/v1/callbacks"]["post"].is_object());
        assert!(value["paths"]["/v1/catalog/items"]["post"].is_object());
        assert!(
            value["paths"]["/v1/streams"]["post"]["requestBody"]["content"]["application/json"]
                ["example"]
                .is_object()
        );
        assert!(
            value["paths"]["/v1/catalog/items"]["post"]["requestBody"]["content"]
                ["multipart/form-data"]["example"]
                .is_object()
        );
    }
}
