mod shutdown;

use std::fs;
use std::future::Future;
use std::io::Write;
use std::sync::Arc;

use anyhow::Result;
use axum::body::{self, BoxBody, Bytes};
use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::Response;
use axum::{Extension, Json, Router};
use log::{error, warn};
use serde::Serialize;
use serde_json::json;
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio;
use tokio::sync;
use tower::util::ServiceExt as _;
use tower_http::services::ServeFile;

struct Writer {
    inner: zstd::Encoder<'static, fs::File>,
    name: String,
}

#[derive(Clone)]
struct Output {
    out: Arc<sync::Mutex<Option<Writer>>>,
}

fn finish(writer: &mut Option<Writer>) -> std::io::Result<()> {
    match writer.take() {
        Some(writer) => writer.inner.finish()?.flush()?,
        None => unimplemented!("already shutdown"),
    };
    Ok(())
}

#[derive(Serialize)]
struct FileListing {
    name: String,
    compressed_size_estimate: u64,
    live: bool,
}

fn parse_date(date: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(date, &Rfc3339).ok()
}

async fn list_files(Extension(state): Extension<Output>) -> (StatusCode, Json<Value>) {
    let live_name = state
        .out
        .lock()
        .await
        .as_ref()
        .map(|v| v.name.to_string())
        .unwrap_or(String::new());
    let mut items = Vec::new();
    okay_or_500(|| async {
        for f in fs::read_dir(".")? {
            let f = f?;

            let val = match f.file_name().to_str() {
                Some(val) => val.to_string(),
                None => continue,
            };

            let ext = ".events.zstd";
            if !val.ends_with(ext) {
                continue;
            }

            let live = val == live_name;

            let val = &val[..val.len() - ext.len()];
            if parse_date(val).is_none() {
                continue;
            }

            let compressed_size_estimate = f.metadata()?.len();

            items.push(FileListing {
                name: val.to_string(),
                compressed_size_estimate,
                live,
            });
        }
        items.sort_by_key(|v| v.name.to_string());

        Ok(json! { items })
    })
    .await
}

async fn fetch_raw(Path(name): Path<String>) -> Response {
    if parse_date(&name).is_none() {
        return empty_status_response(StatusCode::BAD_REQUEST);
    }

    let file_name = format!("{}.events.zstd", name);
    match ServeFile::new_with_mime(
        file_name,
        &"application/zstd".parse().expect("static mime type"),
    )
    .oneshot(axum::http::Request::new(body::Body::empty()))
    .await
    {
        //     extra: Header::new(
        //         "Content-Disposition",
        //         format!("attachment; filename=\"{}\"", file_name),
        //     ),
        Ok(res) => res.map(body::boxed),
        Err(e) => {
            warn!("unable to serve file: {:?}", e);
            empty_status_response(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

fn empty_status_response(status_code: StatusCode) -> Response {
    Response::builder()
        .status(status_code)
        .body(BoxBody::default())
        .expect("static response")
}

async fn cycle(Extension(state): Extension<Output>) -> (StatusCode, Json<Value>) {
    okay_or_500(|| async {
        let mut previous = state.out.lock().await.replace(new_file()?);

        finish(&mut previous)?;
        Ok(json!({}))
    })
    .await
}

async fn okay_or_500<F: Future<Output = Result<Value>>>(
    func: impl FnOnce() -> F,
) -> (StatusCode, Json<Value>) {
    match func().await {
        Ok(resp) => (StatusCode::OK, Json(resp)),
        Err(e) => {
            error!("handling request: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "internal server error "})),
            )
        }
    }
}

async fn store(buf: Bytes, Extension(state): Extension<Output>) -> (StatusCode, Json<Value>) {
    if buf.len() > 4 * 1024 * 1024 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "too long" })),
        );
    }
    let item_length = u64::try_from(8 + 8 + buf.len()).expect("4MB < 2^64 bytes");
    let now = OffsetDateTime::now_utc().unix_timestamp();

    okay_or_500(|| async {
        match state.out.lock().await.as_mut() {
            Some(file) => {
                file.inner.write_all(&item_length.to_le_bytes())?;
                file.inner.write_all(&now.to_le_bytes())?;
                file.inner.write_all(&buf)?;
                Ok(json!({"buffered": true}))
            }
            None => unimplemented!("already shut down?"),
        }
    })
    .await
}

fn path_for_now() -> String {
    let time = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .expect("static formatter");
    format!("{}.events.zstd", time)
}

fn new_file() -> std::io::Result<Writer> {
    let name = path_for_now();
    Ok(Writer {
        inner: zstd::Encoder::new(fs::File::create(&name)?, 3)?,
        name,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let rc = Arc::new(sync::Mutex::new(Some(new_file()?)));
    let state = Output {
        out: Arc::clone(&rc),
    };

    use axum::routing::{get, post};
    let app = Router::new()
        .route("/store", post(store))
        .route("/api/raw", get(list_files))
        .route("/api/raw/:name", get(fetch_raw))
        .route("/api/cycle", post(cycle))
        .layer(Extension(state));

    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .with_graceful_shutdown(shutdown::shutdown_signal())
        .await?;

    let mut guard = rc.lock().await;
    finish(&mut guard)?;

    Ok(())
}
