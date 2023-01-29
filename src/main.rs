mod admin;
mod shutdown;

use std::fs;
use std::future::Future;
use std::io::Write;
use std::net::Ipv6Addr;
use std::sync::Arc;

use anyhow::Result;
use archiv::{Compress, CompressOptions, CompressStream};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::{Json, Router};
use bunyarrs::{vars, vars_dbg, Bunyarr};
use serde_json::json;
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync;

use admin::*;

struct Writer {
    inner: CompressStream<'static, fs::File>,
    name: String,
}

pub struct Output {
    // None means we're in some kind of error state, either shutting down,
    // or unable to create a new file
    out: Arc<sync::Mutex<Option<Writer>>>,
    logger: Bunyarr,
}

fn finish(logger: &Bunyarr, writer: &mut Option<Writer>) -> Result<()> {
    match writer.take() {
        Some(writer) => {
            writer.inner.finish()?;
            logger.info(json!({ "file_name": writer.name }), "completed file");
        }
        None => (),
    };
    Ok(())
}

async fn okay_or_500<F: Future<Output = Result<Value>>>(
    logger: &Bunyarr,
    func: impl FnOnce() -> F,
) -> (StatusCode, Json<Value>) {
    match func().await {
        Ok(resp) => (StatusCode::OK, Json(resp)),
        Err(err) => {
            logger.error(vars_dbg!(err), "error handling request");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "internal server error "})),
            )
        }
    }
}

async fn store(State(state): State<Arc<Output>>, buf: Bytes) -> (StatusCode, Json<Value>) {
    if buf.len() > 4 * 1024 * 1024 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "too long" })),
        );
    }
    let now = OffsetDateTime::now_utc().unix_timestamp();

    okay_or_500(&state.logger, || async {
        let mut opt = state.out.lock().await;
        if opt.is_none() {
            opt.replace(new_file(&state.logger)?);
        }

        match write(
            &mut opt.as_mut().expect("just checked").inner,
            &[&now.to_le_bytes(), &buf],
        ) {
            Ok(()) => Ok(json!({"buffered": true})),
            Err(err) => {
                if let Err(err) = finish(&state.logger, &mut opt) {
                    state
                        .logger
                        .warn(vars_dbg!(err), "unable to emergency finish");
                }
                Err(err)
            }
        }
    })
    .await
}

fn write<W: Write>(file: &mut CompressStream<W>, item: &[&[u8]]) -> Result<()> {
    file.write_item_vectored(item)?;
    file.flush()?;
    Ok(())
}

fn path_for_now() -> String {
    let time = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .expect("static formatter");
    format!("{}.events.archiv", time)
}

fn new_file(logger: &Bunyarr) -> Result<Writer> {
    let file_name = path_for_now();
    let opts = CompressOptions::<'static>::default();
    let inner = opts.stream_compress(fs::File::create(&file_name)?)?;
    logger.info(vars!(file_name), "new event file created");
    Ok(Writer {
        inner,
        name: file_name,
    })
}

async fn healthcheck(State(state): State<Arc<Output>>) -> (StatusCode, Json<Value>) {
    match state.out.lock().await.as_ref() {
        Some(_) => (StatusCode::OK, Json(json!({"ok": true}))),
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"msg": "writer unavailable"})),
        ),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let logger = Bunyarr::with_name("batchy");

    let rc = Arc::new(sync::Mutex::new(Some(new_file(&logger)?)));
    let state = Output {
        out: Arc::clone(&rc),
        logger: Bunyarr::with_name("batchy-handler"),
    };

    use axum::routing::{get, post};
    let app = Router::new()
        .route("/store", post(store))
        .route("/healthcheck", get(healthcheck))
        .route("/api/raw", get(list_files))
        .route("/api/raw/:name", get(fetch_raw))
        .route("/api/cycle", post(cycle))
        .with_state(Arc::new(state));

    let port = 3000;
    logger.info(vars!(port), "server starting");
    axum::Server::bind(&(Ipv6Addr::UNSPECIFIED, port).into())
        .serve(app.into_make_service())
        .with_graceful_shutdown(shutdown::shutdown_signal())
        .await?;

    let mut guard = rc.lock().await;
    finish(&logger, &mut guard)?;

    logger.info((), "shutdown success");
    Ok(())
}
