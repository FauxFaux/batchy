use std::fs;
use std::sync::Arc;
use std::time::Duration;

use crate::{finish, new_file, okay_or_500, Output};
use axum::body::{self, BoxBody};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::Json;
use bunyarrs::vars_dbg;
use serde::Serialize;
use serde_json::json;
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tower::util::ServiceExt as _;
use tower_http::services::ServeFile;

#[derive(Serialize)]
struct FileListing {
    name: String,
    compressed_size_estimate: u64,
    live: bool,
}

fn parse_date(date: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(date, &Rfc3339).ok()
}

pub async fn list_files(State(state): State<Arc<Output>>) -> (StatusCode, Json<Value>) {
    let logger = &state.logger;
    let live_name = state
        .out
        .lock()
        .await
        .as_ref()
        .map(|v| v.name.to_string())
        .unwrap_or(String::new());
    let mut items = Vec::new();
    okay_or_500(logger, || async {
        for f in fs::read_dir(".")? {
            let f = f?;

            let val = match f.file_name().to_str() {
                Some(val) => val.to_string(),
                None => continue,
            };

            let ext = ".events.archiv";
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

pub async fn fetch_raw(State(state): State<Arc<Output>>, Path(name): Path<String>) -> Response {
    if parse_date(&name).is_none() {
        return empty_status_response(StatusCode::BAD_REQUEST);
    }

    let file_name = format!("{}.events.archiv", name);
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
        Err(err) => {
            state.logger.warn(vars_dbg!(err), "unable to serve file");
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

pub async fn cycle(State(state): State<Arc<Output>>) -> (StatusCode, Json<Value>) {
    okay_or_500(&state.logger, || async {
        let mut previous = state.out.lock().await.replace(new_file(&state.logger)?);

        finish(&state.logger, &mut previous)?;
        Ok(json!({}))
    })
    .await
}

pub async fn time_based_cycle(output: Arc<Output>) {
    let hour = 60 * 60;
    let mut interval = tokio::time::interval(Duration::from_secs(24 * hour));
    // consume initial "immediate" firing
    interval.tick().await;

    loop {
        interval.tick().await;

        let mut opt = output.out.lock().await;
        if let Err(err) = finish(&output.logger, &mut opt) {
            output
                .logger
                .error(vars_dbg!(err), "unable to time-based finish");
        }
        match new_file(&output.logger) {
            Ok(next) => {
                opt.replace(next);
            }
            Err(err) => {
                output
                    .logger
                    .error(vars_dbg!(err), "unable to time-based refresh");
            }
        };
    }
}
