use std::io;

use reqwest::StatusCode;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("authenticated account list was empty")]
    Empty,
    #[error("I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("resource not found: {0}")]
    NotFound(String),
    #[error("no primary WARC location for {filename}")]
    PrimaryLocationMissing { filename: String },
    #[error("HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("unexpected status: {0}")]
    Status(StatusCode),
    #[error("invalid URL: {0}")]
    Url(#[from] url::ParseError),
}
