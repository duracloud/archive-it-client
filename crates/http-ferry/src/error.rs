use std::io;
use std::path::PathBuf;

use reqwest::StatusCode;

/// Errors produced by the transfer engine.
///
/// [`Source`](Error::Source) is the seam for caller-supplied failures: the URL
/// resolver and the input item stream hand their own error types to the engine
/// boxed through this variant, so the engine never needs to know about a
/// consumer's domain errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{algorithm} mismatch for {url}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        algorithm: &'static str,
        url: String,
        expected: String,
        actual: String,
    },
    #[error("download path is not a single safe file name: {}", .path.display())]
    InvalidDownloadPath { path: PathBuf },
    #[error("invalid range response for {url}: {details}")]
    InvalidRangeResponse { url: String, details: String },
    #[error("I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("resource not found: {0}")]
    NotFound(String),
    #[error("HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[cfg(feature = "s3")]
    #[error("S3 operation failed: {0}")]
    S3(Box<dyn std::error::Error + Send + Sync>),
    #[error("downloaded {actual} bytes from {url}; expected {expected}")]
    SizeMismatch {
        url: String,
        expected: u64,
        actual: u64,
    },
    #[error("source resolution failed: {0}")]
    Source(Box<dyn std::error::Error + Send + Sync>),
    #[error("unexpected status: {0}")]
    Status(StatusCode),
    #[error("invalid URL: {0}")]
    Url(#[from] url::ParseError),
}

impl Error {
    /// Wrap a caller-supplied error — from the URL resolver or the input item
    /// stream — as a [`Source`](Error::Source). Shorthand for
    /// `Error::Source(Box::new(err))`; accepts anything boxable, including a
    /// `&str`/`String` message.
    pub fn from_source(err: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Error::Source(err.into())
    }
}
