#![doc = include_str!("../README.md")]

use std::fmt::Write;
use std::time::Duration;

mod error;
mod http;
pub mod models;
pub mod partner;
pub mod public;
pub mod wasapi;

pub use error::Error;

/// The transfer engine powering the `download*` methods. Re-exported so callers
/// can name [`http_ferry::Error`] — the error carried by [`DownloadOutcome`] —
/// and the [`DownloadLocation`] trait.
pub use http_ferry;
pub use http_ferry::DownloadLocation;

/// Per-file outcome of a `WasapiClient::download*` stream. An alias for the
/// transfer engine's [`Outcome`](http_ferry::Outcome) carrying a
/// [`WasapiFile`](models::wasapi::WasapiFile); its `error` field is an
/// [`http_ferry::Error`].
pub type DownloadOutcome<L = std::path::PathBuf> =
    http_ferry::Outcome<crate::models::wasapi::WasapiFile, L>;

pub use partner::PartnerClient;
pub use public::PublicClient;
pub use wasapi::{WasapiClient, WebdataQuery};

/// User-facing types for the S3 download destination.
///
/// Construct an [`aws_sdk_s3::Client`] and pass it to
/// [`WasapiClient::download_to_s3`] or
/// [`WasapiClient::download_collection_to_s3`] alongside a bucket and
/// optional key prefix. Each upload's resolved [`S3Location`](s3::S3Location)
/// is reported back via `DownloadOutcome`.
pub mod s3 {
    pub use http_ferry::s3::S3Location;
}

pub const USER_AGENT: &str = concat!("Archive-It-Client (", env!("CARGO_PKG_VERSION"), ")");

pub fn sha1_hex(bytes: impl AsRef<[u8]>) -> String {
    let bytes = bytes.as_ref();
    let mut hex = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(&mut hex, "{b:02x}").expect("writing to String cannot fail");
    }
    hex
}

#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
pub struct PageOpts {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub base_url: String,
    pub timeout: Duration,
    pub download_timeout: Duration,
    pub max_attempts: u32,
    pub backoff: Duration,
}

impl Config {
    pub fn api() -> Self {
        Self {
            base_url: "https://partner.archive-it.org/api/".into(),
            timeout: Duration::from_secs(30),
            download_timeout: Duration::from_secs(30),
            max_attempts: 3,
            backoff: Duration::from_millis(250),
        }
    }

    pub fn wasapi() -> Self {
        Self {
            base_url: "https://partner.archive-it.org/wasapi/v1/".into(),
            timeout: Duration::from_secs(60),
            download_timeout: Duration::from_secs(300),
            ..Self::api()
        }
    }
}
