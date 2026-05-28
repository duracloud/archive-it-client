#![doc = include_str!("../README.md")]

use std::fmt;
use std::fmt::Write as _;
use std::future::Future;
use std::path::PathBuf;
use std::pin::pin;
use std::time::{Duration, Instant};

use futures_core::Stream;
use futures_util::TryStreamExt;
use md5::Md5;
use sha1::{Digest, Sha1};
use url::Url;

mod error;
mod http;
pub mod local;
mod range;
#[cfg(feature = "s3")]
pub mod s3;

pub use error::Error;
pub use http::Downloader;
use http::is_retryable;

const PROGRESS_INTERVAL: Duration = Duration::from_millis(500);

/// Expected integrity hash supplied by the caller for a transfer. The engine
/// hashes the byte stream with the matching algorithm and verifies the result;
/// sinks use it for skip-on-match decisions.
#[derive(Debug, Clone)]
pub enum Checksum {
    Sha1(String),
    Md5(String),
}

impl Checksum {
    /// Lowercase hex digest this checksum carries.
    pub fn hex(&self) -> &str {
        match self {
            Checksum::Sha1(h) | Checksum::Md5(h) => h,
        }
    }

    /// Stable short name for the algorithm, used as an object-metadata key.
    pub fn algorithm(&self) -> &'static str {
        match self {
            Checksum::Sha1(_) => "sha1",
            Checksum::Md5(_) => "md5",
        }
    }

    /// A checksum of the same algorithm carrying `value`. Lets a sink rebuild
    /// the expected variant from a stored metadata string.
    pub fn with_value(&self, value: String) -> Checksum {
        match self {
            Checksum::Sha1(_) => Checksum::Sha1(value),
            Checksum::Md5(_) => Checksum::Md5(value),
        }
    }
}

/// Streaming hasher selected from the caller's expected [`Checksum`]. `None`
/// means no checksum was supplied: the engine still counts bytes but reports
/// `verified: false`.
pub enum Hasher {
    None,
    Sha1(Sha1),
    Md5(Md5),
}

impl Hasher {
    pub fn for_checksum(checksum: Option<&Checksum>) -> Self {
        match checksum {
            Some(Checksum::Sha1(_)) => Hasher::Sha1(Sha1::new()),
            Some(Checksum::Md5(_)) => Hasher::Md5(Md5::new()),
            None => Hasher::None,
        }
    }

    pub fn update(&mut self, bytes: &[u8]) {
        match self {
            Hasher::Sha1(h) => h.update(bytes),
            Hasher::Md5(h) => h.update(bytes),
            Hasher::None => {}
        }
    }

    pub fn finalize_hex(self) -> Option<String> {
        match self {
            Hasher::Sha1(h) => Some(to_hex(&h.finalize())),
            Hasher::Md5(h) => Some(to_hex(&h.finalize())),
            Hasher::None => None,
        }
    }
}

fn to_hex(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(&mut hex, "{b:02x}").expect("writing to String cannot fail");
    }
    hex
}

/// One unit of work for the engine. `meta` is the caller's own item type,
/// carried opaquely and handed straight back via [`Outcome`]; the engine reads
/// only `size`, `checksum`, and `name`.
pub struct Transfer<M> {
    pub size: u64,
    pub checksum: Option<Checksum>,
    pub name: String,
    pub meta: M,
}

/// Borrowed view of a [`Transfer`] handed to sinks at prepare time. The source
/// URL is resolved by the engine and `meta` is the caller's concern, so neither
/// appears here — sinks are domain-agnostic.
#[derive(Clone, Copy)]
pub struct Target<'a> {
    pub name: &'a str,
    pub size: u64,
    pub checksum: Option<&'a Checksum>,
}

pub trait Sink: Send {
    type Location: Send + 'static;

    fn prepare(
        &mut self,
        target: Target<'_>,
    ) -> impl Future<Output = Result<Prepared<Self::Location>, Error>> + Send;

    fn write_chunk(&mut self, chunk: &[u8]) -> impl Future<Output = Result<(), Error>> + Send;

    fn restart(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    fn finalize(self) -> impl Future<Output = Result<Self::Location, Error>> + Send;
}

/// Builds a per-item `Sink`. One factory drives a whole stream of items
/// (singular call sites pass a one-element stream).
pub trait SinkFactory: Send {
    type Sink: Sink<Location = Self::Location> + 'static;
    type Location: DownloadLocation;

    fn make(
        &mut self,
        target: Target<'_>,
    ) -> impl Future<Output = Result<Self::Sink, Error>> + Send;
}

/// Renders a transfer destination for log lines.
///
/// Implemented by location types used with [`Outcome`], such as local paths
/// and S3 object locations.
pub trait DownloadLocation: Send + 'static {
    fn fmt_location(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result;
}

impl DownloadLocation for PathBuf {
    fn fmt_location(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display())
    }
}

impl DownloadLocation for String {
    fn fmt_location(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self)
    }
}

/// Short label for a transfer item (e.g. a filename), used by [`Outcome`]'s
/// `Display` impl to render log lines. Implement it on your `meta` type `M` to
/// get `Display` for the outcomes carrying it.
pub trait Label {
    fn label(&self) -> &str;
}

/// Per-item outcome of a transfer stream, generic over the caller's item type
/// `M`. `Failed` carries per-item errors so a single bad item in a batch
/// doesn't tear down the whole stream. `StreamFailed` carries errors that
/// happen before an item is available, such as a failed listing request or
/// destination preflight.
#[derive(Debug)]
pub enum Outcome<M, L = PathBuf> {
    Downloaded {
        file: M,
        location: L,
        verified: bool,
    },
    Failed {
        file: M,
        error: Error,
    },
    Progress {
        file: M,
        received: u64,
        total: u64,
    },
    Skipped {
        file: M,
        location: L,
    },
    StreamFailed {
        error: Error,
    },
}

impl<M: Label, L: DownloadLocation> fmt::Display for Outcome<M, L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Progress {
                file,
                received,
                total,
            } => {
                let pct = if *total == 0 {
                    100.0
                } else {
                    (*received as f64 / *total as f64) * 100.0
                };
                write!(
                    f,
                    "{}: {pct:.1}% ({received} / {total} bytes)",
                    file.label()
                )
            }
            Self::Downloaded {
                location, verified, ..
            } => {
                write!(f, "downloaded ")?;
                location.fmt_location(f)?;
                write!(
                    f,
                    " ({})",
                    if *verified { "verified" } else { "unverified" }
                )
            }
            Self::Failed { file, error } => write!(f, "failed {}: {error}", file.label()),
            Self::Skipped { location, .. } => {
                write!(f, "skipped ")?;
                location.fmt_location(f)?;
                write!(f, " (already present)")
            }
            Self::StreamFailed { error } => write!(f, "stream failed: {error}"),
        }
    }
}

pub enum Prepared<L> {
    /// Destination already holds this file. Engine yields `Skipped` and stops.
    Skip { location: L },
    /// Begin or resume; engine should fetch starting at `received` and continue
    /// hashing from `partial`. `received == size` is valid and means the engine
    /// skips the byte fetch and goes straight to finalize.
    Resume { received: u64, partial: Hasher },
}

/// One driver for every download path. Pulls items from the input stream,
/// resolves each item's source URL, asks the factory for a per-item sink, and
/// runs `run_download`. Per-item errors (url resolution, sink build, transport
/// failure) yield `Failed` and the loop continues to the next item — a
/// one-element input stream therefore yields exactly one terminal outcome.
pub fn drive<'a, M, F, R>(
    http: &'a Downloader,
    items: impl Stream<Item = Result<Transfer<M>, Error>> + Send + 'a,
    mut resolve: R,
    factory: F,
) -> impl Stream<Item = Outcome<M, F::Location>> + Send + 'a
where
    M: Clone + Send + 'static,
    F: SinkFactory + 'a,
    R: FnMut(&Transfer<M>) -> Result<Url, Error> + Send + 'a,
{
    async_stream::stream! {
        let mut factory = factory;
        let mut items = pin!(items);
        loop {
            let transfer = match items.try_next().await {
                Ok(Some(transfer)) => transfer,
                Ok(None) => break,
                Err(error) => {
                    yield Outcome::StreamFailed { error };
                    return;
                }
            };
            let url = match resolve(&transfer) {
                Ok(u) => u,
                Err(error) => {
                    yield Outcome::Failed { file: transfer.meta, error };
                    continue;
                }
            };
            let target = Target {
                name: &transfer.name,
                size: transfer.size,
                checksum: transfer.checksum.as_ref(),
            };
            let sink = match factory.make(target).await {
                Ok(s) => s,
                Err(error) => {
                    yield Outcome::Failed { file: transfer.meta, error };
                    continue;
                }
            };
            let meta_for_err = transfer.meta.clone();
            let mut events = pin!(run_download(http, url, transfer, sink));
            loop {
                match events.try_next().await {
                    Ok(Some(outcome)) => yield outcome,
                    Ok(None) => break,
                    Err(error) => {
                        yield Outcome::Failed {
                            file: meta_for_err,
                            error,
                        };
                        break;
                    }
                }
            }
        }
    }
}

/// Streams one item's download. Only emits the happy-path `Outcome` variants
/// (`Progress`, `Skipped`, `Downloaded`); per-item faults bubble out as `Err`
/// and `drive` turns them into `Failed`. `StreamFailed` is never produced
/// here — it's reserved for pre-item errors at the `drive` layer.
pub fn run_download<M, S>(
    http: &Downloader,
    url: Url,
    transfer: Transfer<M>,
    sink: S,
) -> impl Stream<Item = Result<Outcome<M, S::Location>, Error>> + Send + '_
where
    M: Clone + Send + 'static,
    S: Sink + Send + 'static,
{
    async_stream::try_stream! {
        let mut sink = sink;

        let (mut received, mut hasher) = match sink
            .prepare(Target {
                name: &transfer.name,
                size: transfer.size,
                checksum: transfer.checksum.as_ref(),
            })
            .await?
        {
            Prepared::Skip { location } => {
                yield Outcome::Skipped { file: transfer.meta, location };
                return;
            }
            Prepared::Resume { received, partial } => (received, partial),
        };

        let mut last_progress: Option<Instant> = None;
        let mut attempts_left = http.max_attempts();
        let mut delay = http.backoff();

        if received > 0 && received < transfer.size {
            yield Outcome::Progress {
                file: transfer.meta.clone(),
                received,
                total: transfer.size,
            };
            last_progress = Some(Instant::now());
        }

        'download: while received < transfer.size {
            let mut response = http.get_response_range(url.clone(), received).await?;

            if received > 0 {
                match response.status() {
                    reqwest::StatusCode::OK => {
                        sink.restart().await?;
                        received = 0;
                        hasher = Hasher::for_checksum(transfer.checksum.as_ref());
                        attempts_left = http.max_attempts();
                        delay = http.backoff();
                    }
                    reqwest::StatusCode::PARTIAL_CONTENT => {
                        range::validate_content_range(&response, received, transfer.size, &url)?;
                    }
                    status => {
                        Err(Error::InvalidRangeResponse {
                            url: url.to_string(),
                            details: format!("expected 200 or 206 for resume, got {status}"),
                        })?;
                    }
                }
            }

            loop {
                let chunk = match response.chunk().await {
                    Ok(Some(chunk)) => chunk,
                    Ok(None) => break 'download,
                    Err(e) => {
                        let err = Error::from(e);
                        if attempts_left > 1 && is_retryable(&err) {
                            attempts_left -= 1;
                            tokio::time::sleep(delay).await;
                            delay = delay.saturating_mul(2);
                            continue 'download;
                        }
                        Err(err)?;
                        unreachable!();
                    }
                };

                sink.write_chunk(&chunk).await?;
                hasher.update(&chunk);
                received += chunk.len() as u64;
                attempts_left = http.max_attempts();
                delay = http.backoff();

                let emit = match last_progress {
                    None => true,
                    Some(t) => t.elapsed() >= PROGRESS_INTERVAL,
                };
                if emit {
                    yield Outcome::Progress {
                        file: transfer.meta.clone(),
                        received,
                        total: transfer.size,
                    };
                    last_progress = Some(Instant::now());
                }
            }
        }

        if received != transfer.size {
            Err(Error::SizeMismatch {
                url: url.to_string(),
                expected: transfer.size,
                actual: received,
            })?;
        }

        let verified = match (transfer.checksum.as_ref(), hasher.finalize_hex()) {
            (Some(expected), Some(actual)) => {
                if actual != expected.hex() {
                    Err(Error::ChecksumMismatch {
                        algorithm: expected.algorithm(),
                        url: url.to_string(),
                        expected: expected.hex().to_owned(),
                        actual,
                    })?;
                }
                true
            }
            _ => false,
        };

        let location = sink.finalize().await?;
        yield Outcome::Downloaded { file: transfer.meta, location, verified };
    }
}
