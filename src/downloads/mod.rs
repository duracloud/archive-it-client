use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::pin;
use std::time::{Duration, Instant};

use futures_core::Stream;
use futures_util::TryStreamExt;
use sha1::{Digest, Sha1};
use url::Url;

use crate::Error;
use crate::http::{Transport, is_retryable};
use crate::models::wasapi::WasapiFile;

pub(crate) mod local;
mod range;
pub(crate) mod s3;

const PROGRESS_INTERVAL: Duration = Duration::from_millis(500);

pub(crate) trait Sink: Send {
    type Location: Send + 'static;

    fn prepare(
        &mut self,
        file: &WasapiFile,
    ) -> impl Future<Output = Result<Prepared<Self::Location>, Error>> + Send;

    fn write_chunk(&mut self, chunk: &[u8]) -> impl Future<Output = Result<(), Error>> + Send;

    fn restart(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    fn finalize(self) -> impl Future<Output = Result<Self::Location, Error>> + Send;
}

/// Builds a per-file `Sink`. One factory drives a whole stream of files
/// (singular call sites pass a one-element file stream).
pub(crate) trait SinkFactory: Send {
    type Sink: Sink<Location = Self::Location> + 'static;
    type Location: DownloadLocation;

    fn make(&mut self, file: &WasapiFile)
    -> impl Future<Output = Result<Self::Sink, Error>> + Send;
}

/// Renders a download destination for log lines.
///
/// Implemented by location types used with `DownloadOutcome<L>`, such as
/// local paths and S3 object locations.
pub trait DownloadLocation: Send + 'static {
    fn fmt_location(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result;
}

impl DownloadLocation for PathBuf {
    fn fmt_location(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display())
    }
}

/// Public per-file outcome surfaced to callers of every `WasapiClient::download*`
/// method. `Failed` carries per-file errors so a single bad file in a
/// collection doesn't tear down the whole stream. `StreamFailed` carries
/// errors that happen before a file is available, such as a failed collection
/// listing request or destination preflight.
#[derive(Debug)]
pub enum DownloadOutcome<L = PathBuf> {
    Downloaded {
        file: WasapiFile,
        location: L,
        verified: bool,
    },
    Failed {
        file: WasapiFile,
        error: Error,
    },
    Progress {
        file: WasapiFile,
        received: u64,
        total: u64,
    },
    Skipped {
        file: WasapiFile,
        location: L,
    },
    StreamFailed {
        error: Error,
    },
}

impl<L: DownloadLocation> fmt::Display for DownloadOutcome<L> {
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
                    file.filename
                )
            }
            Self::Downloaded {
                file,
                location,
                verified,
            } => {
                write!(f, "downloaded ")?;
                location.fmt_location(f)?;
                if *verified {
                    write!(f, " ({} bytes)", file.size)
                } else {
                    write!(f, " ({} bytes, unverified)", file.size)
                }
            }
            Self::Failed { file, error } => {
                write!(f, "failed {}: {error}", file.filename)
            }
            Self::Skipped { location, .. } => {
                write!(f, "skipped ")?;
                location.fmt_location(f)?;
                write!(f, " (already present)")
            }
            Self::StreamFailed { error } => write!(f, "stream failed: {error}"),
        }
    }
}

pub(crate) enum Prepared<L> {
    /// Destination already holds this file. Engine yields `Skipped` and stops.
    Skip { location: L },
    /// Begin or resume; engine should fetch starting at `received` and continue
    /// hashing from `partial_sha1`. `received == file.size` is valid and means
    /// the engine skips the byte fetch and goes straight to finalize.
    Resume { received: u64, partial_sha1: Sha1 },
}

/// Resolve the WARC URL for `file`. Free fn so the driver doesn't need to
/// borrow `WasapiClient`; it just gets the configured primary location prefix.
pub(crate) fn primary_location_url(primary_src: &str, file: &WasapiFile) -> Result<Url, Error> {
    let location = file
        .locations
        .iter()
        .find(|loc| loc.starts_with(primary_src))
        .ok_or_else(|| Error::PrimaryLocationMissing {
            filename: file.filename.clone(),
        })?;
    Ok(Url::parse(location)?)
}

/// One driver for every download path. Pulls files from the input stream,
/// asks the factory for a per-file sink, and runs `run_download`. Per-file
/// errors (sink build, url resolution, transport failure) yield `Failed` and
/// the loop continues to the next file — a one-element file stream therefore
/// yields exactly one terminal outcome.
pub(crate) fn drive<'a, F>(
    transport: &'a Transport,
    primary_src: &'a str,
    files: impl Stream<Item = Result<WasapiFile, Error>> + Send + 'a,
    factory: F,
) -> impl Stream<Item = DownloadOutcome<F::Location>> + Send + 'a
where
    F: SinkFactory + 'a,
{
    async_stream::stream! {
        let mut factory = factory;
        let mut files = pin!(files);
        loop {
            let file = match files.try_next().await {
                Ok(Some(file)) => file,
                Ok(None) => break,
                Err(error) => {
                    yield DownloadOutcome::StreamFailed { error };
                    return;
                }
            };
            let sink = match factory.make(&file).await {
                Ok(s) => s,
                Err(error) => {
                    yield DownloadOutcome::Failed { file, error };
                    continue;
                }
            };
            let url = match primary_location_url(primary_src, &file) {
                Ok(u) => u,
                Err(error) => {
                    yield DownloadOutcome::Failed { file, error };
                    continue;
                }
            };
            let file_for_err = file.clone();
            let mut events = pin!(run_download(transport, url, file, sink));
            loop {
                match events.try_next().await {
                    Ok(Some(outcome)) => yield outcome,
                    Ok(None) => break,
                    Err(error) => {
                        yield DownloadOutcome::Failed {
                            file: file_for_err,
                            error,
                        };
                        break;
                    }
                }
            }
        }
    }
}

/// Streams one file's download. Only emits the happy-path `DownloadOutcome`
/// variants (`Progress`, `Skipped`, `Downloaded`); per-file faults bubble out
/// as `Err` and `drive` turns them into `Failed`. `StreamFailed` is never
/// produced here — it's reserved for pre-file errors at the `drive` layer.
pub(crate) fn run_download<S>(
    transport: &Transport,
    url: Url,
    file: WasapiFile,
    sink: S,
) -> impl Stream<Item = Result<DownloadOutcome<S::Location>, Error>> + Send + '_
where
    S: Sink + Send + 'static,
{
    async_stream::try_stream! {
        let expected_sha1 = file.checksums.sha1.clone();
        let mut sink = sink;

        let (mut received, mut hasher) = match sink.prepare(&file).await? {
            Prepared::Skip { location } => {
                yield DownloadOutcome::Skipped { file, location };
                return;
            }
            Prepared::Resume { received, partial_sha1 } => (received, partial_sha1),
        };

        let mut last_progress: Option<Instant> = None;
        let mut attempts_left = transport.max_attempts();
        let mut delay = transport.backoff();

        if received > 0 && received < file.size {
            yield DownloadOutcome::Progress {
                file: file.clone(),
                received,
                total: file.size,
            };
            last_progress = Some(Instant::now());
        }

        'download: while received < file.size {
            let mut response = transport.get_response_range(url.clone(), received).await?;

            if received > 0 {
                match response.status() {
                    reqwest::StatusCode::OK => {
                        sink.restart().await?;
                        received = 0;
                        hasher = Sha1::new();
                        attempts_left = transport.max_attempts();
                        delay = transport.backoff();
                    }
                    reqwest::StatusCode::PARTIAL_CONTENT => {
                        range::validate_content_range(&response, received, file.size, &url)?;
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
                attempts_left = transport.max_attempts();
                delay = transport.backoff();

                let emit = match last_progress {
                    None => true,
                    Some(t) => t.elapsed() >= PROGRESS_INTERVAL,
                };
                if emit {
                    yield DownloadOutcome::Progress {
                        file: file.clone(),
                        received,
                        total: file.size,
                    };
                    last_progress = Some(Instant::now());
                }
            }
        }

        if received != file.size {
            Err(Error::SizeMismatch {
                url: url.to_string(),
                expected: file.size,
                actual: received,
            })?;
        }

        let verified = if let Some(expected) = expected_sha1.as_deref() {
            let actual = crate::sha1_hex(hasher.finalize());
            if actual != expected {
                Err(Error::ChecksumMismatch {
                    url: url.to_string(),
                    expected: expected.to_owned(),
                    actual,
                })?;
            }
            true
        } else {
            false
        };

        let location = sink.finalize().await?;
        yield DownloadOutcome::Downloaded { file, location, verified };
    }
}
