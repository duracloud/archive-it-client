//! HTTP layer for the transfer engine: a resumable range fetcher with
//! request-level retry. Auth (and any other per-request customization) is
//! injected by the caller via a closure, so this layer carries no knowledge
//! of how a particular service authenticates.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use reqwest::StatusCode;
use url::Url;

use crate::Error;

type Customize = dyn Fn(reqwest::RequestBuilder) -> reqwest::RequestBuilder + Send + Sync;

/// HTTP client for resumable range fetches. The `customize` closure is the
/// auth seam: it is applied to every request, so callers inject basic auth, a
/// bearer token, signed headers, or nothing — the engine stays auth-agnostic.
pub struct Downloader {
    client: reqwest::Client,
    max_attempts: u32,
    backoff: Duration,
    customize: Arc<Customize>,
}

impl Downloader {
    pub fn new(
        client: reqwest::Client,
        max_attempts: u32,
        backoff: Duration,
        customize: impl Fn(reqwest::RequestBuilder) -> reqwest::RequestBuilder + Send + Sync + 'static,
    ) -> Self {
        Self {
            client,
            max_attempts: max_attempts.max(1),
            backoff,
            customize: Arc::new(customize),
        }
    }

    pub(crate) fn backoff(&self) -> Duration {
        self.backoff
    }

    pub(crate) fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    pub(crate) async fn get_response_range(
        &self,
        url: Url,
        from: u64,
    ) -> Result<reqwest::Response, Error> {
        let range = if from > 0 { Some(from) } else { None };
        self.retry(|| self.attempt(&url, range)).await
    }

    async fn retry<F, Fut, T>(&self, mut op: F) -> Result<T, Error>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, Error>>,
    {
        let mut delay = self.backoff;
        let mut attempts_left = self.max_attempts;

        loop {
            attempts_left -= 1;
            let result = op().await;

            if attempts_left == 0 {
                return result;
            }
            match result {
                Ok(v) => return Ok(v),
                Err(e) if !is_retryable(&e) => return Err(e),
                Err(_) => {
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2);
                }
            }
        }
    }

    async fn attempt(
        &self,
        url: &Url,
        range_from: Option<u64>,
    ) -> Result<reqwest::Response, Error> {
        let mut req = self.client.get(url.clone());
        if let Some(from) = range_from {
            req = req.header(reqwest::header::RANGE, format!("bytes={}-", from));
        }
        req = (self.customize)(req);
        let resp = req.send().await?;
        let status = resp.status();
        if status == StatusCode::NOT_FOUND {
            return Err(Error::NotFound(url.to_string()));
        }
        if !status.is_success() {
            return Err(Error::Status(status));
        }
        Ok(resp)
    }
}

pub(crate) fn is_retryable(err: &Error) -> bool {
    match err {
        Error::Request(e) => e.is_timeout() || e.is_connect(),
        Error::Status(s) => s.is_server_error() || *s == StatusCode::TOO_MANY_REQUESTS,
        _ => false,
    }
}
