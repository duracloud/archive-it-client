use std::future::Future;
use std::time::Duration;

use futures_core::Stream;
use serde::Serialize;
use serde::de::DeserializeOwned;
use url::Url;

use crate::{Config, Error, PageOpts, USER_AGENT};

pub(crate) const STREAM_PAGE_SIZE: u32 = 100;

pub(crate) struct Transport {
    backoff: Duration,
    base_url: Url,
    json_client: reqwest::Client,
    creds: Option<(String, String)>,
    max_attempts: u32,
}

impl Transport {
    pub(crate) fn new(cfg: Config, creds: Option<(String, String)>) -> Result<Self, Error> {
        let json_client = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .default_headers(header_map(&cfg.headers)?)
            .read_timeout(cfg.timeout)
            .build()?;
        Ok(Self {
            backoff: cfg.backoff,
            base_url: Url::parse(&cfg.base_url)?,
            json_client,
            creds,
            max_attempts: cfg.max_attempts.max(1),
        })
    }

    pub(crate) async fn get_json<Q, T>(&self, path: &str, query: &Q) -> Result<T, Error>
    where
        Q: Serialize + ?Sized,
        T: DeserializeOwned,
    {
        let url = self.base_url.join(path)?;
        self.retry(|| async {
            let resp = self.attempt(&url, query).await?;
            Ok::<T, Error>(resp.json().await?)
        })
        .await
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

    async fn attempt<Q>(&self, url: &Url, query: &Q) -> Result<reqwest::Response, Error>
    where
        Q: Serialize + ?Sized,
    {
        let mut req = self.json_client.get(url.clone()).query(query);
        if let Some((u, p)) = &self.creds {
            req = req.basic_auth(u, Some(p));
        }
        let resp = req.send().await?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(Error::NotFound(url.to_string()));
        }
        if !status.is_success() {
            return Err(Error::Status(status));
        }
        Ok(resp)
    }
}

/// Build the default `HeaderMap` from `Config::headers`. Values are marked
/// sensitive so tokens are redacted from debug output; parse failures report
/// the header name but never echo the value.
pub(crate) fn header_map(
    headers: &[(String, String)],
) -> Result<reqwest::header::HeaderMap, Error> {
    use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

    let mut map = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers {
        let header_name =
            HeaderName::from_bytes(name.as_bytes()).map_err(|e| Error::InvalidHeader {
                name: name.clone(),
                reason: e.to_string(),
            })?;
        let mut header_value = HeaderValue::from_str(value).map_err(|e| Error::InvalidHeader {
            name: name.clone(),
            reason: e.to_string(),
        })?;
        header_value.set_sensitive(true);
        map.append(header_name, header_value);
    }
    Ok(map)
}

pub(crate) fn paginated<'a, F, Fut, T>(
    fetcher: F,
) -> impl Stream<Item = Result<T, Error>> + Send + 'a
where
    F: Fn(PageOpts) -> Fut + Send + 'a,
    Fut: Future<Output = Result<Vec<T>, Error>> + Send + 'a,
    T: Send + 'a,
{
    async_stream::try_stream! {
        let mut offset: u32 = 0;
        loop {
            let batch = fetcher(PageOpts {
                limit: Some(STREAM_PAGE_SIZE),
                offset: Some(offset),
            })
            .await?;
            let n = batch.len() as u32;
            for item in batch { yield item; }
            if n < STREAM_PAGE_SIZE { break; }
            offset += n;
        }
    }
}

// Retry classification for the API transport. Kept local rather than shared
// with `http-ferry` so the JSON API layer does not depend on the transfer
// engine.
fn is_retryable(err: &Error) -> bool {
    match err {
        Error::Request(e) => e.is_timeout() || e.is_connect(),
        Error::Status(s) => s.is_server_error() || *s == reqwest::StatusCode::TOO_MANY_REQUESTS,
        _ => false,
    }
}
