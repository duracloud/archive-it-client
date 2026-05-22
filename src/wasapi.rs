use std::path::{Path, PathBuf};
use std::pin::pin;

use aws_sdk_s3::Client as AwsS3Client;
use futures_core::Stream;
use futures_util::StreamExt;
use serde::Serialize;

use crate::downloads::local::{LocalDir, LocalPath};
use crate::downloads::s3::{S3Dest, S3Location};
use crate::downloads::{self, DownloadOutcome};
use crate::http::Transport;
use crate::models::wasapi::{Page, WasapiFile};
use crate::{Config, Error};

pub const PRIMARY_LOCATION_SRC: &str = "https://warcs.archive-it.org";
pub const DEFAULT_WEBDATA_PAGE_SIZE: u32 = 50;

#[derive(Debug, Clone, Serialize)]
pub struct WebdataQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filetype: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collection: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub crawl: Option<u64>,
    #[serde(rename = "crawl-time-after", skip_serializing_if = "Option::is_none")]
    pub crawl_time_after: Option<String>,
    #[serde(rename = "crawl-time-before", skip_serializing_if = "Option::is_none")]
    pub crawl_time_before: Option<String>,
    #[serde(rename = "crawl-start-after", skip_serializing_if = "Option::is_none")]
    pub crawl_start_after: Option<String>,
    #[serde(rename = "crawl-start-before", skip_serializing_if = "Option::is_none")]
    pub crawl_start_before: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u32>,
}

impl Default for WebdataQuery {
    fn default() -> Self {
        Self {
            filename: None,
            filetype: None,
            collection: None,
            crawl: None,
            crawl_time_after: None,
            crawl_time_before: None,
            crawl_start_after: None,
            crawl_start_before: None,
            page: None,
            page_size: Some(DEFAULT_WEBDATA_PAGE_SIZE),
        }
    }
}

pub struct WasapiClient {
    transport: Transport,
    primary_location_src: String,
}

impl WasapiClient {
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Result<Self, Error> {
        Self::with_config(username, password, Config::wasapi())
    }

    pub fn with_config(
        username: impl Into<String>,
        password: impl Into<String>,
        cfg: Config,
    ) -> Result<Self, Error> {
        Ok(Self {
            transport: Transport::new(cfg, Some((username.into(), password.into())))?,
            primary_location_src: PRIMARY_LOCATION_SRC.into(),
        })
    }

    pub fn with_primary_location_src(mut self, src: impl Into<String>) -> Self {
        self.primary_location_src = src.into();
        self
    }

    pub fn download(
        &self,
        file: WasapiFile,
        path: impl AsRef<Path>,
    ) -> impl Stream<Item = DownloadOutcome> + Send + '_ {
        let path = path.as_ref().to_path_buf();
        downloads::drive(
            &self.transport,
            &self.primary_location_src,
            single_file(file),
            LocalPath { path },
        )
    }

    pub fn download_collection(
        &self,
        query: WebdataQuery,
        dir: impl Into<PathBuf>,
    ) -> impl Stream<Item = DownloadOutcome> + Send + '_ {
        let dir = dir.into();
        async_stream::stream! {
            // Preflight the destination once so a bad output path fails the
            // stream upfront instead of yielding one Failed per file. Also
            // ensures the directory exists when the collection is empty.
            if let Err(error) = tokio::fs::create_dir_all(&dir).await {
                yield DownloadOutcome::StreamFailed {
                    error: Error::from(error),
                };
                return;
            }
            let inner = downloads::drive(
                &self.transport,
                &self.primary_location_src,
                self.webdata(query),
                LocalDir { dir },
            );
            let mut inner = pin!(inner);
            while let Some(outcome) = inner.next().await {
                yield outcome;
            }
        }
    }

    pub fn download_to_s3(
        &self,
        file: WasapiFile,
        s3: AwsS3Client,
        bucket: impl Into<String>,
        prefix: Option<String>,
    ) -> impl Stream<Item = DownloadOutcome<S3Location>> + Send + '_ {
        downloads::drive(
            &self.transport,
            &self.primary_location_src,
            single_file(file),
            S3Dest {
                client: s3,
                bucket: bucket.into(),
                prefix,
            },
        )
    }

    pub fn download_collection_to_s3(
        &self,
        query: WebdataQuery,
        s3: AwsS3Client,
        bucket: impl Into<String>,
        prefix: Option<String>,
    ) -> impl Stream<Item = DownloadOutcome<S3Location>> + Send + '_ {
        downloads::drive(
            &self.transport,
            &self.primary_location_src,
            self.webdata(query),
            S3Dest {
                client: s3,
                bucket: bucket.into(),
                prefix,
            },
        )
    }

    pub async fn list_webdata(&self, query: &WebdataQuery) -> Result<Page<WasapiFile>, Error> {
        self.transport.get_json("webdata", query).await
    }

    pub async fn list_webdata_next(
        &self,
        prev: &Page<WasapiFile>,
    ) -> Result<Option<Page<WasapiFile>>, Error> {
        match prev.next.as_deref() {
            None => Ok(None),
            Some(url) => self.transport.get_json(url, &()).await.map(Some),
        }
    }

    pub fn webdata(
        &self,
        query: WebdataQuery,
    ) -> impl Stream<Item = Result<WasapiFile, Error>> + Send + '_ {
        async_stream::try_stream! {
            let mut page = self.list_webdata(&query).await?;
            loop {
                let files = std::mem::take(&mut page.files);
                for f in files { yield f; }
                match self.list_webdata_next(&page).await? {
                    Some(next) => page = next,
                    None => break,
                }
            }
        }
    }

    pub fn primary_location<'a>(&self, file: &'a WasapiFile) -> Option<&'a str> {
        file.locations
            .iter()
            .find(|location| location.starts_with(&self.primary_location_src))
            .map(String::as_str)
    }
}

fn single_file(file: WasapiFile) -> impl Stream<Item = Result<WasapiFile, Error>> + Send {
    async_stream::try_stream! { yield file; }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::wasapi::Checksums;

    #[test]
    fn primary_location_uses_configured_source_prefix() {
        let client = WasapiClient::new("u", "p")
            .unwrap()
            .with_primary_location_src("https://example.invalid");
        let file = WasapiFile {
            filename: "ARCHIVEIT-1.warc.gz".into(),
            filetype: "warc".into(),
            checksums: Checksums {
                sha1: Some("sha1".into()),
                md5: Some("md5".into()),
            },
            account: 1,
            size: 1,
            collection: 1,
            crawl: Some(1),
            crawl_time: Some("2020-01-01T00:00:00Z".into()),
            crawl_start: Some("2020-01-01T00:00:00Z".into()),
            store_time: "2020-01-01T00:00:00Z".into(),
            locations: vec![
                "https://other.example.com/warcs/foo.warc.gz".into(),
                "https://example.invalid/warcs/foo.warc.gz".into(),
            ],
        };

        assert_eq!(
            client.primary_location(&file),
            Some("https://example.invalid/warcs/foo.warc.gz")
        );
    }
}
