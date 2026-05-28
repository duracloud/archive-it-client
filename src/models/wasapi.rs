use http_ferry::Label;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Page<T> {
    pub count: u64,
    pub next: Option<String>,
    pub previous: Option<String>,
    pub files: Vec<T>,
    #[serde(rename = "includes-extra")]
    pub includes_extra: Option<bool>,
    #[serde(rename = "request-url")]
    pub request_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct WasapiFile {
    pub filename: String,
    pub filetype: String,
    pub checksums: Checksums,
    pub account: u64,
    pub size: u64,
    pub collection: u64,
    pub crawl: Option<u64>,
    pub crawl_time: Option<String>,
    pub crawl_start: Option<String>,
    pub store_time: String,
    pub locations: Vec<String>,
}

/// Gives the engine a label (the filename) for `DownloadOutcome`'s `Display`.
impl Label for WasapiFile {
    fn label(&self) -> &str {
        &self.filename
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Checksums {
    pub sha1: Option<String>,
    pub md5: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::WasapiFile;

    #[test]
    fn wasapi_file_allows_null_crawl_fields() {
        let file: WasapiFile = serde_json::from_value(serde_json::json!({
            "filename": "ARCHIVEIT-1.warc.gz",
            "filetype": "warc",
            "checksums": { "sha1": "sha1", "md5": "md5" },
            "account": 1,
            "size": 123,
            "collection": 4472,
            "crawl": null,
            "crawl-time": null,
            "crawl-start": null,
            "store-time": "2025-01-01T00:00:00Z",
            "locations": ["https://example.invalid/warcs/foo.warc.gz"]
        }))
        .unwrap();

        assert_eq!(file.filename, "ARCHIVEIT-1.warc.gz");
        assert_eq!(file.size, 123);
        assert_eq!(file.account, 1);
        assert_eq!(file.collection, 4472);
        assert_eq!(file.crawl, None);
        assert_eq!(file.crawl_time, None);
        assert_eq!(file.crawl_start, None);
        assert_eq!(file.store_time, "2025-01-01T00:00:00Z");
    }

    #[test]
    fn wasapi_file_allows_missing_sha1_when_md5_is_present() {
        let file: WasapiFile = serde_json::from_value(serde_json::json!({
            "filename": "ARCHIVEIT-1.warc.gz",
            "filetype": "warc",
            "checksums": { "sha1": null, "md5": "md5" },
            "account": 1,
            "size": 123,
            "collection": 4472,
            "crawl": null,
            "crawl-time": "2025-01-01T00:00:00Z",
            "crawl-start": null,
            "store-time": "2025-01-01T00:00:00Z",
            "locations": ["https://example.invalid/warcs/foo.warc.gz"]
        }))
        .unwrap();

        assert_eq!(file.checksums.sha1, None);
        assert_eq!(file.checksums.md5.as_deref(), Some("md5"));
    }
}
