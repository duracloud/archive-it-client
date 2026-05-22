// Re-download the rows captured by `warcs_audit.rs` into S3.
//
// Reads `warcs_sync.csv` (same schema as `warcs.csv`) — both not-found and
// unmatched rows from the audit — reconstructs a `WasapiFile` per row, and
// drives `download_to_s3` for each. The S3 sink's HEAD-then-skip in
// `prepare()` makes this naturally idempotent — re-runs skip anything
// already uploaded, and unmatched objects get overwritten on re-upload.

use std::env;
use std::io::{self, Write};
use std::pin::pin;

use archive_it_client::models::wasapi::{Checksums, WasapiFile};
use archive_it_client::{DownloadOutcome, Error, WasapiClient};
use aws_config::BehaviorVersion;
use csv::ReaderBuilder;
use futures::StreamExt;

const SYNC_PATH: &str = "warcs_sync.csv";

// Column indices match `warcs_inventory.rs`'s HEADER order.
const COLLECTION_ID_COL: usize = 0;
const ACCOUNT_COL: usize = 2;
const FILENAME_COL: usize = 3;
const FILETYPE_COL: usize = 4;
const SIZE_COL: usize = 5;
const CRAWL_COL: usize = 6;
const CRAWL_TIME_COL: usize = 7;
const CRAWL_START_COL: usize = 8;
const STORE_TIME_COL: usize = 9;
const SHA1_COL: usize = 10;
const MD5_COL: usize = 11;
const ALL_LOCATIONS_COL: usize = 13;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = env::var("ARCHIVE_IT_USERNAME").expect("ARCHIVE_IT_USERNAME env var must be set");
    let pass = env::var("ARCHIVE_IT_PASSWORD").expect("ARCHIVE_IT_PASSWORD env var must be set");
    let bucket = env::var("S3_BUCKET").expect("S3_BUCKET env var must be set");
    let prefix = env::var("S3_KEY_PREFIX").ok().filter(|s| !s.is_empty());

    let aws_cfg = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let s3 = aws_sdk_s3::Client::new(&aws_cfg);
    let client = WasapiClient::new(user, pass)?;

    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(SYNC_PATH)?;

    let mut uploaded = 0_u64;
    let mut skipped = 0_u64;
    let mut wasapi_missing = 0_u64;
    let mut failed = 0_u64;

    for record in rdr.records() {
        let record = record?;
        let file = match row_to_wasapi_file(&record) {
            Ok(f) => f,
            Err(e) => {
                failed += 1;
                eprintln!("row parse error: {e}");
                continue;
            }
        };
        let key = match &prefix {
            Some(p) => format!("{p}{}", file.filename),
            None => file.filename.clone(),
        };
        eprintln!(
            "→ s3://{bucket}/{key} ({} bytes, sha1={:?})",
            file.size, file.checksums.sha1
        );

        let mut stream =
            pin!(client.download_to_s3(file, s3.clone(), bucket.clone(), prefix.clone()));
        let mut showed_progress = false;
        while let Some(outcome) = stream.next().await {
            let is_progress = matches!(outcome, DownloadOutcome::Progress { .. });
            if is_progress {
                print!("\r\x1b[2K{outcome}");
                io::stdout().flush()?;
                showed_progress = true;
                continue;
            }
            if showed_progress {
                println!();
                showed_progress = false;
            }
            println!("{outcome}");
            match outcome {
                DownloadOutcome::Downloaded { .. } => uploaded += 1,
                DownloadOutcome::Skipped { .. } => skipped += 1,
                DownloadOutcome::Failed { error, .. } => {
                    if matches!(error, Error::NotFound(_)) {
                        wasapi_missing += 1;
                    } else {
                        failed += 1;
                    }
                }
                DownloadOutcome::StreamFailed { error } => {
                    failed += 1;
                    eprintln!("stream error: {error}");
                }
                DownloadOutcome::Progress { .. } => unreachable!(),
            }
        }
    }

    eprintln!(
        "summary: {uploaded} uploaded, {skipped} skipped, \
         {wasapi_missing} wasapi-missing, {failed} failed"
    );
    Ok(())
}

fn row_to_wasapi_file(r: &csv::StringRecord) -> Result<WasapiFile, Box<dyn std::error::Error>> {
    let opt = |s: &str| {
        if s.is_empty() {
            None
        } else {
            Some(s.to_owned())
        }
    };
    let get = |i: usize| r.get(i).unwrap_or("");
    Ok(WasapiFile {
        filename: get(FILENAME_COL).to_owned(),
        filetype: get(FILETYPE_COL).to_owned(),
        checksums: Checksums {
            sha1: opt(get(SHA1_COL)),
            md5: opt(get(MD5_COL)),
        },
        account: get(ACCOUNT_COL).parse()?,
        size: get(SIZE_COL).parse()?,
        collection: get(COLLECTION_ID_COL).parse()?,
        crawl: opt(get(CRAWL_COL)).map(|s| s.parse()).transpose()?,
        crawl_time: opt(get(CRAWL_TIME_COL)),
        crawl_start: opt(get(CRAWL_START_COL)),
        store_time: get(STORE_TIME_COL).to_owned(),
        locations: get(ALL_LOCATIONS_COL)
            .split(';')
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect(),
    })
}
