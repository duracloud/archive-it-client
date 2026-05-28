// Match-check report against an S3 bucket using `warcs.csv` as the source
// of truth. For each (filename, sha1, size) row, HEAD the object and
// classify it the way http-ferry's S3 sink (its `should_skip`) would:
//   - matched (sha1):  object's sha1 metadata == WASAPI sha1
//   - matched (size):  no sha1 metadata, but content_length == WASAPI size
//   - unmatched:       object exists but neither matches (sink would re-upload)
//   - not found:       HEAD returned NotFound (sink would do a fresh upload)
//   - expired:         row's store_time is older than the expiration window;
//                      if the object exists, tag it (when enabled) and do not
//                      re-evaluate match; if it doesn't, skip rather than
//                      queue a re-download.
//   - errored:         any other HEAD/tagging failure
//
// Rows in the not-found and unmatched buckets — i.e. anything the sink
// would (re-)upload — are written to `warcs_sync.csv` with the same schema
// as `warcs.csv` so `warcs_sync.rs` can drive a follow-up download for
// each. Expired rows are reported but not queued.

use std::env;

use aws_config::BehaviorVersion;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectOutput};
use aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingError;
use aws_sdk_s3::types::{Tag, Tagging};
use aws_smithy_types::error::display::DisplayErrorContext;
use chrono::{DateTime, Months, Utc};
use csv::{ReaderBuilder, WriterBuilder};
use futures::stream::{self, StreamExt};

const INVENTORY_PATH: &str = "warcs.csv";
const SYNC_PATH: &str = "warcs_sync.csv";
const FILENAME_COL: usize = 3;
const SIZE_COL: usize = 5;
const STORE_TIME_COL: usize = 9;
const SHA1_COL: usize = 10;
const SHA1_METADATA_KEY: &str = "sha1";
const PROGRESS_EVERY: u64 = 1000;
const DEFAULT_CONCURRENCY: usize = 200;
const EXPIRATION_ENABLED: bool = false;
const EXPIRATION_YEARS: u32 = 5;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

enum Outcome {
    MatchedSha1,
    MatchedSize,
    Unmatched(csv::StringRecord, String),
    NotFound(csv::StringRecord, String),
    Expired(csv::StringRecord, String),
    Errored(String),
    Skipped(String),
}

struct RowCtx<'a> {
    s3: &'a aws_sdk_s3::Client,
    bucket: &'a str,
    key_prefix: &'a str,
    expired_time: DateTime<Utc>,
    expired_tagging: Option<Tagging>,
}

struct ParsedRow {
    filename: String,
    expected_sha1: String,
    expected_size: u64,
    store_time: DateTime<Utc>,
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let bucket = env::var("S3_BUCKET").expect("S3_BUCKET env var must be set");
    let key_prefix = env::var("S3_KEY_PREFIX").unwrap_or_default();
    let expired_tagging = if EXPIRATION_ENABLED {
        let tag_key = env::var("EXPIRED_TAG")
            .expect("EXPIRED_TAG env var must be set when EXPIRATION_ENABLED is true");
        let tag = Tag::builder().key(tag_key).value("true").build()?;
        Some(Tagging::builder().tag_set(tag).build()?)
    } else {
        None
    };
    let concurrency: usize = env::var("CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_CONCURRENCY);

    let expired_time = Utc::now() - Months::new(EXPIRATION_YEARS * 12);

    let aws_cfg = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let s3 = aws_sdk_s3::Client::new(&aws_cfg);

    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(INVENTORY_PATH)?;
    let header = rdr.headers()?.clone();
    let mut sync_writer = WriterBuilder::new().from_path(SYNC_PATH)?;
    sync_writer.write_record(&header)?;

    let ctx = RowCtx {
        s3: &s3,
        bucket: &bucket,
        key_prefix: &key_prefix,
        expired_time,
        expired_tagging,
    };
    let ctx_ref = &ctx;

    let mut matched_sha1 = 0_u64;
    let mut matched_size = 0_u64;
    let mut unmatched = 0_u64;
    let mut not_found = 0_u64;
    let mut expired = 0_u64;
    let mut errored = 0_u64;
    let mut skipped_rows = 0_u64;

    let stream = stream::iter(rdr.records())
        .map(|r| async move {
            let record = r?;
            Ok::<Outcome, BoxError>(audit_row(ctx_ref, record).await)
        })
        .buffer_unordered(concurrency);

    tokio::pin!(stream);
    let mut done = 0_u64;
    while let Some(result) = stream.next().await {
        match result? {
            Outcome::MatchedSha1 => matched_sha1 += 1,
            Outcome::MatchedSize => matched_size += 1,
            Outcome::Unmatched(record, msg) => {
                unmatched += 1;
                sync_writer.write_record(&record)?;
                sync_writer.flush()?;
                println!("{msg}");
            }
            Outcome::NotFound(record, msg) => {
                not_found += 1;
                sync_writer.write_record(&record)?;
                sync_writer.flush()?;
                println!("{msg}");
            }
            Outcome::Expired(_record, msg) => {
                expired += 1;
                println!("{msg}");
            }
            Outcome::Errored(msg) => {
                errored += 1;
                println!("{msg}");
            }
            Outcome::Skipped(msg) => {
                skipped_rows += 1;
                println!("{msg}");
            }
        }

        done += 1;
        if done.is_multiple_of(PROGRESS_EVERY) {
            eprintln!(
                "[{done}] sha1={matched_sha1} size={matched_size} \
                 unmatched={unmatched} not_found={not_found} \
                 expired={expired} errored={errored}"
            );
        }
    }

    sync_writer.flush()?;
    eprintln!(
        "summary: {matched_sha1} matched (sha1), {matched_size} matched (size), \
         {unmatched} unmatched, {not_found} not found, {expired} expired, \
         {errored} errored ({skipped_rows} rows skipped)"
    );

    Ok(())
}

async fn audit_row(ctx: &RowCtx<'_>, record: csv::StringRecord) -> Outcome {
    let parsed = match parse_row(&record) {
        Ok(p) => p,
        Err(msg) => return Outcome::Skipped(msg),
    };
    let is_expired = parsed.store_time < ctx.expired_time;
    let key = format!("{}{}", ctx.key_prefix, parsed.filename);

    let head = match ctx
        .s3
        .head_object()
        .bucket(ctx.bucket)
        .key(&key)
        .send()
        .await
    {
        Ok(out) => out,
        Err(SdkError::ServiceError(e)) if matches!(e.err(), HeadObjectError::NotFound(_)) => {
            return if is_expired {
                Outcome::Skipped(format!("not found but expired: s3://{}/{key}", ctx.bucket))
            } else {
                Outcome::NotFound(record, format!("not found: s3://{}/{key}", ctx.bucket))
            };
        }
        Err(e) => {
            return Outcome::Errored(format!(
                "error: s3://{}/{key}: {}",
                ctx.bucket,
                DisplayErrorContext(&e)
            ));
        }
    };

    if !is_expired {
        return classify_existing(&head, &parsed, record, ctx.bucket, &key);
    }

    let Some(tagging) = ctx.expired_tagging.clone() else {
        return Outcome::Expired(
            record,
            format!("expired (would tag): s3://{}/{key}", ctx.bucket),
        );
    };

    match tag_expired(ctx.s3, ctx.bucket, &key, tagging).await {
        Ok(()) => Outcome::Expired(
            record,
            format!("expired (tagged): s3://{}/{key}", ctx.bucket),
        ),
        Err(e) => Outcome::Errored(format!(
            "error tagging expired s3://{}/{key}: {}",
            ctx.bucket,
            DisplayErrorContext(&e)
        )),
    }
}

fn parse_row(record: &csv::StringRecord) -> Result<ParsedRow, String> {
    let filename = record.get(FILENAME_COL).unwrap_or("");
    let expected_sha1 = record.get(SHA1_COL).unwrap_or("");
    let size_str = record.get(SIZE_COL).unwrap_or("");
    let store_time_str = record.get(STORE_TIME_COL).unwrap_or("");
    if filename.is_empty() || size_str.is_empty() {
        return Err("cannot process record without filename and filesize".to_string());
    }
    let expected_size: u64 = size_str
        .parse()
        .map_err(|e| format!("invalid size {size_str:?}: {e}"))?;
    let store_time = DateTime::parse_from_rfc3339(store_time_str)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|e| format!("invalid store_time {store_time_str:?}: {e}"))?;
    Ok(ParsedRow {
        filename: filename.to_string(),
        expected_sha1: expected_sha1.to_string(),
        expected_size,
        store_time,
    })
}

fn classify_existing(
    head: &HeadObjectOutput,
    parsed: &ParsedRow,
    record: csv::StringRecord,
    bucket: &str,
    key: &str,
) -> Outcome {
    let existing_sha1 = head
        .metadata
        .as_ref()
        .and_then(|m| m.get(SHA1_METADATA_KEY))
        .map(String::as_str);
    let existing_size = head.content_length.unwrap_or(0).max(0) as u64;
    match existing_sha1 {
        Some(s) if s == parsed.expected_sha1 => Outcome::MatchedSha1,
        Some(s) => Outcome::Unmatched(
            record,
            format!(
                "unmatched (sha1 differs): s3://{bucket}/{key} \
                 (existing: {s}, expected: {})",
                parsed.expected_sha1
            ),
        ),
        None if existing_size == parsed.expected_size => Outcome::MatchedSize,
        None => Outcome::Unmatched(
            record,
            format!(
                "unmatched (no sha1, size differs): s3://{bucket}/{key} \
                 (existing size: {existing_size}, expected: {})",
                parsed.expected_size
            ),
        ),
    }
}

async fn tag_expired(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    tagging: Tagging,
) -> Result<(), SdkError<PutObjectTaggingError>> {
    s3.put_object_tagging()
        .bucket(bucket)
        .key(key)
        .tagging(tagging)
        .send()
        .await?;
    Ok(())
}
