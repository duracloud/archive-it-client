use std::env;
use std::io::{self, Write};
use std::pin::pin;

use archive_it_client::{DownloadOutcome, WasapiClient, WebdataQuery};
use aws_config::BehaviorVersion;
use futures::{StreamExt, TryStreamExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = env::var("ARCHIVE_IT_USERNAME").expect("ARCHIVE_IT_USERNAME env var must be set");
    let pass = env::var("ARCHIVE_IT_PASSWORD").expect("ARCHIVE_IT_PASSWORD env var must be set");
    let bucket = env::var("S3_BUCKET").expect("S3_BUCKET env var must be set");
    let prefix = env::var("S3_KEY_PREFIX").ok().filter(|s| !s.is_empty());
    let collection: u64 = env::var("COLLECTION_ID")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4472);

    // AWS credentials and region come from the standard provider chain:
    // env vars (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION),
    // shared profile (~/.aws/credentials, ~/.aws/config), or IMDS on EC2.
    // The principal needs s3:GetObject, s3:ListBucket, s3:PutObject, and
    // s3:AbortMultipartUpload on the target.
    let aws_cfg = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let s3 = aws_sdk_s3::Client::new(&aws_cfg);

    let client = WasapiClient::new(user, pass)?;
    // page_size: 1 fetches just the first WARC of the collection so the
    // example exits after one upload. Drop this and iterate the stream
    // for full-collection pulls.
    let query = WebdataQuery {
        collection: Some(collection),
        filetype: Some("warc".into()),
        page_size: Some(1),
        ..Default::default()
    };

    let file = pin!(client.webdata(query))
        .try_next()
        .await?
        .ok_or("collection contains no files")?;

    let key = match &prefix {
        Some(p) => format!("{p}{}", file.filename),
        None => file.filename.clone(),
    };
    println!(
        "uploading {} ({} bytes) → s3://{bucket}/{key}",
        file.filename, file.size
    );

    let mut stream = pin!(client.download_to_s3(file, s3, bucket, prefix));
    let mut showed_progress = false;
    while let Some(outcome) = stream.next().await {
        match &outcome {
            DownloadOutcome::Progress { .. } => {
                print!("\r\x1b[2K{outcome}");
                io::stdout().flush()?;
                showed_progress = true;
            }
            _ => {
                if showed_progress {
                    println!();
                    showed_progress = false;
                }
                println!("{outcome}");
            }
        }
    }

    Ok(())
}
