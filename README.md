# archive-it-client

Rust client for Archive-It's partner API and WASAPI.

Inspiration and examples have been drawn from:

- <https://github.com/sul-dlss/wasapi_client>
- <https://github.com/unt-libraries/py-wasapi-client>
- <https://github.com/WASAPI-Community/data-transfer-apis/tree/master/ait-specification>

## Overview

There are three clients, each scoped to what its endpoints expose under that auth state:

```rust,no_run
use archive_it_client::{PageOpts, PartnerClient, PublicClient, WasapiClient, WebdataQuery};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = "user";
    let pass = "pass";

    // public — no auth, partner registry + public collections
    let public = PublicClient::new()?;
    let accounts = public.list_accounts(PageOpts::default()).await?;
    let collection = public.get_collection(2135).await?;

    // partner — auth scopes every call to your own account
    let partner = PartnerClient::new(user, pass)?;
    let me = partner.my_account().await?;
    let mine = partner.list_collections(PageOpts::default()).await?;

    // wasapi — WARC manifests for a collection
    let wasapi = WasapiClient::new(user, pass)?;
    let query = WebdataQuery {
        collection: Some(4472),
        ..Default::default()
    };
    let page = wasapi.list_webdata(&query).await?;

    Ok(())
}
```

Timeouts and retries (default: 30s, 3 attempts, 250ms exponential backoff; retries on 5xx, 429, timeouts, and connection errors) are configured via `Config`:

```rust,no_run
use std::time::Duration;
use archive_it_client::{Config, PartnerClient};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = "user";
    let pass = "pass";

    let mut cfg = Config::api();
    cfg.timeout = Duration::from_secs(10);
    cfg.max_attempts = 5;
    let client = PartnerClient::with_config(user, pass, cfg)?;

    Ok(())
}
```

## Pagination

There are two options: streaming for transparent pagination, per-page methods for manual
control. Streaming hides the offset/cursor bookkeeping for both API styles
behind a uniform `Stream<Item = Result<T, Error>>`.

### Streaming

Each list endpoint has a streaming variant. Pages are fetched lazily as items
are pulled; dropping the stream stops mid-traversal:

```rust,no_run
use archive_it_client::{PartnerClient, PublicClient, WasapiClient, WebdataQuery};
use futures::TryStreamExt;  // for try_collect / try_next / try_filter / ...

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = "user";
    let pass = "pass";
    let public = PublicClient::new()?;
    let partner = PartnerClient::new(user, pass)?;
    let wasapi = WasapiClient::new(user, pass)?;

    let all: Vec<_> = public.accounts().try_collect().await?;
    let mine: Vec<_> = partner.collections().try_collect().await?;

    let query = WebdataQuery {
        collection: Some(4472),
        ..Default::default()
    };
    let mut files = Box::pin(wasapi.webdata(query));
    while let Some(file) = files.try_next().await? {
        // process one file at a time
    }

    Ok(())
}
```

The streaming methods are:

| Client          | Method                                               |
| --------------- | ---------------------------------------------------- |
| `PublicClient`  | `accounts()`, `collections(account_id: Option<u64>)` |
| `PartnerClient` | `collections()`                                      |
| `WasapiClient`  | `webdata(query: WebdataQuery)`                       |

Internally, `PublicClient` and `PartnerClient` streams fetch 100 items per
request. `WasapiClient` defaults to `page_size=50` unless you override it in
`WebdataQuery`.

The streams expose the standard `futures_core::Stream` trait. To use the
extension methods shown above (`try_collect`, `try_next`, `try_filter`, `take`,
…) add a stream-utilities crate to your `Cargo.toml`:

```toml
[dependencies]
futures = "0.3"           # or tokio-stream = "0.1"
```

### Per-page

When you want to control page size or read pagination metadata
(WASAPI's `count`, `next`), use the lower-level methods:

```rust,no_run
use archive_it_client::{PageOpts, PublicClient, WasapiClient, WebdataQuery};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let public = PublicClient::new()?;
    let wasapi = WasapiClient::new("user", "pass")?;

    // /api — caller passes limit/offset, gets a Vec
    let batch = public
        .list_accounts(PageOpts { limit: Some(50), offset: Some(0) })
        .await?;

    // wasapi — server-driven cursor; follow `next` until exhausted
    let query = WebdataQuery {
        collection: Some(4472),
        ..Default::default()
    };
    let mut page = wasapi.list_webdata(&query).await?;
    println!("{} files total", page.count);
    loop {
        for file in &page.files { /* ... */ }
        match wasapi.list_webdata_next(&page).await? {
            Some(next) => page = next,
            None => break,
        }
    }

    Ok(())
}
```

## Downloads

Two destinations: local filesystem and S3. Both skip the fetch when the
destination already matches — by sha1 when WASAPI supplied one, otherwise
by file size. Every download method returns a `Stream` of `DownloadOutcome`
events — `Progress` / `Downloaded` / `Skipped` / `Failed` per file, plus
`StreamFailed` for errors that occur before a file is available — so callers
can render progress and react to failures uniformly, whether they're
downloading one file or a whole collection.

```rust,no_run
use std::pin::pin;
use archive_it_client::{WasapiClient, WebdataQuery};
use futures::{StreamExt, TryStreamExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wasapi = WasapiClient::new("user", "pass")?;

    // single file → ./out.warc.gz, with progress events
    let file = pin!(wasapi.webdata(WebdataQuery {
        collection: Some(4472),
        page_size: Some(1),
        ..Default::default()
    }))
    .try_next().await?.ok_or("empty")?;
    let mut single = pin!(wasapi.download(file, "./out.warc.gz"));
    while let Some(outcome) = single.next().await {
        println!("{outcome}");
    }

    // whole collection → ./warcs, also a stream of outcomes per file
    let query = WebdataQuery { collection: Some(4472), ..Default::default() };
    let mut stream = pin!(wasapi.download_collection(query, "./warcs"));
    while let Some(outcome) = stream.next().await {
        println!("{outcome}");
    }
    Ok(())
}
```

Local downloads use a `<filename>.part` sidecar so an interrupted run resumes
on the next invocation.

### S3

`WasapiClient::download_to_s3` and `download_collection_to_s3` accept a
pre-built `aws_sdk_s3::Client`, so credentials, region, and HTTP wiring stay
under your control. Multipart upload is driven internally with server-side
crc64nvme as the at-rest integrity contract; sha1 (when supplied by WASAPI)
is recorded as user metadata so subsequent runs can skip on match.

The S3 principal needs `s3:GetObject`, `s3:ListBucket`, `s3:PutObject`, and
`s3:AbortMultipartUpload` on the target.

## Examples

Runnable examples live under `examples/`:

```bash
# no auth — public partner registry
cargo run --example public

# partner API — needs ARCHIVE_IT_USERNAME/ARCHIVE_IT_PASSWORD set
ARCHIVE_IT_USERNAME=user ARCHIVE_IT_PASSWORD=pass cargo run --example partner

# wasapi — needs ARCHIVE_IT_USERNAME/ARCHIVE_IT_PASSWORD set
ARCHIVE_IT_USERNAME=user ARCHIVE_IT_PASSWORD=pass cargo run --example wasapi

# inventory every WARC exposed by WASAPI into ./warcs.csv
ARCHIVE_IT_USERNAME=user ARCHIVE_IT_PASSWORD=pass cargo run --example warcs_inventory

# tally total WARC bytes across every collection on the account
ARCHIVE_IT_USERNAME=user ARCHIVE_IT_PASSWORD=pass cargo run --example count_bytes

# download a collection to ./warcs (resumes via .part sidecars)
ARCHIVE_IT_USERNAME=user ARCHIVE_IT_PASSWORD=pass cargo run --example download_collection

# upload one WARC to S3 (uses standard AWS provider chain for creds)
ARCHIVE_IT_USERNAME=user ARCHIVE_IT_PASSWORD=pass S3_BUCKET=my-bucket \
    cargo run --example download_s3
```

The authenticated examples fail fast if `ARCHIVE_IT_USERNAME` or
`ARCHIVE_IT_PASSWORD` is unset.

## Fixtures

JSON fixtures under `fixtures/` are generated by `fixtures.sh`. It requires
`ARCHIVE_IT_USERNAME` and `ARCHIVE_IT_PASSWORD` (Archive-It partner
credentials) to be set:

```bash
ARCHIVE_IT_USERNAME=user ARCHIVE_IT_PASSWORD=pass ./fixtures.sh
```
