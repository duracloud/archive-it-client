# http-ferry

A resumable, checksum-verified, streaming byte-transfer engine: pull bytes from
an HTTP source and push them into a pluggable sink, hashing as you go. One sink
ships in the box (local file); another (S3 multipart upload) lives behind the
`s3` feature. The caller's own item type rides through untouched, the way
`reqwest` hands your response back to you.

The crate knows nothing about any specific service — you bring the URLs, the
auth, and (optionally) your own sink. It was extracted from `archive-it-client`,
which uses it to download WASAPI WARCs to disk or S3.

The name: the HTTP side is the *source* and `Sink` is the *destination*, so the
crate ferries bytes from one to the other.

## What it does

- **Resumable downloads** over HTTP range requests, including the awkward case
  where a server ignores `Range` and replies `200` instead of `206` (the sink
  is restarted and the byte counter resets).
- **Integrity verification** with a pluggable [`Checksum`] (sha1 / md5). The
  engine hashes the stream with the matching algorithm and fails on mismatch.
- **Skip-on-exists**: a sink can report the destination already holds the file
  (by checksum, or by size when no checksum is supplied) and the engine yields
  `Skipped` without fetching a byte.
- **Progress + per-item error isolation**: a `Stream` of [`Outcome`] events; one
  bad file in a batch yields `Failed` and the stream continues.
- **Retry** with exponential backoff, both at request setup and mid-stream.

## Core concepts

| Type | Role |
|------|------|
| `Downloader` | Owns the HTTP client, retry policy, and a request-customization hook (where you inject auth). |
| `Transfer<M>` | One unit of work: `size`, optional `checksum`, destination `name`, and your opaque `meta`. |
| `Target<'a>` | The borrowed view a sink sees: `name`, `size`, `checksum`. No URL, no `meta` — sinks are domain-agnostic. |
| `Sink` / `SinkFactory` | Where bytes go. Implement these to add a destination (disk, S3, GCS, a database BLOB…). |
| `Outcome<M, L>` | Per-item result stream: `Downloaded` / `Skipped` / `Progress` / `Failed` / `StreamFailed`. |
| `drive(..)` | The one driver. Pulls `Transfer`s, resolves each source URL, builds a sink, runs the download. |

The engine reads only three things off each item — `size`, `checksum`, `name` —
so your rich type (`M`) is never inspected; it is cloned into `Progress` events
and handed back in the terminal `Outcome`.

## Cargo features

- `s3` *(off by default)* — the S3 multipart-upload sink in the `s3` module. It
  pulls in the whole `aws-sdk-s3` dependency tree, so consumers who only
  download to disk don't pay for it. Enable with `features = ["s3"]`.

## Usage

Wire a `Downloader`, hand `drive` a stream of `Transfer`s, a closure that
resolves each item's source URL, and a `SinkFactory`:

```rust,ignore
use std::time::Duration;
use futures_util::StreamExt;
use http_ferry::{Checksum, Downloader, Outcome, Transfer, local::LocalDir};

// 1. An HTTP layer. The `customize` closure is for your auth — inject a bearer
//    token, basic auth, signed headers, or nothing.
let token = std::env::var("TOKEN")?;
let downloader = Downloader::new(
    reqwest::Client::builder().build()?,
    /* max_attempts */ 3,
    /* backoff */ Duration::from_millis(250),
    move |req| req.bearer_auth(&token),
);

// 2. A stream of work items. `meta` is whatever you want back in the outcome.
let items = futures_util::stream::iter(vec![Ok(Transfer {
    size: 1_048_576,
    checksum: Some(Checksum::Sha1("da39a3ee…".into())),
    name: "report.bin".into(),
    meta: (),
})]);

// 3. Drive it: resolve each item's URL, write into ./out via the local sink.
//    `create_all` makes the destination dir up front (it must already exist).
let mut out = std::pin::pin!(http_ferry::drive(
    &downloader,
    items,
    |t: &Transfer<()>| Ok(format!("https://example.com/files/{}", t.name).parse()?),
    LocalDir::create_all("./out")?,
));

while let Some(outcome) = out.next().await {
    match outcome {
        Outcome::Downloaded { location, verified, .. } => {
            println!("ok {} (verified={verified})", location.display());
        }
        Outcome::Progress { received, total, .. } => { /* update a bar */ }
        Outcome::Skipped { .. } => {}
        Outcome::Failed { error, .. } => eprintln!("file failed: {error}"),
        Outcome::StreamFailed { error } => eprintln!("fatal: {error}"),
    }
}
```

### Adding a destination

Implement [`Sink`] (per-file state machine) and [`SinkFactory`] (builds one sink
per item). The engine calls `prepare` once, then `write_chunk` repeatedly, then
`finalize` — or `restart` if the server forced a fresh download mid-stream.

```rust,ignore
use http_ferry::{Error, Hasher, Prepared, Sink, Target};

struct MemSink { name: String, buf: Vec<u8> }

impl Sink for MemSink {
    type Location = String; // identifies where the bytes landed

    async fn prepare(&mut self, target: Target<'_>) -> Result<Prepared<String>, Error> {
        // Inspect target.checksum / target.size to decide skip-vs-fetch.
        // Return a `Hasher` matching the expected checksum so resumed
        // downloads keep hashing from where they left off.
        Ok(Prepared::Resume { received: 0, partial: Hasher::for_checksum(target.checksum) })
    }

    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), Error> {
        self.buf.extend_from_slice(chunk);
        Ok(())
    }

    async fn restart(&mut self) -> Result<(), Error> { self.buf.clear(); Ok(()) }

    async fn finalize(self) -> Result<String, Error> { Ok(self.name) }
}
```

`Location` types implement [`DownloadLocation`] so the engine can render where a
file went. To get `Display` on the outcomes, implement [`Label`] on your `meta`
type `M` (it supplies the filename used in log lines).

## Design notes

- **Auth is a closure, not a credential type.** `Downloader` never
  names "basic auth" or "bearer token" — the consumer supplies a
  `Fn(RequestBuilder) -> RequestBuilder`. This keeps the engine free
  of any service's auth model.
- **URL resolution is a per-item closure passed to `drive`.** Resolution can
  fail per item (yielding a non-fatal `Failed`) without tearing down the
  stream; a failure pulling the *next* item from the source yields a fatal
  `StreamFailed`.
- **Caller errors flow in through [`Error::Source`].** The resolver and the
  input item stream produce the *caller's* error type. The engine type-erases
  them through `Source(Box<dyn Error + Send + Sync>)`, so it never needs to know
  a consumer's domain errors; callers recover the original by `downcast`.
- **No auto-abort of interrupted uploads.** Rust has no `AsyncDrop`, so a sink
  that leaves server-side state (e.g. an S3 multipart upload) documents how to
  garbage-collect it rather than attempting brittle cleanup on drop. The S3 sink
  also defers `CreateMultipartUpload` to the first byte, so a source error
  before any data arrives leaves nothing behind.

[`Checksum`]: crate::Checksum
[`Outcome`]: crate::Outcome
[`Sink`]: crate::Sink
[`SinkFactory`]: crate::SinkFactory
[`DownloadLocation`]: crate::DownloadLocation
[`Label`]: crate::Label
[`Error::Source`]: crate::Error::Source
