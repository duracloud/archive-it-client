use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::path::Path;
use std::pin::pin;

use archive_it_client::wasapi::DEFAULT_WEBDATA_PAGE_SIZE;
use archive_it_client::{PartnerClient, WasapiClient, WebdataQuery};
use csv::{ReaderBuilder, Terminator, Writer, WriterBuilder};
use futures::TryStreamExt;

const OUTPUT_PATH: &str = "warcs.csv";
const COLLECTION_ID_COL: usize = 0;
const FILENAME_COL: usize = 3;
const HEADER: [&str; 14] = [
    "collection_id",
    "collection_name",
    "account_id",
    "filename",
    "filetype",
    "size_bytes",
    "crawl_id",
    "crawl_time",
    "crawl_start",
    "store_time",
    "sha1",
    "md5",
    "primary_location",
    "all_locations",
];

/// Resume state derived from an existing inventory CSV.
#[derive(Debug, Default)]
struct CacheState {
    /// Cached file count per collection_id.
    counts: HashMap<u64, u64>,
    /// (collection_id, filename) of rows already in the CSV.
    seen: HashSet<(u64, String)>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user =
        std::env::var("ARCHIVE_IT_USERNAME").expect("ARCHIVE_IT_USERNAME env var must be set");
    let pass =
        std::env::var("ARCHIVE_IT_PASSWORD").expect("ARCHIVE_IT_PASSWORD env var must be set");

    let partner = PartnerClient::new(user.clone(), pass.clone())?;
    let wasapi = WasapiClient::new(user, pass)?;

    let header_needed = std::fs::metadata(OUTPUT_PATH)
        .map(|m| m.len() == 0)
        .unwrap_or(true);
    let CacheState {
        counts: cache_counts,
        mut seen,
    } = if header_needed {
        CacheState::default()
    } else {
        read_cache_state(Path::new(OUTPUT_PATH))?
    };
    if !seen.is_empty() {
        eprintln!(
            "resuming: {} files cached across {} collections in {OUTPUT_PATH}",
            seen.len(),
            cache_counts.len()
        );
    }

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(OUTPUT_PATH)?;
    let mut writer: Writer<_> = WriterBuilder::new()
        .terminator(Terminator::Any(b'\n'))
        .from_writer(file);
    if header_needed {
        writer.write_record(HEADER)?;
        writer.flush()?;
    }

    let mut collections = pin!(partner.collections());
    let mut collection_count = 0_u64;
    let mut collection_skipped = 0_u64;
    let mut warc_count = 0_u64;
    let mut cache_hit_count = 0_u64;

    while let Some(collection) = collections.try_next().await? {
        let cached = cache_counts.get(&collection.id).copied().unwrap_or(0);

        let query = WebdataQuery {
            collection: Some(collection.id),
            filetype: Some("warc".to_owned()),
            page_size: Some(DEFAULT_WEBDATA_PAGE_SIZE),
            ..Default::default()
        };

        let mut page = wasapi.list_webdata(&query).await?;
        let total = page.count;

        // First-page short-circuit: skip only when the cache is exactly in
        // sync. Net-decrease (cached > total) means deletions on the API
        // side, which can hide additions — paginate fully to catch them.
        // (Aside: I don't know if this is a real concern)
        if total > 0 && cached == total {
            collection_skipped += 1;
            cache_hit_count += cached;
            eprintln!(
                "collection {} ({}) -> all {} cached, skipping",
                collection.id, collection.name, total
            );
            continue;
        }

        if cached > total {
            eprintln!(
                "collection {} ({}) -> cache has {} entries but API reports {} \
                 (possible deletions); paginating fully to catch any additions",
                collection.id, collection.name, cached, total
            );
        }

        collection_count += 1;
        if cached > 0 {
            eprintln!(
                "collection {} ({}) -> {} reported WARC files ({} cached, ~{} to fetch)",
                collection.id,
                collection.name,
                total,
                cached,
                total.saturating_sub(cached)
            );
        } else {
            eprintln!(
                "collection {} ({}) -> {} reported WARC files",
                collection.id, collection.name, total
            );
        }

        let expected_new = total.saturating_sub(cached);
        let mut page_num = 0_u32;
        let mut written_in_collection = 0_u64;
        let mut cached_in_collection = 0_u64;

        loop {
            page_num += 1;
            let mut written_this_page = 0_u64;
            let mut cached_this_page = 0_u64;

            for file in page.files.drain(..) {
                if file.filetype != "warc" {
                    continue;
                }
                if !seen.insert((collection.id, file.filename.clone())) {
                    cached_this_page += 1;
                    continue;
                }

                let primary_location = wasapi
                    .primary_location(&file)
                    .unwrap_or_default()
                    .to_owned();
                let all_locations = file.locations.join(";");

                writer.write_record([
                    collection.id.to_string(),
                    collection.name.clone(),
                    file.account.to_string(),
                    file.filename,
                    file.filetype,
                    file.size.to_string(),
                    opt_u64(file.crawl),
                    opt_string(file.crawl_time),
                    opt_string(file.crawl_start),
                    file.store_time,
                    opt_string(file.checksums.sha1),
                    opt_string(file.checksums.md5),
                    primary_location,
                    all_locations,
                ])?;
                written_this_page += 1;
            }

            // Flush after each page so successfully-fetched rows are durable
            // even if the next page errors out.
            writer.flush()?;

            written_in_collection += written_this_page;
            cached_in_collection += cached_this_page;
            let processed = written_in_collection + cached_in_collection;
            let pct = if total == 0 {
                100.0
            } else {
                (processed as f64 / total as f64) * 100.0
            };
            eprintln!(
                "  collection {} page {}: +{} new, {} cached \
                 (collection {}/{} = {:.1}%)",
                collection.id, page_num, written_this_page, cached_this_page, processed, total, pct
            );

            // Inner short-circuit: once we've found at least the number of
            // new files the API's count delta implies, stop paginating.
            if expected_new > 0 && written_in_collection >= expected_new {
                eprintln!(
                    "  collection {} found {} new (≥ expected {}), stopping early",
                    collection.id, written_in_collection, expected_new
                );
                break;
            }

            match wasapi.list_webdata_next(&page).await? {
                Some(next) => page = next,
                None => break,
            }
        }

        warc_count += written_in_collection;
        cache_hit_count += cached_in_collection;
    }

    writer.flush()?;
    eprintln!(
        "wrote {warc_count} new WARC rows across {collection_count} collections \
         (skipped {collection_skipped} fully-cached collections, \
         {cache_hit_count} cached rows total) to {OUTPUT_PATH}"
    );

    Ok(())
}

fn read_cache_state(path: &Path) -> Result<CacheState, csv::Error> {
    let mut rdr = ReaderBuilder::new().has_headers(true).from_path(path)?;
    let mut state = CacheState::default();
    for record in rdr.records() {
        let record = record?;
        let Some(id_str) = record.get(COLLECTION_ID_COL) else {
            continue;
        };
        let Ok(collection_id) = id_str.parse::<u64>() else {
            continue;
        };
        let Some(filename) = record.get(FILENAME_COL) else {
            continue;
        };
        if filename.is_empty() {
            continue;
        }

        if state.seen.insert((collection_id, filename.to_owned())) {
            *state.counts.entry(collection_id).or_insert(0) += 1;
        }
    }
    Ok(state)
}

fn opt_string(value: Option<String>) -> String {
    value.unwrap_or_default()
}

fn opt_u64(value: Option<u64>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}
