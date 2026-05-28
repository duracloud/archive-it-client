use std::ffi::OsStr;
use std::path::{Component, Path, PathBuf};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{Checksum, Error, Hasher, Prepared, Sink, SinkFactory, Target};

/// Collection destination: writes each file to `dir/<name>`. `dir` must already
/// exist before driving — destination preflight is not a per-item concern (a bad
/// output path should fail the stream once, not once per file). Use
/// [`LocalDir::create_all`] to create it up front.
pub struct LocalDir {
    pub dir: PathBuf,
}

impl LocalDir {
    /// Build a `LocalDir`, creating `dir` (and parents) first. A cheap one-time
    /// synchronous preflight (a single `std::fs::create_dir_all`); fine to call
    /// from async setup code before entering the transfer loop.
    pub fn create_all(dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }
}

impl SinkFactory for LocalDir {
    type Sink = LocalSink;
    type Location = PathBuf;

    async fn make(&mut self, target: Target<'_>) -> Result<LocalSink, Error> {
        // `name` comes from the source listing, not the caller, so it is
        // untrusted. `Path::join` silently escapes the directory on an absolute
        // path or `..`, so require a single, plain filename component.
        let name = single_component(target.name).ok_or_else(|| Error::InvalidDownloadPath {
            path: PathBuf::from(target.name),
        })?;
        LocalSink::new(self.dir.join(name))
    }
}

/// Returns the lone normal filename component of `name`, or `None` if `name` is
/// empty, absolute, contains a separator, or includes `.`/`..` — anything that
/// could place the output outside the destination directory.
fn single_component(name: &str) -> Option<&OsStr> {
    let mut components = Path::new(name).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(c)), None) => Some(c),
        _ => None,
    }
}

/// Singular destination: writes one file to a fixed, caller-chosen path.
pub struct LocalPath {
    pub path: PathBuf,
}

impl SinkFactory for LocalPath {
    type Sink = LocalSink;
    type Location = PathBuf;

    async fn make(&mut self, _target: Target<'_>) -> Result<LocalSink, Error> {
        LocalSink::new(self.path.clone())
    }
}

pub struct LocalSink {
    final_path: PathBuf,
    partial_path: PathBuf,
    out: Option<tokio::fs::File>,
}

impl LocalSink {
    pub(crate) fn new(final_path: PathBuf) -> Result<Self, Error> {
        let partial_path = partial_path(&final_path)?;
        Ok(Self {
            final_path,
            partial_path,
            out: None,
        })
    }
}

impl Sink for LocalSink {
    type Location = PathBuf;

    async fn prepare(&mut self, target: Target<'_>) -> Result<Prepared<Self::Location>, Error> {
        if existing_matches_destination(&self.final_path, target.checksum, target.size).await? {
            return Ok(Prepared::Skip {
                location: self.final_path.clone(),
            });
        }

        let (received, partial) =
            examine_partial(&self.partial_path, target.size, target.checksum).await?;
        let out = if received > 0 {
            tokio::fs::OpenOptions::new()
                .append(true)
                .open(&self.partial_path)
                .await?
        } else {
            tokio::fs::File::create(&self.partial_path).await?
        };
        self.out = Some(out);
        Ok(Prepared::Resume { received, partial })
    }

    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), Error> {
        let out = self
            .out
            .as_mut()
            .expect("write_chunk before prepare or after finalize");
        out.write_all(chunk).await?;
        Ok(())
    }

    async fn restart(&mut self) -> Result<(), Error> {
        let replacement = tokio::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.partial_path)
            .await?;
        self.out = Some(replacement);
        Ok(())
    }

    async fn finalize(mut self) -> Result<Self::Location, Error> {
        let out = self.out.take().expect("finalize without prepare");
        out.sync_all().await?;
        drop(out);
        tokio::fs::rename(&self.partial_path, &self.final_path).await?;
        Ok(self.final_path)
    }
}

// Skip-existing rule:
// - checksum from the caller: only a hash match is good enough.
// - no checksum: fall back to size match. Re-downloading a multi-GB WARC every
//   run when no hash was supplied is the alternative; size is the cheapest
//   evidence we have.
async fn existing_matches_destination(
    path: &Path,
    checksum: Option<&Checksum>,
    size: u64,
) -> Result<bool, Error> {
    if !tokio::fs::try_exists(path).await? {
        return Ok(false);
    }
    match checksum {
        Some(expected) => {
            let mut hasher = Hasher::for_checksum(Some(expected));
            seed_hasher_from_file(path, &mut hasher).await?;
            Ok(hasher.finalize_hex().as_deref() == Some(expected.hex()))
        }
        None => {
            let m = tokio::fs::metadata(path).await?;
            Ok(m.len() == size)
        }
    }
}

async fn examine_partial(
    partial_path: &Path,
    expected_size: u64,
    checksum: Option<&Checksum>,
) -> Result<(u64, Hasher), Error> {
    if !tokio::fs::try_exists(partial_path).await? {
        return Ok((0, Hasher::for_checksum(checksum)));
    }
    let m = tokio::fs::metadata(partial_path).await?;
    if m.len() > expected_size {
        tokio::fs::remove_file(partial_path).await?;
        return Ok((0, Hasher::for_checksum(checksum)));
    }
    let mut hasher = Hasher::for_checksum(checksum);
    if m.len() > 0 {
        seed_hasher_from_file(partial_path, &mut hasher).await?;
    }
    Ok((m.len(), hasher))
}

async fn seed_hasher_from_file(path: &Path, hasher: &mut Hasher) -> Result<(), Error> {
    let mut f = tokio::fs::File::open(path).await?;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(())
}

fn partial_path(path: &Path) -> Result<PathBuf, Error> {
    let mut file_name = path
        .file_name()
        .ok_or_else(|| Error::InvalidDownloadPath { path: path.into() })?
        .to_os_string();
    file_name.push(".part");
    Ok(path.with_file_name(file_name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_path_appends_part_suffix() {
        let result = partial_path(Path::new("/tmp/foo.warc.gz")).unwrap();
        assert_eq!(result, PathBuf::from("/tmp/foo.warc.gz.part"));
    }

    #[test]
    fn partial_path_rejects_path_with_no_filename() {
        let err = partial_path(Path::new("/")).unwrap_err();
        assert!(matches!(err, Error::InvalidDownloadPath { .. }));
    }

    #[test]
    fn single_component_accepts_a_plain_filename() {
        assert_eq!(
            single_component("foo.warc.gz"),
            Some(OsStr::new("foo.warc.gz"))
        );
    }

    #[test]
    fn single_component_rejects_traversal_and_multi_component_names() {
        assert!(single_component("").is_none());
        assert!(single_component("..").is_none());
        assert!(single_component(".").is_none());
        assert!(single_component("/etc/passwd").is_none());
        assert!(single_component("../../etc/passwd").is_none());
        assert!(single_component("sub/dir/file.warc.gz").is_none());
    }
}
