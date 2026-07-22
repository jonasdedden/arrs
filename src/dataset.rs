use std::fmt::Debug;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;

use crate::Result;
use crate::cli::LanceArgs;
use crate::error::Error;

/// The name Lance's implicit default branch is surfaced under. Lance stores it
/// as `None` internally; the adapter and commands normalise that to `"main"`.
pub const MAIN_BRANCH: &str = "main";

/// Stream of `RecordBatch` results produced by a scan.
pub type BatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

/// Options controlling a `scan()`.
///
/// Passed as a struct (rather than a growing list of positional parameters) so
/// that new knobs can be added without churning every call site. Today it
/// carries a column projection and an optional row predicate; row-id emission
/// is expected to land here next. All fields are borrowed, so the struct is
/// cheap to `Copy` and construct inline at each command.
#[derive(Debug, Default, Clone, Copy)]
pub struct ScanOptions<'a> {
    /// Columns to include, in the given order. `None` means all columns.
    pub projection: Option<&'a [String]>,
    /// SQL-style predicate. Only matching rows are produced, and the filter is
    /// applied *before* any positional selection the command performs. `None`
    /// means no filtering.
    pub filter: Option<&'a str>,
}

/// Format-agnostic dataset view used by every command.
///
/// Input-format adapters (Lance today, potentially others in the future) implement this trait.
/// Commands are written against the trait only — they never see format-specific types.
#[async_trait]
pub trait Dataset: Send + Sync + Debug {
    /// Path or URI the dataset was opened from.
    fn origin(&self) -> &str;

    /// Logical arrow schema of the dataset.
    fn arrow_schema(&self) -> SchemaRef;

    /// Pretty-printed format-native schema (for `schema --type physical`), optionally
    /// projected to a subset of columns.
    fn physical_schema_debug(&self, projection: Option<&[String]>) -> Result<String>;

    /// Total row count, optionally restricted to rows matching `filter` (a
    /// SQL-style predicate). Adapters that can count through an index (Lance)
    /// should do so rather than scanning.
    async fn count_rows(&self, filter: Option<&str>) -> Result<u64>;

    /// Stream rows according to `options` (projection + optional filter).
    async fn scan(&self, options: &ScanOptions<'_>) -> Result<BatchStream>;

    /// Materialise a `RecordBatch` containing only the rows at the given indices,
    /// in the order given. `indices` must all be < `count_rows()`.
    async fn take(&self, indices: &[u64], projection: Option<&[String]>) -> Result<RecordBatch>;

    /// Returns `Some(...)` when this dataset is backed by a format that supports
    /// Lance-specific operations (versions, branches, indices). The default
    /// `None` covers any future format that doesn't.
    fn lance(&self) -> Option<&dyn LanceCapabilities> {
        None
    }

    /// Optional metadata-derived per-column statistics (the `stats` command).
    ///
    /// The default returns `None`, meaning "no shortcut — compute by scanning".
    /// A backend that stores fragment-level statistics (Lance keeps min/max and
    /// null counts per fragment) can override this to answer without a full
    /// scan, mirroring the `lance()` capability hook. The streaming fallback in
    /// `crate::stats::compute` is always correct, so overriding is a pure
    /// optimisation. `options` carries the same projection + filter the scan
    /// fallback would use.
    async fn stats(&self, _options: &ScanOptions<'_>) -> Option<Result<Vec<ColumnStats>>> {
        None
    }
}

/// Lance-specific operations exposed beyond the format-agnostic `Dataset` trait.
#[async_trait]
pub trait LanceCapabilities: Send + Sync {
    /// Manifest (dataset) version number of the currently checked-out version.
    /// Pure metadata already resident after `open`, so this is infallible and
    /// synchronous. Used by the `stat` command's `format` line.
    fn manifest_version(&self) -> u64;

    /// List versions on `branch` (defaults to `main` when `None`). When
    /// `tagged_only` is true, drops untagged versions from the result.
    async fn list_versions(
        &self,
        branch: Option<&str>,
        tagged_only: bool,
    ) -> Result<Vec<VersionInfo>>;

    /// List every branch the dataset has, including the default `main`.
    async fn list_branches(&self) -> Result<Vec<BranchInfo>>;

    /// List indices defined on the active version of the dataset.
    async fn list_indices(&self) -> Result<Vec<IndexInfo>>;

    /// Per-index coverage statistics: indexed vs unindexed row counts (which
    /// diverge as rows are appended after an index is built), plus the raw
    /// Lance statistics JSON so callers can pass through type-specific internals
    /// (IVF partitions, PQ sub-vectors, …) without arrs understanding them.
    async fn index_stats(&self) -> Result<Vec<IndexStats>>;

    /// List every tag in the dataset, regardless of branch.
    async fn list_tags(&self) -> Result<Vec<TagInfo>>;

    /// List the physical fragments of the active version of the dataset.
    ///
    /// Row counts, deletion counts and file lists come straight from the
    /// manifest, so this stays fast regardless of dataset size. When
    /// `with_size` is true, each fragment's on-disk byte size is also
    /// computed — from the manifest when the size is cached there, otherwise
    /// via concurrent object-store lookups. Pass `false` to skip that entirely
    /// (leaving `FragmentInfo::size` as `None`) for very remote or huge datasets.
    async fn list_fragments(&self, with_size: bool) -> Result<Vec<FragmentInfo>>;

    /// Nearest-neighbor vector search over a `FixedSizeList`-of-float column.
    ///
    /// Uses an ANN index when one exists on the column and falls back to flat
    /// (brute-force) KNN otherwise; `VectorSearchResult::used_index` reports
    /// which path was taken. The query vector is validated against the column
    /// width and cast to the column's element type by the adapter.
    async fn search(&self, params: &VectorSearchParams<'_>) -> Result<VectorSearchResult>;

    /// The `(branch, version)` this handle is currently checked out to.
    ///
    /// Read straight from the loaded manifest (no I/O). Used by `diff` to label
    /// each endpoint and to detect a cross-branch comparison after both handles
    /// have been opened and any tag/branch selectors resolved.
    fn checkout_state(&self) -> CheckoutState;

    /// True when `column` is a Lance blob-encoded column (`lance-encoding:blob`
    /// field metadata). Such columns store payloads too large to materialize
    /// through a normal scan/`take`, so the `blob` command reads them via
    /// [`Self::open_blob`] instead. Returns `false` for a missing column too;
    /// the caller validates existence separately against the arrow schema.
    fn is_blob_column(&self, column: &str) -> bool;

    /// Open a streaming reader over the blob payload at row offset `index` in
    /// the blob-encoded `column`. `index` is a resolved (non-negative) offset.
    /// `Ok(None)` means the cell is null (no payload to extract). Bytes are
    /// pulled lazily so multi-GB payloads never need to be held in memory.
    async fn open_blob(&self, column: &str, index: u64) -> Result<Option<Box<dyn BlobRead>>>;
}

/// Streaming reader over a single Lance blob cell's payload.
///
/// Bytes are pulled in bounded chunks rather than materialized all at once, so
/// extracting a multi-GB payload stays within a fixed memory budget. Backed by
/// Lance's `BlobFile` in the Lance adapter.
#[async_trait]
pub trait BlobRead: Send {
    /// Read up to `max` bytes from the current cursor, advancing it. Returns an
    /// empty buffer once the payload is exhausted.
    async fn read_chunk(&mut self, max: usize) -> Result<Vec<u8>>;
}

/// The resolved branch and version of an opened Lance handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckoutState {
    pub branch: String,
    pub version: u64,
}

/// Parameters for an `arrs search` nearest-neighbor query.
#[derive(Debug)]
pub struct VectorSearchParams<'a> {
    /// Vector column to search (a `FixedSizeList` of f16/f32/f64).
    pub column: &'a str,
    /// Query vector. Parsed from JSON and carried as f32 (Lance casts it to the
    /// column's element type); validated against the column width.
    pub vector: &'a [f32],
    /// Number of nearest neighbors to return.
    pub k: usize,
    /// IVF partitions to probe (`None` → Lance default). No effect without an index.
    pub nprobes: Option<usize>,
    /// Refine factor for re-ranking (`None` → no refinement).
    pub refine_factor: Option<u32>,
    /// Output column projection; `_distance` is always appended regardless.
    pub projection: Option<&'a [String]>,
}

/// Outcome of a vector search: the output schema (including the trailing
/// `_distance` column), the row stream, and whether an ANN index was used.
pub struct VectorSearchResult {
    pub schema: SchemaRef,
    pub stream: BatchStream,
    /// `false` when no ANN index covers the column and flat KNN was used.
    pub used_index: bool,
}

/// One row in `arrs versions` output.
#[derive(Debug, Clone)]
pub struct VersionInfo {
    pub version: u64,
    pub timestamp: DateTime<Utc>,
    pub tag: Option<String>,
    pub message: Option<String>,
}

/// One row in `arrs branches` output.
#[derive(Debug, Clone)]
pub struct BranchInfo {
    pub name: String,
    pub parent_branch: Option<String>,
    pub parent_version: Option<u64>,
    pub created_at: Option<DateTime<Utc>>,
}

/// One row in `arrs indices` output.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    /// Index type as Lance reports it (e.g. `BTree`, `IVF_PQ`, `INVERTED`).
    pub index_type: String,
    pub uuid: String,
    pub columns: Vec<String>,
    pub dataset_version: u64,
    pub created_at: Option<DateTime<Utc>>,
}

/// One row in `arrs stats` output: summary statistics for a single column.
///
/// Statistics that don't apply to a column's type are `None` and render as a
/// blank cell. `count` is the number of non-null values (as in `df.describe()`),
/// so `count + nulls` is the total number of rows considered.
#[derive(Debug, Clone)]
pub struct ColumnStats {
    /// Column name.
    pub column: String,
    /// Human-readable arrow type (e.g. `Int32`, `Timestamp(Microsecond, Some("UTC"))`).
    pub data_type: String,
    /// Number of non-null values.
    pub count: u64,
    /// Number of null values.
    pub nulls: u64,
    /// Minimum value, pre-formatted for display. Numeric, temporal, string, and
    /// boolean columns only.
    pub min: Option<String>,
    /// Maximum value, pre-formatted for display. Same type coverage as `min`.
    pub max: Option<String>,
    /// Arithmetic mean. Numeric columns only. `NaN` when the column contains any
    /// `NaN` (matching numpy's plain mean).
    pub mean: Option<f64>,
    /// Sample standard deviation (ddof = 1). Numeric columns with at least two
    /// non-null values only.
    pub stddev: Option<f64>,
    /// Distinct-value count, either exact (e.g. `42`) or a capped marker
    /// (e.g. `>10000`) once cardinality exceeds the tracking cap.
    pub distinct: Option<String>,
}

/// One row in `arrs index-stats` output.
#[derive(Debug, Clone)]
pub struct IndexStats {
    pub name: String,
    /// Index type as Lance reports it (e.g. `BTree`, `IVF_PQ`).
    pub index_type: String,
    /// Rows currently covered by the index.
    pub indexed_rows: u64,
    /// Rows appended after the index was built and not yet reindexed.
    pub unindexed_rows: u64,
    /// Raw Lance statistics JSON string, passed through for type-specific
    /// internals. Emitted verbatim in `jsonl` output; omitted from table/csv.
    pub detail: String,
}

impl IndexStats {
    /// Fraction of rows covered by the index in `0.0..=1.0`, or `None` when the
    /// index has no rows at all (coverage is undefined).
    pub fn coverage(&self) -> Option<f64> {
        let total = self.indexed_rows + self.unindexed_rows;
        (total > 0).then(|| self.indexed_rows as f64 / total as f64)
    }
}

/// One row in `arrs tags` output.
#[derive(Debug, Clone)]
pub struct TagInfo {
    pub name: String,
    pub branch: String,
    pub version: u64,
}

/// One row in `arrs fragments` output.
#[derive(Debug, Clone)]
pub struct FragmentInfo {
    /// Fragment id, unique and stable within the dataset.
    pub id: u64,
    /// Rows physically stored in the fragment, ignoring deletions.
    pub physical_rows: u64,
    /// Rows tombstoned by the fragment's deletion file (0 when there is none).
    pub deleted_rows: u64,
    /// Number of data files backing the fragment.
    pub num_files: u64,
    /// Relative paths of the fragment's data files.
    pub files: Vec<String>,
    /// Summed on-disk size of the data files in bytes, or `None` when size
    /// computation was skipped (see `LanceCapabilities::list_fragments`).
    pub size: Option<u64>,
}

/// Open a dataset at `input`, optionally checking out a specific Lance
/// branch/version/tag. `input` is either a local path (`/data/foo.lance`,
/// with or without a `file://` prefix) or an object-store URI (`s3://…`,
/// `gs://…`, `az://…`). Returns an error if the dataset is not Lance and any
/// `LanceArgs` field is set.
pub async fn open(input: &str, lance: Option<&LanceArgs>) -> Result<Arc<dyn Dataset>> {
    match detect_format(input)? {
        Format::Lance => {
            let ds = crate::lance::LanceDataset::open(input, lance).await?;
            Ok(Arc::new(ds))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Lance,
}

/// Determine which adapter should be used for `input`.
///
/// Scheme-qualified inputs (`s3://`, `gs://`, `az://`, `file://`, …) are
/// resolved by the object store rather than the local filesystem, so we can't
/// probe them cheaply here. Instead we defer to the adapter's own `open`, which
/// maps any failure to a URI-bearing error — this avoids duplicating Lance's
/// object-store resolution logic. Scheme-less inputs are local paths and keep
/// the `_versions/` directory heuristic, so a future non-Lance format can still
/// be dispatched from here.
///
/// Today we only support Lance; the function exists in match shape so a Parquet
/// (or other) arm can be added without touching call sites.
pub fn detect_format(input: &str) -> Result<Format> {
    if has_scheme(input) || is_lance_dataset(input) {
        Ok(Format::Lance)
    } else {
        Err(Error::UnknownFormat {
            path: input.to_string(),
        })
    }
}

/// True when `input` starts with a URI scheme followed by `://`
/// (e.g. `s3://bucket/…`, `file:///data/…`). A bare Windows drive letter such
/// as `C:\data` has no `//` and is therefore correctly treated as a local path.
fn has_scheme(input: &str) -> bool {
    let Some((scheme, _rest)) = input.split_once("://") else {
        return false;
    };
    !scheme.is_empty()
        && scheme.starts_with(|c: char| c.is_ascii_alphabetic())
        && scheme
            .chars()
            .all(|c: char| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
}

fn is_lance_dataset(input: &str) -> bool {
    // A Lance dataset is a directory that contains a `_versions/` subfolder.
    // (`_transactions/` is the other typical marker but not always present in
    // freshly written datasets.)
    let p = Path::new(input);
    p.is_dir() && p.join("_versions").is_dir()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheme_detection_matches_object_store_uris() {
        for uri in [
            "s3://bucket/data.lance",
            "gs://analytics/events.lance",
            "az://container/data.lance",
            "file:///data/foo.lance",
            "s3+http://host/bucket/data",
        ] {
            assert!(has_scheme(uri), "expected scheme in {uri:?}");
        }
    }

    #[test]
    fn scheme_detection_rejects_local_paths() {
        for path in [
            "/data/foo.lance",
            "./relative/foo.lance",
            "foo.lance",
            "C:\\data\\foo.lance",
            "",
            "://missing-scheme",
        ] {
            assert!(!has_scheme(path), "did not expect scheme in {path:?}");
        }
    }

    #[test]
    fn scheme_qualified_input_dispatches_to_lance_without_touching_fs() {
        // No filesystem access happens for scheme-qualified inputs; a bucket URI
        // that obviously doesn't exist locally still resolves to `Lance`.
        assert_eq!(
            detect_format("s3://no-such-bucket/data.lance").unwrap(),
            Format::Lance,
        );
    }

    #[test]
    fn scheme_less_nonexistent_local_path_is_unknown_format() {
        let err = detect_format("/definitely/not/a/dataset/xyz").unwrap_err();
        match err {
            Error::UnknownFormat { path } => {
                assert_eq!(path, "/definitely/not/a/dataset/xyz");
            }
            other => panic!("expected UnknownFormat, got {other:?}"),
        }
    }
}
