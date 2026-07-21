use std::fmt::Debug;
use std::path::{Path, PathBuf};
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
    fn origin(&self) -> &Path;

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
}

/// Lance-specific operations exposed beyond the format-agnostic `Dataset` trait.
#[async_trait]
pub trait LanceCapabilities: Send + Sync {
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
    pub uuid: String,
    pub columns: Vec<String>,
    pub dataset_version: u64,
    pub created_at: Option<DateTime<Utc>>,
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

/// Open a dataset at `path`, optionally checking out a specific Lance
/// branch/version/tag. Returns an error if the dataset is not Lance and any
/// `LanceArgs` field is set.
pub async fn open(path: &Path, lance: Option<&LanceArgs>) -> Result<Arc<dyn Dataset>> {
    match detect_format(path)? {
        Format::Lance => {
            let ds = crate::lance::LanceDataset::open(path, lance).await?;
            Ok(Arc::new(ds))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Lance,
}

/// Determine which adapter should be used for `path`. Today we only support
/// Lance; the function exists in match shape so a Parquet (or other) arm can
/// be added without touching call sites.
pub fn detect_format(path: &Path) -> Result<Format> {
    if is_lance_dataset(path) {
        Ok(Format::Lance)
    } else {
        Err(Error::UnknownFormat {
            path: path.to_path_buf(),
        })
    }
}

fn is_lance_dataset(path: &Path) -> bool {
    // A Lance dataset is a directory that contains a `_versions/` subfolder.
    // (`_transactions/` is the other typical marker but not always present in
    // freshly written datasets.)
    let p: PathBuf = path.into();
    p.is_dir() && p.join("_versions").is_dir()
}
