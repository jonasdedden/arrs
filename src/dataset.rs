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

    /// Total row count.
    async fn count_rows(&self) -> Result<u64>;

    /// Stream all rows, optionally projected to the given columns.
    async fn scan(&self, projection: Option<&[String]>) -> Result<BatchStream>;

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
