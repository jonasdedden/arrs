use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::RecordBatchReader;
use arrow_schema::{Schema as ArrowSchema, SchemaRef};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use futures::StreamExt;
use lance::Dataset as InnerLance;
use lance::dataset::ProjectionRequest;
use lance_index::DatasetIndexExt as _;

use crate::Result;
use crate::cli::LanceArgs;
use crate::dataset::{
    BatchStream, BranchInfo, Dataset, IndexInfo, LanceCapabilities, ScanOptions, TagInfo,
    VersionInfo,
};
use crate::error::Error;

const MAIN_BRANCH: &str = "main";

#[derive(Debug)]
pub struct LanceDataset {
    inner: InnerLance,
    origin: PathBuf,
    arrow_schema: SchemaRef,
}

impl LanceDataset {
    pub async fn open(path: &Path, lance: Option<&LanceArgs>) -> Result<Self> {
        let uri = path.to_string_lossy().into_owned();
        let inner = InnerLance::open(&uri).await.map_err(|e| Error::LanceOpen {
            path: path.to_path_buf(),
            source: Box::new(e),
        })?;
        let inner = apply_checkout(inner, lance).await?;
        let arrow_schema: SchemaRef = Arc::new(ArrowSchema::from(inner.schema()));
        Ok(Self {
            inner,
            origin: path.to_path_buf(),
            arrow_schema,
        })
    }

    fn projection_request(&self, projection: Option<&[String]>) -> ProjectionRequest {
        match projection {
            Some(cols) => ProjectionRequest::from_columns(cols.iter(), self.inner.schema()),
            None => ProjectionRequest::from_schema(self.inner.schema().clone()),
        }
    }

    /// Parse `predicate` against the dataset schema without running a scan,
    /// mapping any failure to [`Error::InvalidPredicate`]. Used to give a
    /// filtered `count_rows` the same clear error a `scan` would produce.
    fn validate_predicate(&self, predicate: &str) -> Result<()> {
        let mut scanner = self.inner.scan();
        // `filter()` only stores the SQL string today, but map it to
        // `InvalidPredicate` too so a future eager-parsing Lance keeps the
        // context. `get_expr_filter()` forces the parse against the schema.
        scanner
            .filter(predicate)
            .map_err(|e| Error::InvalidPredicate(predicate_error_message(&e)))?;
        scanner
            .get_expr_filter()
            .map_err(|e| Error::InvalidPredicate(predicate_error_message(&e)))?;
        Ok(())
    }
}

/// Turn a Lance/DataFusion predicate-parse error into a concise message.
///
/// These errors bake a source location such as
/// `, /home/user/.cargo/registry/.../scanner.rs:422:33` into their `Display`;
/// strip that noise so the user sees only what is wrong with their SQL.
fn predicate_error_message<E: std::error::Error>(err: &E) -> String {
    strip_source_locations(&err.to_string())
}

fn strip_source_locations(msg: &str) -> String {
    let mut s = msg.trim_end();
    // Peel off one or more trailing ", <path>.rs:<line>:<col>" segments.
    while let Some(idx) = s.rfind(", ") {
        if looks_like_source_location(s[idx + 2..].trim()) {
            s = s[..idx].trim_end();
        } else {
            break;
        }
    }
    s.to_string()
}

fn looks_like_source_location(tail: &str) -> bool {
    // e.g. "/home/user/.cargo/registry/.../scanner.rs:422:33"
    tail.contains(".rs:")
        && tail.rsplit(':').take(2).filter(|s| !s.is_empty()).count() == 2
        && tail
            .rsplit(':')
            .take(2)
            .all(|seg| !seg.is_empty() && seg.chars().all(|c| c.is_ascii_digit()))
}

async fn apply_checkout(mut ds: InnerLance, lance: Option<&LanceArgs>) -> Result<InnerLance> {
    let Some(args) = lance else { return Ok(ds) };

    if let Some(tag) = &args.tag {
        // If the user also supplied --branch, verify the tag actually lives on
        // that branch rather than silently letting the tag's branch win.
        if let Some(requested) = &args.branch {
            let content = ds
                .tags()
                .get(tag)
                .await
                .map_err(|e| Error::Lance(Box::new(e)))?;
            let tag_branch = content.branch.as_deref().unwrap_or(MAIN_BRANCH);
            if tag_branch != requested.as_str() {
                return Err(Error::TagBranchMismatch {
                    tag: tag.clone(),
                    tag_branch: tag_branch.to_string(),
                    requested_branch: requested.clone(),
                });
            }
        }
        // `Ref::Tag` resolves both branch and version itself.
        ds = ds
            .checkout_version(tag.as_str())
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        return Ok(ds);
    }

    if let Some(branch) = &args.branch {
        ds = ds
            .checkout_branch(branch)
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
    }
    if let Some(version) = args.version {
        ds = ds
            .checkout_version(version)
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
    }
    Ok(ds)
}

#[async_trait]
impl Dataset for LanceDataset {
    fn origin(&self) -> &Path {
        &self.origin
    }

    fn arrow_schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn physical_schema_debug(&self, projection: Option<&[String]>) -> Result<String> {
        match projection {
            None => Ok(format!("{:#?}", self.inner.schema())),
            Some(cols) => {
                let projected = self
                    .inner
                    .schema()
                    .project(cols)
                    .map_err(|e| Error::Lance(Box::new(e)))?;
                Ok(format!("{projected:#?}"))
            }
        }
    }

    async fn count_rows(&self, filter: Option<&str>) -> Result<u64> {
        // Validate the predicate up front so a bad `--where` surfaces as an
        // `InvalidPredicate` rather than an opaque count failure. Lance pushes
        // the filter into scalar indices when available, so this stays cheap.
        if let Some(pred) = filter {
            self.validate_predicate(pred)?;
        }
        let n = self
            .inner
            .count_rows(filter.map(str::to_owned))
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        Ok(n as u64)
    }

    async fn scan(&self, options: &ScanOptions<'_>) -> Result<BatchStream> {
        let mut scanner = self.inner.scan();
        if let Some(cols) = options.projection {
            scanner
                .project(cols)
                .map_err(|e| Error::Lance(Box::new(e)))?;
        }
        if let Some(pred) = options.filter {
            // `filter()` only stores the SQL string; force an eager parse via
            // `get_expr_filter()` so an invalid predicate is reported here with
            // context instead of failing deep inside the stream.
            scanner
                .filter(pred)
                .map_err(|e| Error::InvalidPredicate(predicate_error_message(&e)))?;
            scanner
                .get_expr_filter()
                .map_err(|e| Error::InvalidPredicate(predicate_error_message(&e)))?;
        }
        let stream = scanner
            .try_into_stream()
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        let stream = stream.map(|r| r.map_err(|e| Error::Lance(Box::new(e))));
        Ok(Box::pin(stream))
    }

    async fn take(&self, indices: &[u64], projection: Option<&[String]>) -> Result<RecordBatch> {
        let req = self.projection_request(projection);
        self.inner
            .take(indices, req)
            .await
            .map_err(|e| Error::Lance(Box::new(e)))
    }

    fn lance(&self) -> Option<&dyn LanceCapabilities> {
        Some(self)
    }
}

#[async_trait]
impl LanceCapabilities for LanceDataset {
    async fn list_versions(
        &self,
        branch: Option<&str>,
        tagged_only: bool,
    ) -> Result<Vec<VersionInfo>> {
        // Use a branch-scoped clone so the dataset's own active version isn't disturbed.
        let scoped = match branch {
            Some(b) if b != MAIN_BRANCH => self
                .inner
                .clone()
                .checkout_branch(b)
                .await
                .map_err(|e| Error::Lance(Box::new(e)))?,
            _ => self.inner.clone(),
        };
        let target_branch = branch.unwrap_or(MAIN_BRANCH);

        let versions = scoped
            .versions()
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;

        // Tags are dataset-wide; group them by version *for this branch* so we
        // can attach a `tag` (or comma-joined tags) to each VersionInfo row.
        let tags = self
            .inner
            .tags()
            .list()
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        let mut tags_for_version: HashMap<u64, Vec<String>> = HashMap::new();
        for (name, content) in tags {
            let content_branch = content.branch.as_deref().unwrap_or(MAIN_BRANCH);
            if content_branch == target_branch {
                tags_for_version
                    .entry(content.version)
                    .or_default()
                    .push(name);
            }
        }

        let mut out: Vec<VersionInfo> = versions
            .into_iter()
            .map(|v| {
                let mut tag_names = tags_for_version.remove(&v.version).unwrap_or_default();
                tag_names.sort();
                let tag = if tag_names.is_empty() {
                    None
                } else {
                    Some(tag_names.join(","))
                };
                let message = v.metadata.get("message").cloned();
                VersionInfo {
                    version: v.version,
                    timestamp: v.timestamp,
                    tag,
                    message,
                }
            })
            .collect();

        if tagged_only {
            out.retain(|v| v.tag.is_some());
        }
        Ok(out)
    }

    async fn list_branches(&self) -> Result<Vec<BranchInfo>> {
        let map = self
            .inner
            .list_branches()
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;

        // Lance stores `parent_branch: None` to mean "main" (main is the
        // implicit default branch). Normalise the display so users don't see
        // null on every branch that was forked from main.
        let mut out: Vec<BranchInfo> = map
            .into_iter()
            .map(|(name, content)| BranchInfo {
                name,
                parent_branch: Some(
                    content
                        .parent_branch
                        .unwrap_or_else(|| MAIN_BRANCH.to_string()),
                ),
                parent_version: Some(content.parent_version),
                created_at: unix_seconds_to_utc(content.create_at),
            })
            .collect();

        // `list_branches()` skips the implicit default branch — surface it
        // explicitly so the CLI shows a complete picture. Main has no parent;
        // `created_at` is taken from v1's manifest timestamp as a proxy for
        // "when main came into existence".
        if !out.iter().any(|b| b.name == MAIN_BRANCH) {
            let main_inner = self
                .inner
                .clone()
                .checkout_branch(MAIN_BRANCH)
                .await
                .map_err(|e| Error::Lance(Box::new(e)))?;
            let main_created_at = main_inner
                .versions()
                .await
                .map_err(|e| Error::Lance(Box::new(e)))?
                .into_iter()
                .next()
                .map(|v| v.timestamp);
            out.insert(
                0,
                BranchInfo {
                    name: MAIN_BRANCH.to_string(),
                    parent_branch: None,
                    parent_version: None,
                    created_at: main_created_at,
                },
            );
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    async fn list_tags(&self) -> Result<Vec<TagInfo>> {
        let tags = self
            .inner
            .tags()
            .list()
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        let mut out: Vec<TagInfo> = tags
            .into_iter()
            .map(|(name, content)| TagInfo {
                name,
                branch: content.branch.unwrap_or_else(|| MAIN_BRANCH.to_string()),
                version: content.version,
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    async fn list_indices(&self) -> Result<Vec<IndexInfo>> {
        let indices = self
            .inner
            .load_indices()
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        let schema = self.inner.schema();

        Ok(indices
            .iter()
            .map(|m| {
                let columns = m
                    .fields
                    .iter()
                    .map(|id| {
                        schema
                            .field_by_id(*id)
                            .map(|f| f.name.clone())
                            .unwrap_or_else(|| format!("<field_id={id}>"))
                    })
                    .collect();
                IndexInfo {
                    name: m.name.clone(),
                    uuid: m.uuid.to_string(),
                    columns,
                    dataset_version: m.dataset_version,
                    created_at: m.created_at,
                }
            })
            .collect())
    }
}

fn unix_seconds_to_utc(seconds: u64) -> Option<DateTime<Utc>> {
    let secs = i64::try_from(seconds).ok()?;
    Utc.timestamp_opt(secs, 0).single()
}

/// Write a `RecordBatchReader` into a new Lance dataset at `path`.
///
/// Exposed for tests and external fixture builders; not used by the CLI itself.
pub async fn write_dataset<R>(path: &Path, reader: R) -> Result<()>
where
    R: RecordBatchReader + Send + 'static,
{
    let uri = path.to_string_lossy().into_owned();
    InnerLance::write(reader, uri.as_str(), None)
        .await
        .map_err(|e| Error::Lance(Box::new(e)))?;
    Ok(())
}
