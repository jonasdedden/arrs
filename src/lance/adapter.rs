use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::Float32Array;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchReader;
use arrow_schema::{DataType, Schema as ArrowSchema, SchemaRef};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use futures::{StreamExt, TryStreamExt};
use lance::Dataset as InnerLance;
use lance::dataset::ProjectionRequest;
use lance_index::DatasetIndexExt as _;
use lance_index::vector::DIST_COL;

use crate::Result;
use crate::cli::LanceArgs;
use crate::dataset::{
    BatchStream, BranchInfo, Dataset, FragmentInfo, IndexInfo, LanceCapabilities, ScanOptions,
    TagInfo, VectorSearchParams, VectorSearchResult, VersionInfo,
};
use crate::error::Error;

const MAIN_BRANCH: &str = "main";

/// Max in-flight object-store `size` lookups when computing fragment sizes.
/// Fragments are typically backed by a single data file, so this bounds the
/// number of concurrent `head`-style requests to a remote store.
const SIZE_CONCURRENCY: usize = 16;

#[derive(Debug)]
pub struct LanceDataset {
    inner: InnerLance,
    origin: String,
    arrow_schema: SchemaRef,
}

impl LanceDataset {
    /// Open the Lance dataset at `input`. `input` is passed to Lance verbatim,
    /// so it may be a local path (with or without a `file://` prefix) or an
    /// object-store URI (`s3://…`, `gs://…`, `az://…`); credentials are taken
    /// from the ambient environment. On failure the error carries `input` and
    /// the underlying object-store cause.
    pub async fn open(input: &str, lance: Option<&LanceArgs>) -> Result<Self> {
        let inner = InnerLance::open(input)
            .await
            .map_err(|e| Error::LanceOpen {
                path: input.to_string(),
                source: Box::new(e),
            })?;
        let inner = apply_checkout(inner, lance).await?;
        let arrow_schema: SchemaRef = Arc::new(ArrowSchema::from(inner.schema()));
        Ok(Self {
            inner,
            origin: input.to_string(),
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
    fn origin(&self) -> &str {
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

    async fn list_fragments(&self, with_size: bool) -> Result<Vec<FragmentInfo>> {
        let fragments = self.inner.get_fragments();

        // Everything except size comes straight from the manifest, so no I/O
        // happens on the common path. We only fall back to a per-fragment await
        // for legacy fragments whose manifest omits the row/deletion counts.
        // Those fallback awaits (`physical_rows()` / `count_deletions()`) are
        // untested by construction: lance 4.0 always populates these fields for
        // freshly written datasets, so the test suite never reaches them.
        let mut out: Vec<FragmentInfo> = Vec::with_capacity(fragments.len());
        for frag in &fragments {
            let meta = frag.metadata();
            let physical_rows = match meta.physical_rows {
                Some(n) => n as u64,
                None => frag
                    .physical_rows()
                    .await
                    .map_err(|e| Error::Lance(Box::new(e)))? as u64,
            };
            let deleted_rows = match &meta.deletion_file {
                None => 0,
                Some(df) => match df.num_deleted_rows {
                    Some(n) => n as u64,
                    None => frag
                        .count_deletions()
                        .await
                        .map_err(|e| Error::Lance(Box::new(e)))? as u64,
                },
            };
            let files: Vec<String> = meta.files.iter().map(|f| f.path.clone()).collect();
            out.push(FragmentInfo {
                id: meta.id,
                physical_rows,
                deleted_rows,
                num_files: files.len() as u64,
                files,
                size: None,
            });
        }

        if with_size {
            let data_dir = self.inner.data_dir();
            let object_store = self.inner.object_store();

            // Collect owned `(relative path, cached size)` specs up front so the
            // concurrent closures below borrow nothing from `fragments` — that
            // keeps the async blocks free of the higher-ranked lifetimes that
            // `buffer_unordered` otherwise can't satisfy.
            let specs: Vec<(usize, Vec<(String, Option<u64>)>)> = fragments
                .iter()
                .enumerate()
                .map(|(i, frag)| {
                    let files = frag
                        .metadata()
                        .files
                        .iter()
                        .map(|f| (f.path.clone(), f.file_size_bytes.get().map(|n| n.get())))
                        .collect();
                    (i, files)
                })
                .collect();

            // Prefer the size cached in the manifest; only hit the object store
            // for files that don't record it. Requests run concurrently, keyed
            // by index so results can be reassembled after `buffer_unordered`
            // returns them out of order. The object-store `size()` fallback is
            // untested by construction: lance 4.0 records `file_size_bytes` in
            // the manifest for fresh datasets, so tests take the cached branch.
            let sized: Vec<(usize, u64)> = futures::stream::iter(specs)
                .map(|(i, files)| {
                    let data_dir = &data_dir;
                    async move {
                        let mut total = 0u64;
                        for (path, cached) in files {
                            match cached {
                                Some(sz) => total += sz,
                                None => {
                                    let object_path = data_dir.child(path.as_str());
                                    total += object_store
                                        .size(&object_path)
                                        .await
                                        .map_err(|e| Error::Lance(Box::new(e)))?;
                                }
                            }
                        }
                        Ok::<(usize, u64), Error>((i, total))
                    }
                })
                .buffer_unordered(SIZE_CONCURRENCY)
                .try_collect()
                .await?;
            for (i, total) in sized {
                out[i].size = Some(total);
            }
        }

        Ok(out)
    }

    async fn search(&self, params: &VectorSearchParams<'_>) -> Result<VectorSearchResult> {
        // Validate the target column ourselves so the error messages are precise
        // (`query has 512 dims, column embedding has 768`) rather than relying on
        // Lance's internal wording.
        let dim = vector_column_dim(&self.arrow_schema, params.column)?;
        if params.vector.len() != dim {
            return Err(Error::VectorDimMismatch {
                query: params.vector.len(),
                column: params.column.to_string(),
                column_dims: dim,
            });
        }

        // Lance coerces from a Float32Array to the column's f16/f32/f64 element
        // type internally, so f32 is the interchange type for the query vector.
        let query = Float32Array::from(params.vector.to_vec());

        let mut scanner = self.inner.scan();
        scanner
            .nearest(params.column, &query, params.k)
            .map_err(|e| Error::Lance(Box::new(e)))?;
        if let Some(n) = params.nprobes {
            scanner.nprobes(n);
        }
        if let Some(factor) = params.refine_factor {
            scanner.refine(factor);
        }

        // Build the projection explicitly and opt out of Lance's deprecated
        // scoring autoprojection (which silently appends `_distance` today but
        // is slated to stop). We always force-include `_distance` ourselves —
        // it is a recognised system column, resolved against the search output
        // schema — so it is present regardless of the user's `--columns`.
        scanner.disable_scoring_autoprojection();
        let mut projection: Vec<String> = match params.projection {
            Some(cols) => cols.to_vec(),
            None => self
                .arrow_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
        };
        if !projection.iter().any(|c| c == DIST_COL) {
            projection.push(DIST_COL.to_string());
        }
        scanner
            .project(&projection)
            .map_err(|e| Error::Lance(Box::new(e)))?;

        let used_index = self.column_has_ann_index(params.column).await?;

        // `schema()` reflects the projection plus the trailing `_distance` column.
        let schema = scanner
            .schema()
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        let stream = scanner
            .try_into_stream()
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        let stream = stream.map(|r| r.map_err(|e| Error::Lance(Box::new(e))));

        Ok(VectorSearchResult {
            schema,
            stream: Box::pin(stream),
            used_index,
        })
    }
}

impl LanceDataset {
    /// True when at least one index covers `column`. Vector columns only ever
    /// carry ANN indices, so index coverage is a reliable "is this indexed"
    /// signal for the flat-KNN stderr note.
    async fn column_has_ann_index(&self, column: &str) -> Result<bool> {
        let indices = self
            .inner
            .load_indices()
            .await
            .map_err(|e| Error::Lance(Box::new(e)))?;
        let schema = self.inner.schema();
        Ok(indices.iter().any(|m| {
            m.fields
                .iter()
                .filter_map(|id| schema.field_by_id(*id))
                .any(|f| f.name == column)
        }))
    }
}

/// Resolve `column` to the width of its `FixedSizeList`-of-float type, erroring
/// precisely when the column is missing or is not a float vector column.
fn vector_column_dim(schema: &ArrowSchema, column: &str) -> Result<usize> {
    let field = schema.field_with_name(column).map_err(|_| {
        let available = schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>()
            .join(", ");
        Error::UnknownColumn {
            name: column.to_string(),
            available,
        }
    })?;
    match field.data_type() {
        DataType::FixedSizeList(inner, size)
            if matches!(
                inner.data_type(),
                DataType::Float16 | DataType::Float32 | DataType::Float64
            ) =>
        {
            Ok(*size as usize)
        }
        other => Err(Error::NotVectorColumn {
            column: column.to_string(),
            data_type: other.to_string(),
        }),
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
