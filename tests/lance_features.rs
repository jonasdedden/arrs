//! Integration tests for Lance-specific features:
//! - `versions` / `branches` / `indices` commands.
//! - `--branch` / `--version` / `--tag` checkout flags.

mod common;

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
use arrow_array::{Array, Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt as _;
use lance::Dataset as LanceInner;
use lance::index::vector::VectorIndexParams;
use lance_index::DatasetIndexExt as _;
use lance_index::IndexType;
use lance_index::scalar::ScalarIndexParams;
use lance_linalg::distance::DistanceType;
use tempfile::TempDir;
use tokio::runtime::Runtime;

use arrs::cli::{BinaryFormat, Format, LanceArgs};
use arrs::dataset::{self, VectorSearchParams};
use arrs::error::Error;
use arrs::output::make_writer;
use arrs::output::table::TableStyle;

use common::tempdir;

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
    ]))
}

fn batch(ids: Vec<i32>, vals: Vec<&str>) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(vals)),
        ],
    )
    .unwrap()
}

/// Build a dataset with three versions on `main`, a tag on v2, and a `dev`
/// branch off v2. No index — index creation hits datafusion's shared sort
/// pool and serialises poorly across cargo's parallel test threads, so it's
/// only added by `build_fixture_with_index`.
async fn build_fixture(tmp: &TempDir, name: &str) -> String {
    let path = tmp.path().join(name);
    let uri = path.to_string_lossy().into_owned();

    // v1
    let iter = RecordBatchIterator::new(vec![Ok(batch(vec![1, 2], vec!["a", "b"]))], schema());
    let mut ds = LanceInner::write(iter, uri.as_str(), None).await.unwrap();

    // v2 (append)
    let iter = RecordBatchIterator::new(vec![Ok(batch(vec![3], vec!["c"]))], schema());
    ds.append(iter, None).await.unwrap();

    // tag v2-tag → version 2 of main
    ds.tags().create("v2-tag", 2u64).await.unwrap();

    // v3 on main (append again)
    let iter = RecordBatchIterator::new(vec![Ok(batch(vec![4], vec!["d"]))], schema());
    ds.append(iter, None).await.unwrap();

    // branch `dev` off main version 2
    let _ = ds.create_branch("dev", 2u64, None).await.unwrap();

    uri
}

/// Like `build_fixture` but with an additional BTree index on `id`.
/// Only used by the single index test.
async fn build_fixture_with_index(tmp: &TempDir, name: &str) -> String {
    let uri = build_fixture(tmp, name).await;
    let mut ds = LanceInner::open(uri.as_str()).await.unwrap();
    ds.create_index(
        &["id"],
        IndexType::BTree,
        Some("idx_id".to_string()),
        &ScalarIndexParams::default(),
        false,
    )
    .await
    .unwrap();
    uri
}

// ----------------------------- vector search --------------------------------

/// Rows for the vector-search fixture: distinct 4-d corners of a cube so a
/// nearest-neighbor query has an unambiguous, deterministic ordering.
const VECTOR_DIM: i32 = 4;
const VECTOR_ROWS: [(i32, [f32; 4]); 8] = [
    (0, [0.0, 0.0, 0.0, 0.0]),
    (1, [1.0, 0.0, 0.0, 0.0]),
    (2, [0.0, 1.0, 0.0, 0.0]),
    (3, [1.0, 1.0, 0.0, 0.0]),
    (4, [0.0, 0.0, 1.0, 0.0]),
    (5, [1.0, 0.0, 1.0, 0.0]),
    (6, [0.0, 1.0, 1.0, 0.0]),
    (7, [1.0, 1.0, 1.0, 0.0]),
];

fn vector_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                VECTOR_DIM,
            ),
            false,
        ),
    ]))
}

fn vector_batch() -> RecordBatch {
    let ids = Int32Array::from(VECTOR_ROWS.iter().map(|(id, _)| *id).collect::<Vec<_>>());
    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), VECTOR_DIM);
    for (_, v) in VECTOR_ROWS.iter() {
        builder.values().append_slice(v);
        builder.append(true);
    }
    RecordBatch::try_new(
        vector_schema(),
        vec![Arc::new(ids), Arc::new(builder.finish())],
    )
    .unwrap()
}

/// Build a dataset with an `id` column and a `FixedSizeList<Float32; 4>`
/// `embedding` column. When `with_index`, adds an IVF_FLAT index on the vector
/// column (single partition → exact, deterministic results at fixture scale;
/// IVF_PQ needs far more rows than is practical to train reproducibly here).
async fn build_vector_fixture(tmp: &TempDir, name: &str, with_index: bool) -> String {
    let path = tmp.path().join(name);
    let uri = path.to_string_lossy().into_owned();
    let iter = RecordBatchIterator::new(vec![Ok(vector_batch())], vector_schema());
    let mut ds = LanceInner::write(iter, uri.as_str(), None).await.unwrap();
    if with_index {
        let params = VectorIndexParams::ivf_flat(1, DistanceType::L2);
        ds.create_index(
            &["embedding"],
            IndexType::Vector,
            Some("idx_embedding".to_string()),
            &params,
            false,
        )
        .await
        .unwrap();
    }
    uri
}

/// Pull every row out of a search result as `(id, _distance)` pairs, in the
/// order the stream yields them.
async fn collect_id_distance(result: arrs::dataset::VectorSearchResult) -> Vec<(i32, f32)> {
    use arrow_array::Float32Array;
    let mut stream = result.stream;
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let dist = batch
            .column_by_name("_distance")
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            out.push((ids.value(i), dist.value(i)));
        }
    }
    out
}

#[test]
fn search_with_index_returns_ordered_neighbors() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_vector_fixture(&tmp, "vec", true).await;
        let ds = dataset::open(&path, None).await.unwrap();
        let lance = ds.lance().unwrap();

        let query = vec![0.9_f32, 0.8, 0.1, 0.0];
        let params = VectorSearchParams {
            column: "embedding",
            vector: &query,
            k: 2,
            nprobes: Some(1),
            refine_factor: None,
            projection: None,
        };
        let result = lance.search(&params).await.unwrap();
        assert!(result.used_index, "IVF_FLAT index should be used");
        assert!(result.schema.field_with_name("_distance").is_ok());

        let rows = collect_id_distance(result).await;
        assert_eq!(rows.len(), 2);
        // Query is closest to [1,1,0,0] (id 3), then [1,0,0,0] (id 1).
        assert_eq!(rows[0].0, 3);
        assert_eq!(rows[1].0, 1);
        assert!(rows[0].1 <= rows[1].1, "distances must be ascending");
    });
}

#[test]
fn search_without_index_falls_back_to_flat_knn() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_vector_fixture(&tmp, "vec", false).await;
        let ds = dataset::open(&path, None).await.unwrap();
        let lance = ds.lance().unwrap();

        let query = vec![0.9_f32, 0.8, 0.1, 0.0];
        let params = VectorSearchParams {
            column: "embedding",
            vector: &query,
            k: 2,
            nprobes: None,
            refine_factor: None,
            projection: None,
        };
        let result = lance.search(&params).await.unwrap();
        assert!(!result.used_index, "no index → flat KNN");

        let rows = collect_id_distance(result).await;
        // Same deterministic ordering as the indexed path.
        assert_eq!(
            rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![3, 1]
        );
    });
}

#[test]
fn search_dimension_mismatch_is_precise() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_vector_fixture(&tmp, "vec", false).await;
        let ds = dataset::open(&path, None).await.unwrap();
        let lance = ds.lance().unwrap();

        let query = vec![0.1_f32, 0.2]; // 2 dims vs column's 4
        let params = VectorSearchParams {
            column: "embedding",
            vector: &query,
            k: 1,
            nprobes: None,
            refine_factor: None,
            projection: None,
        };
        let err = match lance.search(&params).await {
            Ok(_) => panic!("expected VectorDimMismatch error"),
            Err(e) => e,
        };
        match err {
            Error::VectorDimMismatch {
                query,
                column,
                column_dims,
            } => {
                assert_eq!(query, 2);
                assert_eq!(column, "embedding");
                assert_eq!(column_dims, 4);
            }
            other => panic!("expected VectorDimMismatch, got {other:?}"),
        }
    });
}

#[test]
fn search_on_non_vector_column_errors() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_vector_fixture(&tmp, "vec", false).await;
        let ds = dataset::open(&path, None).await.unwrap();
        let lance = ds.lance().unwrap();

        let query = vec![0.1_f32, 0.2, 0.3, 0.4];
        let params = VectorSearchParams {
            column: "id", // Int32, not a vector column
            vector: &query,
            k: 1,
            nprobes: None,
            refine_factor: None,
            projection: None,
        };
        let err = match lance.search(&params).await {
            Ok(_) => panic!("expected NotVectorColumn error"),
            Err(e) => e,
        };
        assert!(matches!(err, Error::NotVectorColumn { .. }), "got {err:?}");
    });
}

#[test]
fn search_projection_composes_with_distance() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_vector_fixture(&tmp, "vec", true).await;
        let ds = dataset::open(&path, None).await.unwrap();
        let lance = ds.lance().unwrap();

        let query = vec![0.9_f32, 0.8, 0.1, 0.0];
        let projection = vec!["id".to_string()];
        let params = VectorSearchParams {
            column: "embedding",
            vector: &query,
            k: 2,
            nprobes: Some(1),
            refine_factor: None,
            projection: Some(&projection),
        };
        let result = lance.search(&params).await.unwrap();
        // Projected to `id` only, but `_distance` is always appended; the
        // unprojected `embedding` column must be absent.
        assert!(result.schema.field_with_name("id").is_ok());
        assert!(result.schema.field_with_name("_distance").is_ok());
        assert!(result.schema.field_with_name("embedding").is_err());
    });
}

#[test]
fn search_output_works_in_all_formats() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_vector_fixture(&tmp, "vec", true).await;
        let ds = dataset::open(&path, None).await.unwrap();
        let lance = ds.lance().unwrap();

        let query = vec![0.9_f32, 0.8, 0.1, 0.0];
        for format in [Format::Jsonl, Format::Csv, Format::Table] {
            let projection = vec!["id".to_string()];
            let params = VectorSearchParams {
                column: "embedding",
                vector: &query,
                k: 2,
                nprobes: Some(1),
                refine_factor: None,
                projection: Some(&projection),
            };
            let result = lance.search(&params).await.unwrap();
            let schema = result.schema.clone();
            let mut stream = result.stream;

            let mut out: Vec<u8> = Vec::new();
            {
                let mut w = make_writer(
                    format,
                    BinaryFormat::None,
                    TableStyle::Plain,
                    Cursor::new(&mut out),
                );
                w.start(&schema).unwrap();
                while let Some(batch) = stream.next().await {
                    w.write_batch(&batch.unwrap()).unwrap();
                }
                w.finish().unwrap();
            }
            let text = String::from_utf8(out).unwrap();
            assert!(
                text.contains("_distance"),
                "format {format:?} output missing _distance column: {text}"
            );
        }
    });
}

// ----------------------------- adapter-level --------------------------------

#[test]
fn list_versions_tagged_only_returns_only_tagged() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_fixture(&tmp, "ds").await;
        let ds = dataset::open(&path, None).await.unwrap();
        let lance = ds.lance().unwrap();

        let versions = lance.list_versions(None, true).await.unwrap();
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].version, 2);
        assert_eq!(versions[0].tag.as_deref(), Some("v2-tag"));
    });
}

#[test]
fn list_versions_default_lists_all_main_versions() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_fixture(&tmp, "ds").await;
        let ds = dataset::open(&path, None).await.unwrap();
        let lance = ds.lance().unwrap();

        // tagged_only = false (the CLI default) → every version is listed.
        let versions = lance.list_versions(None, false).await.unwrap();
        assert!(versions.iter().any(|v| v.version == 1 && v.tag.is_none()));
        let tagged = versions.iter().find(|v| v.version == 2).unwrap();
        assert_eq!(tagged.tag.as_deref(), Some("v2-tag"));
    });
}

#[test]
fn list_branches_includes_main_and_dev() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_fixture(&tmp, "ds").await;
        let ds = dataset::open(&path, None).await.unwrap();
        let lance = ds.lance().unwrap();

        let branches = lance.list_branches().await.unwrap();
        let names: Vec<&str> = branches.iter().map(|b| b.name.as_str()).collect();
        assert!(names.contains(&"main"));
        assert!(names.contains(&"dev"));
    });
}

#[test]
fn list_tags_returns_cross_branch_view() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_fixture(&tmp, "ds").await;

        // The fixture creates a `dev` branch from main v2 — give it its own
        // version + tag so we can assert tags are surfaced cross-branch.
        let dev = LanceInner::open(path.as_str())
            .await
            .unwrap()
            .checkout_branch("dev")
            .await
            .unwrap();
        dev.tags()
            .create("release-on-dev", ("dev", 2u64))
            .await
            .unwrap();

        let ds = dataset::open(&path, None).await.unwrap();
        let lance = ds.lance().unwrap();
        let tags = lance.list_tags().await.unwrap();

        let by_name: std::collections::HashMap<&str, &arrs::dataset::TagInfo> =
            tags.iter().map(|t| (t.name.as_str(), t)).collect();
        let v2 = by_name.get("v2-tag").expect("v2-tag listed");
        assert_eq!(v2.branch, "main");
        assert_eq!(v2.version, 2);
        let on_dev = by_name
            .get("release-on-dev")
            .expect("release-on-dev listed");
        assert_eq!(on_dev.branch, "dev");
        assert_eq!(on_dev.version, 2);
    });
}

#[test]
fn list_indices_finds_btree_index() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_fixture_with_index(&tmp, "ds").await;
        let ds = dataset::open(&path, None).await.unwrap();
        let lance = ds.lance().unwrap();

        let indices = lance.list_indices().await.unwrap();
        assert_eq!(indices.len(), 1);
        assert_eq!(indices[0].name, "idx_id");
        assert_eq!(indices[0].columns, vec!["id".to_string()]);
    });
}

/// Build a dataset with three fragments (one per append) and, when `delete` is
/// set, tombstone a single row so exactly one fragment carries a deletion file.
async fn build_fragmented(tmp: &TempDir, name: &str, delete: bool) -> String {
    let path = tmp.path().join(name);
    let uri = path.to_string_lossy().into_owned();

    let iter = RecordBatchIterator::new(vec![Ok(batch(vec![1, 2], vec!["a", "b"]))], schema());
    let mut ds = LanceInner::write(iter, uri.as_str(), None).await.unwrap();

    let iter = RecordBatchIterator::new(vec![Ok(batch(vec![3, 4], vec!["c", "d"]))], schema());
    ds.append(iter, None).await.unwrap();

    let iter = RecordBatchIterator::new(vec![Ok(batch(vec![5, 6], vec!["e", "f"]))], schema());
    ds.append(iter, None).await.unwrap();

    if delete {
        // `id = 3` lives in the second fragment.
        ds.delete("id = 3").await.unwrap();
    }
    uri
}

#[test]
fn list_fragments_reports_rows_files_and_sizes() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_fragmented(&tmp, "ds", false).await;
        let ds = dataset::open(&path, None).await.unwrap();
        let lance = ds.lance().unwrap();

        let fragments = lance.list_fragments(true).await.unwrap();
        // Three appends → three fragments, each with two physical rows.
        assert_eq!(fragments.len(), 3);
        let total_physical: u64 = fragments.iter().map(|f| f.physical_rows).sum();
        assert_eq!(total_physical, 6);
        for f in &fragments {
            assert_eq!(f.deleted_rows, 0);
            assert!(f.num_files >= 1);
            assert_eq!(f.num_files as usize, f.files.len());
            // Local dataset: on-disk size is known and non-zero.
            assert!(f.size.is_some_and(|s| s > 0));
        }
        // Fragment ids are unique.
        let mut ids: Vec<u64> = fragments.iter().map(|f| f.id).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 3);
    });
}

#[test]
fn list_fragments_counts_deleted_rows() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_fragmented(&tmp, "ds", true).await;
        let ds = dataset::open(&path, None).await.unwrap();
        let lance = ds.lance().unwrap();

        let fragments = lance.list_fragments(false).await.unwrap();
        let total_deleted: u64 = fragments.iter().map(|f| f.deleted_rows).sum();
        assert_eq!(total_deleted, 1);
        // Exactly one fragment carries the deletion.
        assert_eq!(fragments.iter().filter(|f| f.deleted_rows > 0).count(), 1);
        // Physical rows are unaffected by deletions (tombstone, not rewrite).
        let total_physical: u64 = fragments.iter().map(|f| f.physical_rows).sum();
        assert_eq!(total_physical, 6);
    });
}

#[test]
fn list_fragments_no_size_leaves_size_unset() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_fragmented(&tmp, "ds", false).await;
        let ds = dataset::open(&path, None).await.unwrap();
        let lance = ds.lance().unwrap();

        let fragments = lance.list_fragments(false).await.unwrap();
        assert!(!fragments.is_empty());
        assert!(fragments.iter().all(|f| f.size.is_none()));
    });
}

#[test]
fn list_fragments_respects_version_checkout() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_fragmented(&tmp, "ds", false).await;

        // Version 1 predates the two appends → a single fragment.
        let lance_args = LanceArgs {
            version: Some(1),
            ..LanceArgs::default()
        };
        let ds = dataset::open(&path, Some(&lance_args)).await.unwrap();
        let lance = ds.lance().unwrap();
        let fragments = lance.list_fragments(false).await.unwrap();
        assert_eq!(fragments.len(), 1);
        assert_eq!(fragments[0].physical_rows, 2);
    });
}

// ----------------------------- checkout flags -------------------------------

#[test]
fn checkout_by_version_yields_old_rowcount() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_fixture(&tmp, "ds").await;

        let lance = LanceArgs {
            version: Some(1),
            ..LanceArgs::default()
        };
        let ds = dataset::open(&path, Some(&lance)).await.unwrap();
        assert_eq!(ds.count_rows(None).await.unwrap(), 2);
    });
}

#[test]
fn checkout_by_tag_yields_tagged_rowcount() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_fixture(&tmp, "ds").await;

        let lance = LanceArgs {
            tag: Some("v2-tag".to_string()),
            ..LanceArgs::default()
        };
        let ds = dataset::open(&path, Some(&lance)).await.unwrap();
        // v2 = v1 (2 rows) + v2 append (1 row) = 3 rows
        assert_eq!(ds.count_rows(None).await.unwrap(), 3);
    });
}

#[test]
fn checkout_by_branch_uses_branch_latest() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_fixture(&tmp, "ds").await;

        let lance = LanceArgs {
            branch: Some("dev".to_string()),
            ..LanceArgs::default()
        };
        let ds = dataset::open(&path, Some(&lance)).await.unwrap();
        // dev was branched from v2 of main and never appended to → 3 rows.
        assert_eq!(ds.count_rows(None).await.unwrap(), 3);
    });
}

#[test]
fn checkout_tag_with_mismatched_branch_errors() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_fixture(&tmp, "ds").await;

        // v2-tag was created on `main`; asking for it via --branch dev must error.
        let lance = LanceArgs {
            tag: Some("v2-tag".to_string()),
            branch: Some("dev".to_string()),
            ..LanceArgs::default()
        };
        let err = dataset::open(&path, Some(&lance)).await.unwrap_err();
        assert!(matches!(err, arrs::error::Error::TagBranchMismatch { .. }));
    });
}

#[test]
fn checkout_tag_with_matching_branch_ok() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_fixture(&tmp, "ds").await;

        let lance = LanceArgs {
            tag: Some("v2-tag".to_string()),
            branch: Some("main".to_string()),
            ..LanceArgs::default()
        };
        let ds = dataset::open(&path, Some(&lance)).await.unwrap();
        assert_eq!(ds.count_rows(None).await.unwrap(), 3);
    });
}

#[test]
fn checkout_unknown_branch_errors() {
    runtime().block_on(async {
        let tmp = tempdir();
        let path = build_fixture(&tmp, "ds").await;

        let lance = LanceArgs {
            branch: Some("nope".to_string()),
            ..LanceArgs::default()
        };
        let res = dataset::open(&path, Some(&lance)).await;
        assert!(res.is_err());
    });
}

#[test]
fn open_non_lance_path_errors_with_unknown_format() {
    // A directory that lacks `_versions/` is not recognised as a Lance dataset.
    runtime().block_on(async {
        let tmp = tempdir();
        let path = tmp.path().join("not-a-dataset");
        std::fs::create_dir_all(&path).unwrap();
        let err = dataset::open(path.to_str().unwrap(), None)
            .await
            .unwrap_err();
        assert!(matches!(err, arrs::error::Error::UnknownFormat { .. }));
    });
}

#[test]
fn open_via_file_uri_scheme_matches_local_path() {
    // A `file://` URI takes the scheme-qualified dispatch path (no local
    // `_versions/` probe) yet must resolve to the same local dataset.
    runtime().block_on(async {
        let tmp = tempdir();
        let uri = build_fixture(&tmp, "ds").await; // absolute local path
        let file_uri = format!("file://{uri}");

        let ds = dataset::open(&file_uri, None).await.unwrap();
        assert_eq!(ds.origin(), file_uri);
        assert_eq!(ds.count_rows(None).await.unwrap(), 4);
    });
}

#[test]
fn open_nonexistent_scheme_uri_errors_with_uri_in_message() {
    // Scheme-qualified inputs skip the local heuristic and defer to the adapter,
    // whose error must name the offending URI and carry a readable cause rather
    // than a raw debug dump.
    runtime().block_on(async {
        let tmp = tempdir();
        let missing = format!("file://{}/does-not-exist.lance", tmp.path().display());

        let err = dataset::open(&missing, None).await.unwrap_err();
        match &err {
            arrs::error::Error::LanceOpen { path, source } => {
                assert_eq!(path, &missing);
                // The wrapped cause is surfaced via Display, not `{:?}`.
                assert!(!format!("{source}").is_empty());
            }
            other => panic!("expected LanceOpen, got {other:?}"),
        }
        // Top-level Display carries the URI for the user.
        assert!(format!("{err}").contains(&missing));
    });
}
