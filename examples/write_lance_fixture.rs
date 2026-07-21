//! Write a Lance dataset to ./sample.lance with multiple versions on `main`,
//! a tag, a `dev` branch, and a BTree index — enough to exercise every Lance
//! feature the CLI exposes.
//!
//! Run:
//!     cargo run --example write_lance_fixture
//!
//! Then try the CLI:
//!     # Metadata commands
//!     cargo run -- versions sample.lance                       # every main version
//!     cargo run -- versions --tagged-only sample.lance         # only tagged versions
//!     cargo run -- versions --branch dev sample.lance          # dev branch (every version)
//!     cargo run -- branches sample.lance
//!     cargo run -- tags sample.lance                            # cross-branch tag listing
//!     cargo run -- indices sample.lance
//!     cargo run -- fragments sample.lance                       # fragments with rows/sizes
//!
//!     # Predicate filtering
//!     cargo run -- head --where "id > 5" sample.lance          # first matching rows
//!     cargo run -- rowcount --where "flag = true" sample.lance # count matching rows
//!
//!     # Time-travel reads
//!     cargo run -- rowcount sample.lance                       # latest of main
//!     cargo run -- rowcount --version 1 sample.lance           # just v1
//!     cargo run -- rowcount --tag release-v2 sample.lance      # at the tag
//!     cargo run -- head -n 5 --branch dev sample.lance         # dev rows
//!
//!     # Negative cases
//!     cargo run -- head --branch main --tag release-on-dev sample.lance  # error: tag/branch mismatch

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{
    BinaryArray, BooleanArray, Float64Array, Int32Array, RecordBatch, RecordBatchIterator,
    StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use lance::Dataset;
use lance_index::DatasetIndexExt as _;
use lance_index::IndexType;
use lance_index::scalar::ScalarIndexParams;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("data", DataType::Binary, true),
        Field::new_list(
            "tags",
            Arc::new(Field::new("item", DataType::Utf8, true)),
            true,
        ),
    ]))
}

/// Build a record batch with the rows whose `id` is in `ids`. Other columns
/// are derived deterministically so each row is identifiable in the output.
fn build_batch(ids: &[i32], name_prefix: &str) -> RecordBatch {
    let names: Vec<String> = ids.iter().map(|i| format!("{name_prefix}{i}")).collect();
    let scores: Vec<Option<f64>> = ids
        .iter()
        .map(|i| match i % 5 {
            0 => Some(f64::NAN),
            1 => None,
            2 => Some(f64::INFINITY),
            n => Some(f64::from(n) * 1.5),
        })
        .collect();
    let flags: Vec<Option<bool>> = ids.iter().map(|i| Some(i % 2 == 0)).collect();
    let base_ts = 1_700_000_000_000_000i64;
    let ts: Vec<Option<i64>> = ids
        .iter()
        .map(|i| Some(base_ts + i64::from(*i) * 1_000_000))
        .collect();
    let datas: Vec<Option<Vec<u8>>> = ids
        .iter()
        .map(|i| Some(vec![0xAB, (*i & 0xff) as u8]))
        .collect();

    let mut tags_builder = ListBuilder::new(StringBuilder::new());
    for i in ids {
        tags_builder
            .values()
            .append_value(name_prefix.trim_end_matches('-'));
        tags_builder.values().append_value(format!("id-{i}"));
        tags_builder.append(true);
    }

    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(
                names.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(scores)),
            Arc::new(BooleanArray::from(flags)),
            Arc::new(TimestampMicrosecondArray::from(ts)),
            Arc::new(BinaryArray::from_opt_vec(
                datas.iter().map(|o| o.as_deref()).collect::<Vec<_>>(),
            )),
            Arc::new(tags_builder.finish()),
        ],
    )
    .unwrap()
}

async fn append(ds: &mut Dataset, batch: RecordBatch) -> anyhow::Result<()> {
    let iter = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema());
    ds.append(iter, None).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = PathBuf::from("sample.lance");
    if path.exists() {
        std::fs::remove_dir_all(&path)?;
    }
    let uri = path.to_string_lossy().into_owned();

    // ----- main: v1 -----------------------------------------------------------
    let v1 = build_batch(&[1, 2, 3], "main-");
    let iter = RecordBatchIterator::new(vec![Ok(v1)].into_iter(), schema());
    let mut ds = Dataset::write(iter, uri.as_str(), None).await?;
    println!("main v1: 3 rows (ids 1..=3)");

    // ----- main: v2 (append) --------------------------------------------------
    append(&mut ds, build_batch(&[4, 5, 6], "main-")).await?;
    println!("main v2: appended 3 rows (ids 4..=6)");

    // tag the v2 manifest as `release-v2`
    ds.tags().create("release-v2", 2u64).await?;
    println!("tag 'release-v2' -> main v2");

    // ----- main: v3 (append) --------------------------------------------------
    append(&mut ds, build_batch(&[7, 8, 9], "main-")).await?;
    println!("main v3: appended 3 rows (ids 7..=9)");

    // ----- branch: dev branched from main v2 ---------------------------------
    let mut dev = ds.create_branch("dev", 2u64, None).await?;
    println!("branch 'dev' from main v2");

    // dev v2 (append): rows clearly distinguishable by id range and prefix
    append(&mut dev, build_batch(&[100, 101, 102], "dev-")).await?;
    println!("dev: appended 3 rows (ids 100..=102) on top of inherited 6");

    // tag release-on-dev points at dev's latest version
    dev.tags().create("release-on-dev", ("dev", 2u64)).await?;
    println!("tag 'release-on-dev' -> dev v2");

    // ----- index on main's latest --------------------------------------------
    ds.create_index(
        &["id"],
        IndexType::BTree,
        Some("idx_id".to_string()),
        &ScalarIndexParams::default(),
        false,
    )
    .await?;
    println!("BTree index 'idx_id' on column 'id' (main latest)");

    println!("\nfixture ready at {}", path.display());
    Ok(())
}
