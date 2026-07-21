//! End-to-end integration tests: drive the library API with realistic
//! Lance datasets and assert on the captured output.

mod common;

use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_schema::SchemaRef;
use arrs::cli::{BinaryFormat, Cli, Command, FilterArg, Format, LanceArgs};
use arrs::commands::dispatch;
use arrs::dataset;
use arrs::dataset::ScanOptions;
use arrs::indices;
use arrs::output::make_writer;
use arrs::output::table::TableStyle;
use arrs::projection;
use futures::StreamExt;
use tokio::runtime::Runtime;

use common::{tempdir, write_full, write_simple, write_with_binary};

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn project(schema: &SchemaRef, projection: Option<&[String]>) -> SchemaRef {
    match projection {
        None => schema.clone(),
        Some(cols) => {
            let fields: Vec<_> = cols
                .iter()
                .map(|n| schema.field_with_name(n).unwrap().clone())
                .collect();
            Arc::new(arrow_schema::Schema::new(fields))
        }
    }
}

async fn collect_cat(
    inputs: Vec<PathBuf>,
    format: Format,
    binary_format: BinaryFormat,
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
) -> arrs::Result<String> {
    let mut out: Vec<u8> = Vec::new();
    {
        let first = dataset::open(&inputs[0], None).await?;
        let s = first.arrow_schema();
        let proj = projection::resolve(&s, columns, exclude)?;
        let projected = project(&s, proj.as_deref());
        let mut w = make_writer(
            format,
            binary_format,
            TableStyle::Plain,
            Cursor::new(&mut out),
        );
        w.start(&projected)?;
        for p in &inputs {
            let ds = dataset::open(p, None).await?;
            let options = ScanOptions {
                projection: proj.as_deref(),
                filter: None,
            };
            let mut stream = ds.scan(&options).await?;
            while let Some(b) = stream.next().await {
                w.write_batch(&b?)?;
            }
        }
        w.finish()?;
    }
    Ok(String::from_utf8(out).unwrap())
}

async fn collect_head(
    input: &Path,
    limit: u64,
    format: Format,
    binary_format: BinaryFormat,
) -> arrs::Result<String> {
    let ds = dataset::open(input, None).await?;
    let s = ds.arrow_schema();
    let projected = project(&s, None);
    let mut out: Vec<u8> = Vec::new();
    {
        let mut w = make_writer(
            format,
            binary_format,
            TableStyle::Plain,
            Cursor::new(&mut out),
        );
        w.start(&projected)?;
        let mut remaining = limit;
        if remaining > 0 {
            let mut stream = ds.scan(&ScanOptions::default()).await?;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                let rows = batch.num_rows() as u64;
                if rows <= remaining {
                    w.write_batch(&batch)?;
                    remaining -= rows;
                } else {
                    w.write_batch(&batch.slice(0, remaining as usize))?;
                    remaining = 0;
                }
                if remaining == 0 {
                    break;
                }
            }
        }
        w.finish()?;
    }
    Ok(String::from_utf8(out).unwrap())
}

async fn collect_tail(
    input: &Path,
    limit: u64,
    format: Format,
    binary_format: BinaryFormat,
) -> arrs::Result<String> {
    let ds = dataset::open(input, None).await?;
    let s = ds.arrow_schema();
    let projected = project(&s, None);
    let rowcount = ds.count_rows(None).await?;
    let take_n = limit.min(rowcount);
    let mut out: Vec<u8> = Vec::new();
    {
        let mut w = make_writer(
            format,
            binary_format,
            TableStyle::Plain,
            Cursor::new(&mut out),
        );
        w.start(&projected)?;
        if take_n > 0 {
            let start = rowcount - take_n;
            let idx: Vec<u64> = (start..rowcount).collect();
            let batch = ds.take(&idx, None).await?;
            w.write_batch(&batch)?;
        }
        w.finish()?;
    }
    Ok(String::from_utf8(out).unwrap())
}

async fn collect_take(
    input: &Path,
    idx: &str,
    format: Format,
    binary_format: BinaryFormat,
) -> arrs::Result<String> {
    let ds = dataset::open(input, None).await?;
    let s = ds.arrow_schema();
    let projected = project(&s, None);
    let rowcount = ds.count_rows(None).await?;
    let indices = indices::resolve(idx, rowcount)?;
    let mut out: Vec<u8> = Vec::new();
    {
        let mut w = make_writer(
            format,
            binary_format,
            TableStyle::Plain,
            Cursor::new(&mut out),
        );
        w.start(&projected)?;
        if !indices.is_empty() {
            let batch = ds.take(&indices, None).await?;
            w.write_batch(&batch)?;
        }
        w.finish()?;
    }
    Ok(String::from_utf8(out).unwrap())
}

async fn collect_sample(
    input: &Path,
    limit: u64,
    seed: u64,
    format: Format,
    binary_format: BinaryFormat,
) -> arrs::Result<String> {
    use rand::SeedableRng;
    use rand::prelude::*;
    use rand_chacha::ChaCha20Rng;

    let ds = dataset::open(input, None).await?;
    let s = ds.arrow_schema();
    let projected = project(&s, None);
    let rowcount = ds.count_rows(None).await?;
    let mut pool: Vec<u64> = (0..rowcount).collect();
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    pool.shuffle(&mut rng);
    pool.truncate(limit as usize);
    let mut out: Vec<u8> = Vec::new();
    {
        let mut w = make_writer(
            format,
            binary_format,
            TableStyle::Plain,
            Cursor::new(&mut out),
        );
        w.start(&projected)?;
        if !pool.is_empty() {
            let batch = ds.take(&pool, None).await?;
            w.write_batch(&batch)?;
        }
        w.finish()?;
    }
    Ok(String::from_utf8(out).unwrap())
}

/// Scan `input` with a `--where` filter and return every matching row as
/// JSONL. Mirrors what `cat --where` / `head --where` (large limit) produce.
async fn collect_scan_where(input: &Path, filter: &str) -> arrs::Result<String> {
    let ds = dataset::open(input, None).await?;
    let s = ds.arrow_schema();
    let projected = project(&s, None);
    let mut out: Vec<u8> = Vec::new();
    {
        let mut w = make_writer(
            Format::Jsonl,
            BinaryFormat::None,
            TableStyle::Plain,
            Cursor::new(&mut out),
        );
        w.start(&projected)?;
        let options = ScanOptions {
            projection: None,
            filter: Some(filter),
        };
        let mut stream = ds.scan(&options).await?;
        while let Some(batch) = stream.next().await {
            w.write_batch(&batch?)?;
        }
        w.finish()?;
    }
    Ok(String::from_utf8(out).unwrap())
}

/// Filtered `tail`: the last `limit` *matching* rows. Mirrors the streaming
/// ring-buffer path in `commands::tail`.
async fn collect_tail_where(input: &Path, limit: u64, filter: &str) -> arrs::Result<String> {
    use std::collections::VecDeque;

    let ds = dataset::open(input, None).await?;
    let s = ds.arrow_schema();
    let projected = project(&s, None);
    let mut out: Vec<u8> = Vec::new();
    {
        let mut w = make_writer(
            Format::Jsonl,
            BinaryFormat::None,
            TableStyle::Plain,
            Cursor::new(&mut out),
        );
        w.start(&projected)?;
        if limit > 0 {
            let options = ScanOptions {
                projection: None,
                filter: Some(filter),
            };
            let mut stream = ds.scan(&options).await?;
            let mut buffered: VecDeque<arrow_array::RecordBatch> = VecDeque::new();
            let mut buffered_rows: u64 = 0;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                if batch.num_rows() == 0 {
                    continue;
                }
                buffered_rows += batch.num_rows() as u64;
                buffered.push_back(batch);
                while let Some(front) = buffered.front() {
                    let front_rows = front.num_rows() as u64;
                    if buffered_rows - front_rows >= limit {
                        buffered_rows -= front_rows;
                        buffered.pop_front();
                    } else {
                        break;
                    }
                }
            }
            let mut skip = buffered_rows - limit.min(buffered_rows);
            for batch in buffered {
                let rows = batch.num_rows() as u64;
                if skip >= rows {
                    skip -= rows;
                    continue;
                }
                w.write_batch(&batch.slice(skip as usize, (rows - skip) as usize))?;
                skip = 0;
            }
        }
        w.finish()?;
    }
    Ok(String::from_utf8(out).unwrap())
}

/// Filtered `sample`: reservoir-sample `limit` matching rows. Mirrors the
/// streaming path in `commands::sample`.
async fn collect_sample_where(
    input: &Path,
    limit: u64,
    seed: u64,
    filter: &str,
) -> arrs::Result<String> {
    use rand::SeedableRng;
    use rand::prelude::*;
    use rand_chacha::ChaCha20Rng;

    let ds = dataset::open(input, None).await?;
    let s = ds.arrow_schema();
    let projected = project(&s, None);
    let mut out: Vec<u8> = Vec::new();
    {
        let mut w = make_writer(
            Format::Jsonl,
            BinaryFormat::None,
            TableStyle::Plain,
            Cursor::new(&mut out),
        );
        w.start(&projected)?;
        if limit > 0 {
            let options = ScanOptions {
                projection: None,
                filter: Some(filter),
            };
            let mut stream = ds.scan(&options).await?;
            let cap = limit as usize;
            let mut rng = ChaCha20Rng::seed_from_u64(seed);
            let mut reservoir: Vec<arrow_array::RecordBatch> = Vec::with_capacity(cap);
            let mut seen: u64 = 0;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                for r in 0..batch.num_rows() {
                    let row = batch.slice(r, 1);
                    if reservoir.len() < cap {
                        reservoir.push(row);
                    } else {
                        let j = rng.random_range(0..=seen);
                        if (j as usize) < cap {
                            reservoir[j as usize] = row;
                        }
                    }
                    seen += 1;
                }
            }
            if limit > seen {
                return Err(arrs::error::Error::SampleTooLarge {
                    requested: limit,
                    rowcount: seen,
                });
            }
            if !reservoir.is_empty() {
                let schema = reservoir[0].schema();
                let combined = arrow::compute::concat_batches(&schema, &reservoir)?;
                w.write_batch(&combined)?;
            }
        }
        w.finish()?;
    }
    Ok(String::from_utf8(out).unwrap())
}

// -------------------- tests --------------------

#[test]
fn rowcount_is_5_for_simple_fixture() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "simple").await;
        let ds = dataset::open(&p, None).await.unwrap();
        assert_eq!(ds.count_rows(None).await.unwrap(), 5);
    });
}

#[test]
fn cat_jsonl_emits_nan_and_infinity_as_strings() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_cat(vec![p], Format::Jsonl, BinaryFormat::None, None, None)
            .await
            .unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 5);
        let v0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(v0["id"], 1);
        assert_eq!(v0["name"], "alice");
        assert_eq!(v0["score"], 10.5);
        let v2: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(v2["score"], "NaN");
        let v3: serde_json::Value = serde_json::from_str(lines[3]).unwrap();
        assert_eq!(v3["score"], "Infinity");
        let v1: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(v1["score"], serde_json::Value::Null);
    });
}

#[test]
fn cat_csv_header_and_null_cells() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_cat(vec![p], Format::Csv, BinaryFormat::None, None, None)
            .await
            .unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "id,name,score");
        assert_eq!(lines[1], "1,alice,10.5");
        assert_eq!(lines[2], "2,bob,");
        assert_eq!(lines[3], "3,,NaN");
        assert_eq!(lines[4], "4,dan,inf");
        assert_eq!(lines[5], "5,eve,-1.25");
        assert_eq!(lines.len(), 6);
    });
}

#[test]
fn head_respects_limit() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_head(&p, 2, Format::Jsonl, BinaryFormat::None)
            .await
            .unwrap();
        assert_eq!(out.lines().count(), 2);
    });
}

#[test]
fn head_with_oversize_limit_returns_all_rows() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_head(&p, 100, Format::Jsonl, BinaryFormat::None)
            .await
            .unwrap();
        assert_eq!(out.lines().count(), 5);
    });
}

#[test]
fn tail_returns_last_rows() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_tail(&p, 2, Format::Jsonl, BinaryFormat::None)
            .await
            .unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 2);
        let v0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        let v1: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(v0["id"], 4);
        assert_eq!(v1["id"], 5);
    });
}

#[test]
fn take_supports_ranges_and_negatives() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_take(&p, "-1,0,1:2", Format::Jsonl, BinaryFormat::None)
            .await
            .unwrap();
        let ids: Vec<i64> = out
            .lines()
            .map(|l| {
                serde_json::from_str::<serde_json::Value>(l).unwrap()["id"]
                    .as_i64()
                    .unwrap()
            })
            .collect();
        assert_eq!(ids, vec![5, 1, 2, 3]);
    });
}

#[test]
fn take_out_of_range_errors() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let err = collect_take(&p, "100", Format::Jsonl, BinaryFormat::None)
            .await
            .unwrap_err();
        assert!(matches!(err, arrs::error::Error::IndexOutOfRange { .. }));
    });
}

#[test]
fn sample_is_reproducible_with_seed() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let a = collect_sample(&p, 3, 42, Format::Jsonl, BinaryFormat::None)
            .await
            .unwrap();
        let b = collect_sample(&p, 3, 42, Format::Jsonl, BinaryFormat::None)
            .await
            .unwrap();
        assert_eq!(a, b);
        assert_eq!(a.lines().count(), 3);
    });
}

#[test]
fn jsonl_binary_hex_emits_backslash_x_format() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_full(&tmp, "f").await;
        let out = collect_cat(vec![p], Format::Jsonl, BinaryFormat::Hex, None, None)
            .await
            .unwrap();
        let v0: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
        assert_eq!(v0["data"], "\\x00\\xff");
    });
}

#[test]
fn jsonl_binary_none_renders_placeholder() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_with_binary(&tmp, "b").await;
        let out = collect_cat(vec![p], Format::Jsonl, BinaryFormat::None, None, None)
            .await
            .unwrap();
        let v0: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
        assert_eq!(v0["data"], "BINARY_DATA");
        assert_eq!(v0["id"], 1);
        // Null binary values stay null.
        let v1: serde_json::Value = serde_json::from_str(out.lines().nth(1).unwrap()).unwrap();
        assert_eq!(v1["data"], serde_json::Value::Null);
    });
}

#[test]
fn csv_binary_none_renders_placeholder() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_with_binary(&tmp, "b").await;
        let out = collect_cat(vec![p], Format::Csv, BinaryFormat::None, None, None)
            .await
            .unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "id,data");
        assert_eq!(lines[1], "1,BINARY_DATA");
        // Null binary cell stays empty.
        assert_eq!(lines[2], "2,");
        assert_eq!(lines[3], "3,BINARY_DATA");
    });
}

#[test]
fn jsonl_binary_none_placeholder_for_nested_binary() {
    // Nested binary (inside a struct) should also be replaced by the placeholder
    // rather than silently becoming null.
    runtime().block_on(async {
        use arrow_array::{BinaryArray, Int32Array, RecordBatch, RecordBatchIterator, StructArray};
        use arrow_schema::{DataType, Field, Fields, Schema};
        use std::sync::Arc;

        let inner_fields: Fields = vec![
            Field::new("payload", DataType::Binary, true),
            Field::new("n", DataType::Int32, true),
        ]
        .into();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "wrap",
            DataType::Struct(inner_fields.clone()),
            true,
        )]));
        let payload = Arc::new(BinaryArray::from_opt_vec(vec![Some(b"hello".as_ref())]));
        let n = Arc::new(Int32Array::from(vec![7]));
        let wrap = StructArray::new(inner_fields, vec![payload, n], None);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(wrap)]).unwrap();
        let tmp = tempdir();
        let path = tmp.path().join("struct_bin");
        let iter = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        arrs::lance::write_dataset(&path, iter).await.unwrap();

        let out = collect_cat(vec![path], Format::Jsonl, BinaryFormat::None, None, None)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
        assert_eq!(v["wrap"]["payload"], "BINARY_DATA");
        assert_eq!(v["wrap"]["n"], 7);
    });
}

#[test]
fn csv_binary_hex_emits_escape_sequence() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_with_binary(&tmp, "b").await;
        let out = collect_cat(vec![p], Format::Csv, BinaryFormat::Hex, None, None)
            .await
            .unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "id,data");
        // csv::Writer quotes when a record field contains characters that need escaping.
        // Backslashes on their own are not special in CSV, so these render unquoted.
        assert_eq!(lines[1], r"1,\x00\xff");
        assert_eq!(lines[2], "2,");
        assert_eq!(lines[3], r"3,\x68\x69");
    });
}

#[test]
fn csv_binary_base64_is_valid_base64() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_with_binary(&tmp, "b").await;
        let out = collect_cat(vec![p], Format::Csv, BinaryFormat::Base64, None, None)
            .await
            .unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "id,data");
        assert_eq!(lines[1], "1,AP8=");
        assert_eq!(lines[2], "2,");
        assert_eq!(lines[3], "3,aGk=");
    });
}

#[test]
fn jsonl_binary_base64_emits_standard_alphabet() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_with_binary(&tmp, "b").await;
        let out = collect_cat(vec![p], Format::Jsonl, BinaryFormat::Base64, None, None)
            .await
            .unwrap();
        let lines: Vec<&str> = out.lines().collect();
        let v0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(v0["data"], "AP8=");
        let v1: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(v1["data"], serde_json::Value::Null);
        let v2: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(v2["data"], "aGk=");
    });
}

#[test]
fn explicit_include_of_binary_with_none_still_emits_placeholder() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_with_binary(&tmp, "b").await;
        let cols = vec!["id".to_string(), "data".to_string()];
        let out = collect_cat(
            vec![p],
            Format::Jsonl,
            BinaryFormat::None,
            Some(&cols),
            None,
        )
        .await
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
        assert_eq!(v["id"], 1);
        assert_eq!(v["data"], "BINARY_DATA");
    });
}

#[test]
fn jsonl_emits_lists_as_arrays() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_full(&tmp, "f").await;
        let out = collect_cat(vec![p], Format::Jsonl, BinaryFormat::Hex, None, None)
            .await
            .unwrap();
        let mut lines = out.lines();
        let v0: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        assert_eq!(v0["tags"], serde_json::json!(["a", "b"]));
        let v1: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        assert_eq!(v1["tags"], serde_json::Value::Null);
    });
}

#[test]
fn columns_preserves_user_order() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let cols = vec!["score".to_string(), "id".to_string()];
        let out = collect_cat(
            vec![p],
            Format::Jsonl,
            BinaryFormat::None,
            Some(&cols),
            None,
        )
        .await
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
        let keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["score", "id"]);
    });
}

#[test]
fn exclude_columns_drops_specified() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let excl = vec!["name".to_string()];
        let out = collect_cat(
            vec![p],
            Format::Jsonl,
            BinaryFormat::None,
            None,
            Some(&excl),
        )
        .await
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
        let keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["id", "score"]);
    });
}

#[test]
fn unknown_column_errors() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let cols = vec!["zzz".to_string()];
        let err = collect_cat(
            vec![p],
            Format::Jsonl,
            BinaryFormat::None,
            Some(&cols),
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, arrs::error::Error::UnknownColumn { .. }));
    });
}

#[test]
fn cat_table_renders_header_and_rows_in_ascii() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_cat(vec![p], Format::Table, BinaryFormat::None, None, None)
            .await
            .unwrap();
        // Test runs are non-tty → ASCII preset (uses '+', '|', '-').
        assert!(out.contains('+'), "table border missing in:\n{out}");
        assert!(out.contains("| name"), "header missing in:\n{out}");
        assert!(out.contains("| alice"), "alice row missing in:\n{out}");
        assert!(
            out.contains("NaN"),
            "NaN should render literally in:\n{out}"
        );
    });
}

#[test]
fn jsonl_emits_lists_as_arrays_table_compatibility() {
    // Table format must render nested cells as compact JSON literals, even
    // though CSV would have rejected them. Uses the full fixture which has a
    // List<Utf8> column.
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_full(&tmp, "f").await;
        let out = collect_cat(vec![p], Format::Table, BinaryFormat::None, None, None)
            .await
            .unwrap();
        assert!(
            out.contains("[\"a\",\"b\"]"),
            "list cell missing in:\n{out}"
        );
    });
}

#[test]
fn format_on_schema_errors() {
    runtime().block_on(async {
        let cli = Cli {
            format: Some(Format::Table),
            binary_format: BinaryFormat::None,
            columns: None,
            exclude_columns: None,
            command: Command::Schema {
                input: std::path::PathBuf::from("does-not-matter"),
                ty: arrs::cli::SchemaType::Arrow,
                lance: arrs::cli::LanceArgs::default(),
            },
        };
        let res = dispatch(cli).await;
        assert!(matches!(
            res,
            Err(arrs::error::Error::FormatNotApplicable { command: "schema" })
        ));
    });
}

#[test]
fn format_on_rowcount_errors() {
    runtime().block_on(async {
        let cli = Cli {
            format: Some(Format::Jsonl),
            binary_format: BinaryFormat::None,
            columns: None,
            exclude_columns: None,
            command: Command::Rowcount {
                input: std::path::PathBuf::from("does-not-matter"),
                filter: FilterArg::default(),
                lance: arrs::cli::LanceArgs::default(),
            },
        };
        let res = dispatch(cli).await;
        assert!(matches!(
            res,
            Err(arrs::error::Error::FormatNotApplicable {
                command: "rowcount"
            })
        ));
    });
}

#[test]
fn empty_cat_via_dispatch_errors() {
    runtime().block_on(async {
        let cli = Cli {
            format: Some(Format::Jsonl),
            binary_format: BinaryFormat::None,
            columns: None,
            exclude_columns: None,
            command: Command::Cat {
                inputs: vec![],
                filter: FilterArg::default(),
                lance: arrs::cli::LanceArgs::default(),
            },
        };
        let res = dispatch(cli).await;
        assert!(matches!(res, Err(arrs::error::Error::EmptyInputs)));
    });
}

#[test]
fn csv_quotes_column_name_containing_comma() {
    // csv::Writer handles quoting automatically; names with commas or newlines
    // emerge as standard-CSV-quoted tokens rather than being rejected.
    runtime().block_on(async {
        use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
        use arrow_schema::{DataType, Field, Schema};
        let tmp = tempdir();
        let path = tmp.path().join("weird");
        let schema = Arc::new(Schema::new(vec![Field::new("a,b", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap();
        let iter = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        arrs::lance::write_dataset(&path, iter).await.unwrap();
        let out = collect_cat(vec![path], Format::Csv, BinaryFormat::None, None, None)
            .await
            .unwrap();
        let mut lines = out.lines();
        assert_eq!(lines.next().unwrap(), r#""a,b""#);
        assert_eq!(lines.next().unwrap(), "1");
    });
}

// -------------------- --where predicate filtering --------------------

fn ids(out: &str) -> Vec<i64> {
    out.lines()
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).unwrap()["id"]
                .as_i64()
                .unwrap()
        })
        .collect()
}

#[test]
fn where_number_predicate_filters_and_preserves_order() {
    // Filtered scan keeps dataset order, so `head -n 1` semantics fall out of
    // taking the first matching row.
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_scan_where(&p, "id >= 3").await.unwrap();
        assert_eq!(ids(&out), vec![3, 4, 5]);
    });
}

#[test]
fn where_string_predicate_filters_rows() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_scan_where(&p, "name = 'alice'").await.unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 1);
        let v: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(v["id"], 1);
        assert_eq!(v["name"], "alice");
    });
}

#[test]
fn where_empty_result_set_yields_no_rows() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_scan_where(&p, "id > 100").await.unwrap();
        assert_eq!(out.lines().count(), 0);
    });
}

#[test]
fn rowcount_with_where_uses_filtered_count() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let ds = dataset::open(&p, None).await.unwrap();
        assert_eq!(ds.count_rows(Some("id >= 4")).await.unwrap(), 2);
        // Empty result set counts as zero, not an error.
        assert_eq!(ds.count_rows(Some("id > 100")).await.unwrap(), 0);
    });
}

#[test]
fn invalid_where_predicate_on_scan_errors() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let ds = dataset::open(&p, None).await.unwrap();
        let options = ScanOptions {
            projection: None,
            filter: Some("not_a_column > 1"),
        };
        // `scan` yields a non-Debug `BatchStream` on success, so match rather
        // than `unwrap_err`.
        match ds.scan(&options).await {
            Err(arrs::error::Error::InvalidPredicate(_)) => {}
            other => panic!("expected InvalidPredicate, got {:?}", other.map(|_| ())),
        }
    });
}

#[test]
fn invalid_where_predicate_on_rowcount_errors() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let ds = dataset::open(&p, None).await.unwrap();
        let err = ds
            .count_rows(Some("this is not sql ((("))
            .await
            .unwrap_err();
        assert!(matches!(err, arrs::error::Error::InvalidPredicate(_)));
    });
}

#[test]
fn take_with_where_is_rejected() {
    runtime().block_on(async {
        let cli = Cli {
            format: None,
            binary_format: BinaryFormat::None,
            columns: None,
            exclude_columns: None,
            command: Command::Take {
                input: std::path::PathBuf::from("does-not-matter"),
                indices: "0".to_string(),
                filter: FilterArg {
                    predicate: Some("id > 1".to_string()),
                },
                lance: LanceArgs::default(),
            },
        };
        let res = dispatch(cli).await;
        assert!(matches!(res, Err(arrs::error::Error::TakeWhereConflict)));
    });
}

#[test]
fn tail_where_returns_last_matching_rows() {
    // Matching rows are ids [1, 3, 5] (odd). The last two matching are [3, 5];
    // note this differs from "last two rows then filter" (which would be [5]).
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_tail_where(&p, 2, "id % 2 = 1").await.unwrap();
        assert_eq!(ids(&out), vec![3, 5]);
    });
}

#[test]
fn tail_where_limit_exceeds_matches_returns_all_matching() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        let out = collect_tail_where(&p, 100, "id >= 4").await.unwrap();
        assert_eq!(ids(&out), vec![4, 5]);
    });
}

#[test]
fn sample_where_samples_only_matching_rows() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        // Matching rows are ids [2, 3, 4, 5]; a size-2 sample must draw from them.
        let a = collect_sample_where(&p, 2, 7, "id >= 2").await.unwrap();
        let b = collect_sample_where(&p, 2, 7, "id >= 2").await.unwrap();
        assert_eq!(a, b, "same seed must be reproducible");
        let sampled = ids(&a);
        assert_eq!(sampled.len(), 2);
        for id in sampled {
            assert!((2..=5).contains(&id), "sampled id {id} not in matching set");
        }
    });
}

#[test]
fn sample_where_larger_than_match_count_errors() {
    runtime().block_on(async {
        let tmp = tempdir();
        let p = write_simple(&tmp, "s").await;
        // Only one row matches, so a sample of 3 is impossible.
        let err = collect_sample_where(&p, 3, 1, "id = 1").await.unwrap_err();
        assert!(matches!(
            err,
            arrs::error::Error::SampleTooLarge {
                requested: 3,
                rowcount: 1
            }
        ));
    });
}
