use std::path::Path;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};

use crate::Result;
use crate::cli::{BinaryFormat, Format, LanceArgs};
use crate::commands::common::make_stdout_writer;
use crate::dataset::{self, FragmentInfo};
use crate::error::Error;

pub async fn run(
    input: &Path,
    lance: &LanceArgs,
    verbose: bool,
    no_size: bool,
    format: Format,
    binary_format: BinaryFormat,
) -> Result<()> {
    let ds = dataset::open(input, Some(lance)).await?;
    let lance_caps = ds.lance().ok_or_else(|| Error::NotLance {
        command: "fragments",
        path: input.to_path_buf(),
    })?;

    let fragments = lance_caps.list_fragments(!no_size).await?;

    // `size` renders differently per format: a human-readable string in the
    // table (bytes are noise when eyeballing), raw bytes everywhere else so the
    // output stays machine-parseable. The `files` column is wide, so it's only
    // added to the table under `--verbose`; jsonl/csv always carry it.
    let human_size = format == Format::Table;
    let include_files = verbose || format != Format::Table;

    let (schema, columns) = build_batch(&fragments, human_size, include_files);
    let batch = RecordBatch::try_new(schema.clone(), columns)?;

    let mut writer = make_stdout_writer(format, binary_format);
    writer.start(&schema)?;
    writer.write_batch(&batch)?;
    writer.finish()?;
    // Drop the writer (flushing its buffered stdout) before the summary so the
    // two don't interleave.
    drop(writer);

    if format == Format::Table {
        print_summary(&fragments, no_size);
    }
    Ok(())
}

/// Build the output schema and columns for the fragment rows. `human_size`
/// picks a `Utf8` (formatted) vs `UInt64` (raw) `size` column; `include_files`
/// toggles the comma-joined `files` column.
fn build_batch(
    fragments: &[FragmentInfo],
    human_size: bool,
    include_files: bool,
) -> (Arc<Schema>, Vec<Arc<dyn Array>>) {
    let mut fields = vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("physical_rows", DataType::UInt64, false),
        Field::new("deleted_rows", DataType::UInt64, false),
        Field::new("num_files", DataType::UInt64, false),
    ];
    let mut columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(UInt64Array::from(
            fragments.iter().map(|f| f.id).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            fragments
                .iter()
                .map(|f| f.physical_rows)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            fragments.iter().map(|f| f.deleted_rows).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            fragments.iter().map(|f| f.num_files).collect::<Vec<_>>(),
        )),
    ];

    if include_files {
        fields.push(Field::new("files", DataType::Utf8, false));
        columns.push(Arc::new(StringArray::from(
            fragments
                .iter()
                .map(|f| f.files.join(","))
                .collect::<Vec<_>>(),
        )));
    }

    if human_size {
        fields.push(Field::new("size", DataType::Utf8, true));
        columns.push(Arc::new(StringArray::from(
            fragments
                .iter()
                .map(|f| f.size.map(human_bytes))
                .collect::<Vec<_>>(),
        )));
    } else {
        fields.push(Field::new("size", DataType::UInt64, true));
        columns.push(Arc::new(UInt64Array::from(
            fragments.iter().map(|f| f.size).collect::<Vec<_>>(),
        )));
    }

    (Arc::new(Schema::new(fields)), columns)
}

/// Print the trailing table-mode summary: totals across all fragments. The
/// byte total is omitted when sizes weren't computed (`--no-size`).
fn print_summary(fragments: &[FragmentInfo], no_size: bool) {
    let total_physical: u64 = fragments.iter().map(|f| f.physical_rows).sum();
    let total_deleted: u64 = fragments.iter().map(|f| f.deleted_rows).sum();
    let mut line = format!(
        "{} fragment(s), {} physical row(s), {} deleted row(s)",
        fragments.len(),
        total_physical,
        total_deleted
    );
    if !no_size {
        let total_bytes: u64 = fragments.iter().filter_map(|f| f.size).sum();
        line.push_str(&format!(", {} total", human_bytes(total_bytes)));
    }
    println!("{line}");
}

/// Format a byte count with binary (KiB/MiB/…) units for human consumption.
fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}
