use crate::Result;
use crate::cli::{Format, LanceArgs};
use crate::commands::common::make_stdout_writer;
use crate::dataset;
use crate::output::RenderOptions;
use crate::projection;
use crate::stats;

/// `arrs stats`: per-column summary statistics (a `df.describe()` for datasets).
///
/// Streams the dataset once, folding `arrow` aggregate state per column, and
/// prints one row per column through the shared metadata-table writer, so
/// `--format table|jsonl|csv` all work. Respects `--columns`/`--exclude-columns`
/// and `--where`.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    input: &str,
    format: Format,
    render: RenderOptions,
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
    filter: Option<&str>,
    lance: &LanceArgs,
) -> Result<()> {
    let ds = dataset::open(input, Some(lance)).await?;
    let schema = ds.arrow_schema();
    let projection = projection::resolve(&schema, columns, exclude)?;

    let column_stats = stats::compute(ds.as_ref(), projection.as_deref(), filter).await?;
    let batch = stats::to_record_batch(&column_stats)?;

    let mut writer = make_stdout_writer(format, render);
    writer.start(&stats::output_schema())?;
    writer.write_batch(&batch)?;
    writer.finish()?;
    Ok(())
}
