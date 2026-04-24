use std::path::Path;

use crate::Result;
use crate::cli::{BinaryFormat, Format, LanceArgs};
use crate::commands::common::{make_stdout_writer, project_arrow_schema};
use crate::dataset;
use crate::projection;

pub async fn run(
    input: &Path,
    limit: u64,
    format: Format,
    binary_format: BinaryFormat,
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
    lance: &LanceArgs,
) -> Result<()> {
    let ds = dataset::open(input, Some(lance)).await?;
    let arrow_schema = ds.arrow_schema();
    let projection = projection::resolve(&arrow_schema, columns, exclude)?;
    let projected_schema = project_arrow_schema(arrow_schema.as_ref(), projection.as_deref());

    let mut writer = make_stdout_writer(format, binary_format);
    writer.start(&projected_schema)?;

    let rowcount = ds.count_rows().await?;
    let take_count = limit.min(rowcount);
    if take_count == 0 {
        writer.finish()?;
        return Ok(());
    }
    let start = rowcount - take_count;
    let indices: Vec<u64> = (start..rowcount).collect();
    let batch = ds.take(&indices, projection.as_deref()).await?;
    writer.write_batch(&batch)?;
    writer.finish()?;
    Ok(())
}
