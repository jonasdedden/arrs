use std::path::Path;

use futures::StreamExt;

use crate::Result;
use crate::cli::{BinaryFormat, Format, LanceArgs};
use crate::commands::common::{make_stdout_writer, project_arrow_schema};
use crate::dataset::{self, ScanOptions};
use crate::projection;

#[allow(clippy::too_many_arguments)]
pub async fn run(
    input: &Path,
    limit: u64,
    format: Format,
    binary_format: BinaryFormat,
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
    filter: Option<&str>,
    lance: &LanceArgs,
) -> Result<()> {
    let ds = dataset::open(input, Some(lance)).await?;
    let arrow_schema = ds.arrow_schema();
    let projection = projection::resolve(&arrow_schema, columns, exclude)?;
    let projected_schema = project_arrow_schema(arrow_schema.as_ref(), projection.as_deref());

    let mut writer = make_stdout_writer(format, binary_format);
    writer.start(&projected_schema)?;

    let mut remaining = limit;
    if remaining == 0 {
        writer.finish()?;
        return Ok(());
    }

    let options = ScanOptions {
        projection: projection.as_deref(),
        filter,
    };
    let mut stream = ds.scan(&options).await?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let rows = batch.num_rows() as u64;
        if rows <= remaining {
            writer.write_batch(&batch)?;
            remaining -= rows;
        } else {
            let slice = batch.slice(0, remaining as usize);
            writer.write_batch(&slice)?;
            remaining = 0;
        }
        if remaining == 0 {
            break;
        }
    }
    writer.finish()?;
    Ok(())
}
