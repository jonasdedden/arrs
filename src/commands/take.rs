use std::path::Path;

use crate::Result;
use crate::cli::{BinaryFormat, Format, LanceArgs};
use crate::commands::common::{make_stdout_writer, project_arrow_schema};
use crate::dataset;
use crate::indices;
use crate::projection;

pub async fn run(
    input: &Path,
    indices_raw: &str,
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

    let rowcount = ds.count_rows().await?;
    let indices = indices::resolve(indices_raw, rowcount)?;

    let mut writer = make_stdout_writer(format, binary_format);
    writer.start(&projected_schema)?;

    if !indices.is_empty() {
        let batch = ds.take(&indices, projection.as_deref()).await?;
        writer.write_batch(&batch)?;
    }
    writer.finish()?;
    Ok(())
}
