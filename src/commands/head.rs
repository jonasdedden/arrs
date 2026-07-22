use futures::StreamExt;

use crate::Result;
use crate::cli::{Format, LanceArgs};
use crate::commands::common::{make_stdout_writer, project_arrow_schema};
use crate::dataset::{self, ScanOptions};
use crate::output::RenderOptions;
use crate::projection;

#[allow(clippy::too_many_arguments)]
pub async fn run(
    input: &str,
    limit: u64,
    format: Format,
    render: RenderOptions,
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
    filter: Option<&str>,
    lance: &LanceArgs,
) -> Result<()> {
    let ds = dataset::open(input, Some(lance)).await?;
    let arrow_schema = ds.arrow_schema();
    let projection = projection::resolve(&arrow_schema, columns, exclude)?;
    let projected_schema = project_arrow_schema(arrow_schema.as_ref(), projection.as_deref());

    // Open the scan before emitting the header: the adapter validates the
    // predicate eagerly, so an invalid `--where` must not leave a stray header
    // on stdout.
    let mut stream = if limit > 0 {
        let options = ScanOptions {
            projection: projection.as_deref(),
            filter,
        };
        Some(ds.scan(&options).await?)
    } else {
        None
    };

    let mut writer = make_stdout_writer(format, render);
    writer.start(&projected_schema)?;

    if let Some(stream) = stream.as_mut() {
        let mut remaining = limit;
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
    }
    writer.finish()?;
    Ok(())
}
