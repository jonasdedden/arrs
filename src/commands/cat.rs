use futures::StreamExt;

use crate::Result;
use crate::cli::{Format, LanceArgs};
use crate::commands::common::{make_stdout_writer, project_arrow_schema, schemas_match};
use crate::dataset::{self, ScanOptions};
use crate::error::Error;
use crate::output::RenderOptions;
use crate::projection;

pub async fn run(
    inputs: &[String],
    format: Format,
    render: RenderOptions,
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
    filter: Option<&str>,
    lance: &LanceArgs,
) -> Result<()> {
    if inputs.is_empty() {
        return Err(Error::EmptyInputs);
    }

    let mut opened = Vec::with_capacity(inputs.len());
    for path in inputs {
        opened.push(dataset::open(path, Some(lance)).await?);
    }

    let first_schema = opened[0].arrow_schema();
    let projection = projection::resolve(&first_schema, columns, exclude)?;
    let projected_schema = project_arrow_schema(first_schema.as_ref(), projection.as_deref());

    for (ds, path) in opened.iter().zip(inputs.iter()).skip(1) {
        let other = ds.arrow_schema();
        if let Err(field) = schemas_match(&first_schema, &other) {
            return Err(Error::SchemaMismatch {
                left: inputs[0].clone(),
                right: path.clone(),
                field,
            });
        }
    }

    let options = ScanOptions {
        projection: projection.as_deref(),
        filter,
    };
    // Open every scan first: the adapter validates the predicate eagerly, so a
    // bad `--where` errors here, before we emit the output header to stdout.
    let mut streams = Vec::with_capacity(opened.len());
    for ds in &opened {
        streams.push(ds.scan(&options).await?);
    }

    let mut writer = make_stdout_writer(format, render);
    writer.start(&projected_schema)?;
    for mut stream in streams {
        while let Some(batch) = stream.next().await {
            writer.write_batch(&batch?)?;
        }
    }
    writer.finish()?;
    Ok(())
}
