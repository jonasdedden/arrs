use std::path::PathBuf;

use futures::StreamExt;

use crate::Result;
use crate::cli::{BinaryFormat, Format, LanceArgs};
use crate::commands::common::{make_stdout_writer, project_arrow_schema, schemas_match};
use crate::dataset;
use crate::error::Error;
use crate::projection;

pub async fn run(
    inputs: &[PathBuf],
    format: Format,
    binary_format: BinaryFormat,
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
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

    let mut writer = make_stdout_writer(format, binary_format);
    writer.start(&projected_schema)?;

    for ds in &opened {
        let mut stream = ds.scan(projection.as_deref()).await?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            writer.write_batch(&batch)?;
        }
    }
    writer.finish()?;
    Ok(())
}
