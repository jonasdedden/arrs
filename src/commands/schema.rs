use std::path::Path;

use crate::Result;
use crate::cli::{LanceArgs, SchemaType};
use crate::commands::common::project_arrow_schema;
use crate::dataset;
use crate::projection;

pub async fn run(
    input: &Path,
    ty: SchemaType,
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
    lance: &LanceArgs,
) -> Result<()> {
    let ds = dataset::open(input, Some(lance)).await?;
    let arrow_schema = ds.arrow_schema();
    let projection = projection::resolve(&arrow_schema, columns, exclude)?;

    match ty {
        SchemaType::Arrow => {
            let projected = project_arrow_schema(arrow_schema.as_ref(), projection.as_deref());
            println!("{:#?}", projected.as_ref());
        }
        SchemaType::Physical => {
            let debug = ds.physical_schema_debug(projection.as_deref())?;
            println!("{debug}");
        }
    }
    Ok(())
}
