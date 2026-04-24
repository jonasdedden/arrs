use std::path::Path;

use rand::SeedableRng;
use rand::prelude::*;
use rand_chacha::ChaCha20Rng;

use crate::Result;
use crate::cli::{BinaryFormat, Format, LanceArgs};
use crate::commands::common::{make_stdout_writer, project_arrow_schema};
use crate::dataset;
use crate::error::Error;
use crate::projection;

#[allow(clippy::too_many_arguments)]
pub async fn run(
    input: &Path,
    limit: u64,
    seed: Option<u64>,
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
    if limit > rowcount {
        return Err(Error::SampleTooLarge {
            requested: limit,
            rowcount,
        });
    }

    let indices = if limit == 0 {
        Vec::new()
    } else {
        let mut pool: Vec<u64> = (0..rowcount).collect();
        match seed {
            Some(s) => {
                let mut rng = ChaCha20Rng::seed_from_u64(s);
                pool.shuffle(&mut rng);
            }
            None => {
                let mut rng = rand::rng();
                pool.shuffle(&mut rng);
            }
        }
        pool.truncate(limit as usize);
        pool
    };

    let mut writer = make_stdout_writer(format, binary_format);
    writer.start(&projected_schema)?;
    if !indices.is_empty() {
        let batch = ds.take(&indices, projection.as_deref()).await?;
        writer.write_batch(&batch)?;
    }
    writer.finish()?;
    Ok(())
}
