use std::path::Path;

use arrow_array::RecordBatch;
use futures::StreamExt;
use rand::SeedableRng;
use rand::prelude::*;
use rand_chacha::ChaCha20Rng;

use crate::Result;
use crate::cli::{BinaryFormat, Format, LanceArgs};
use crate::commands::common::{make_stdout_writer, project_arrow_schema};
use crate::dataset::{self, Dataset, ScanOptions};
use crate::error::Error;
use crate::output::RowWriter;
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
    filter: Option<&str>,
    lance: &LanceArgs,
) -> Result<()> {
    let ds = dataset::open(input, Some(lance)).await?;
    let arrow_schema = ds.arrow_schema();
    let projection = projection::resolve(&arrow_schema, columns, exclude)?;
    let projected_schema = project_arrow_schema(arrow_schema.as_ref(), projection.as_deref());

    let mut writer = make_stdout_writer(format, binary_format);
    writer.start(&projected_schema)?;

    match filter {
        // Without a filter the row count is known, so shuffle positional
        // indices and `take` the chosen rows in one shot.
        None => {
            sample_by_index(
                ds.as_ref(),
                limit,
                seed,
                projection.as_deref(),
                writer.as_mut(),
            )
            .await?
        }
        // With a filter we can't know how many rows match without scanning, and
        // positional indices no longer address the matching rows — reservoir
        // sample over the filtered stream instead.
        Some(pred) => {
            sample_by_reservoir(
                ds.as_ref(),
                limit,
                seed,
                projection.as_deref(),
                pred,
                writer.as_mut(),
            )
            .await?
        }
    }

    writer.finish()?;
    Ok(())
}

/// Build the RNG the sampler draws from: deterministic when a seed is given,
/// otherwise seeded from the thread RNG. `ChaCha20Rng` in both arms keeps the
/// type uniform and `Send`.
fn make_rng(seed: Option<u64>) -> ChaCha20Rng {
    match seed {
        Some(s) => ChaCha20Rng::seed_from_u64(s),
        None => ChaCha20Rng::from_rng(&mut rand::rng()),
    }
}

/// No-filter fast path: pick `limit` distinct positions from `0..rowcount` and
/// materialise them with a single `take`.
async fn sample_by_index(
    ds: &dyn Dataset,
    limit: u64,
    seed: Option<u64>,
    projection: Option<&[String]>,
    writer: &mut dyn RowWriter,
) -> Result<()> {
    let rowcount = ds.count_rows(None).await?;
    if limit > rowcount {
        return Err(Error::SampleTooLarge {
            requested: limit,
            rowcount,
        });
    }
    if limit == 0 {
        return Ok(());
    }

    let mut pool: Vec<u64> = (0..rowcount).collect();
    let mut rng = make_rng(seed);
    pool.shuffle(&mut rng);
    pool.truncate(limit as usize);

    let batch = ds.take(&pool, projection).await?;
    writer.write_batch(&batch)?;
    Ok(())
}

/// Filtered path: reservoir-sample (Algorithm R) `limit` rows in a single pass
/// over the matching rows. The reservoir holds one-row slices, so peak memory
/// is bounded by `limit` (plus the source batches those slices reference)
/// rather than the whole filtered result set.
async fn sample_by_reservoir(
    ds: &dyn Dataset,
    limit: u64,
    seed: Option<u64>,
    projection: Option<&[String]>,
    filter: &str,
    writer: &mut dyn RowWriter,
) -> Result<()> {
    if limit == 0 {
        return Ok(());
    }

    let options = ScanOptions {
        projection,
        filter: Some(filter),
    };
    let mut stream = ds.scan(&options).await?;

    let cap = limit as usize;
    let mut rng = make_rng(seed);
    let mut reservoir: Vec<RecordBatch> = Vec::with_capacity(cap);
    let mut seen: u64 = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        for r in 0..batch.num_rows() {
            let row = batch.slice(r, 1);
            if reservoir.len() < cap {
                reservoir.push(row);
            } else {
                // Keep the incoming row with probability cap/(seen+1).
                let j = rng.random_range(0..=seen);
                if (j as usize) < cap {
                    reservoir[j as usize] = row;
                }
            }
            seen += 1;
        }
    }

    if limit > seen {
        return Err(Error::SampleTooLarge {
            requested: limit,
            rowcount: seen,
        });
    }

    let schema = reservoir[0].schema();
    let combined = arrow::compute::concat_batches(&schema, &reservoir)?;
    writer.write_batch(&combined)?;
    Ok(())
}
