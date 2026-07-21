use std::collections::VecDeque;
use std::path::Path;

use arrow_array::RecordBatch;
use futures::StreamExt;

use crate::Result;
use crate::cli::{BinaryFormat, Format, LanceArgs};
use crate::commands::common::{make_stdout_writer, project_arrow_schema};
use crate::dataset::{self, Dataset, ScanOptions};
use crate::output::RowWriter;
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

    if limit > 0 {
        match filter {
            // Without a filter the row count is known up front, so we can jump
            // straight to the last `N` rows with a positional `take`.
            None => {
                tail_by_take(ds.as_ref(), limit, projection.as_deref(), writer.as_mut()).await?
            }
            // With a filter, positional indices no longer line up with the
            // matching rows, so stream the filtered rows and keep the tail.
            Some(pred) => {
                tail_by_stream(
                    ds.as_ref(),
                    limit,
                    projection.as_deref(),
                    pred,
                    writer.as_mut(),
                )
                .await?
            }
        }
    }

    writer.finish()?;
    Ok(())
}

/// Fast path: no filter, so `count_rows` + a single `take` of the trailing
/// indices is exact and cheap.
async fn tail_by_take(
    ds: &dyn Dataset,
    limit: u64,
    projection: Option<&[String]>,
    writer: &mut dyn RowWriter,
) -> Result<()> {
    let rowcount = ds.count_rows(None).await?;
    let take_count = limit.min(rowcount);
    if take_count == 0 {
        return Ok(());
    }
    let start = rowcount - take_count;
    let indices: Vec<u64> = (start..rowcount).collect();
    let batch = ds.take(&indices, projection).await?;
    writer.write_batch(&batch)?;
    Ok(())
}

/// Filtered path: stream the matching rows, retaining only enough trailing
/// batches to cover the last `limit` rows. Memory is bounded to roughly
/// `limit` rows plus one batch rather than the whole (filtered) result set.
async fn tail_by_stream(
    ds: &dyn Dataset,
    limit: u64,
    projection: Option<&[String]>,
    filter: &str,
    writer: &mut dyn RowWriter,
) -> Result<()> {
    let options = ScanOptions {
        projection,
        filter: Some(filter),
    };
    let mut stream = ds.scan(&options).await?;

    let mut buffered: VecDeque<RecordBatch> = VecDeque::new();
    let mut buffered_rows: u64 = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        buffered_rows += batch.num_rows() as u64;
        buffered.push_back(batch);
        // Drop leading batches that can't hold any of the last `limit` rows.
        while let Some(front) = buffered.front() {
            let front_rows = front.num_rows() as u64;
            if buffered_rows - front_rows >= limit {
                buffered_rows -= front_rows;
                buffered.pop_front();
            } else {
                break;
            }
        }
    }

    // Emit the trailing `limit` rows, slicing the first retained batch when the
    // buffer overshoots.
    let mut skip = buffered_rows - limit.min(buffered_rows);
    for batch in buffered {
        let rows = batch.num_rows() as u64;
        if skip >= rows {
            skip -= rows;
            continue;
        }
        let slice = batch.slice(skip as usize, (rows - skip) as usize);
        writer.write_batch(&slice)?;
        skip = 0;
    }
    Ok(())
}
