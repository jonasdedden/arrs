use crate::Result;
use crate::cli::LanceArgs;
use crate::dataset;

pub async fn run(input: &str, filter: Option<&str>, lance: &LanceArgs) -> Result<()> {
    let ds = dataset::open(input, Some(lance)).await?;
    // When a filter is set the adapter uses its native filtered count (for
    // Lance, pushed into scalar indices when available) rather than scanning.
    let n = ds.count_rows(filter).await?;
    println!("{n}");
    Ok(())
}
