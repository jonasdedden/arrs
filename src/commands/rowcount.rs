use std::path::Path;

use crate::Result;
use crate::cli::LanceArgs;
use crate::dataset;

pub async fn run(input: &Path, lance: &LanceArgs) -> Result<()> {
    let ds = dataset::open(input, Some(lance)).await?;
    let n = ds.count_rows().await?;
    println!("{n}");
    Ok(())
}
