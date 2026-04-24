use std::path::Path;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray, TimestampMicrosecondArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

use crate::Result;
use crate::cli::{BinaryFormat, Format};
use crate::commands::common::make_stdout_writer;
use crate::dataset;
use crate::error::Error;

pub async fn run(input: &Path, format: Format, binary_format: BinaryFormat) -> Result<()> {
    let ds = dataset::open(input, None).await?;
    let lance = ds.lance().ok_or_else(|| Error::NotLance {
        command: "branches",
        path: input.to_path_buf(),
    })?;

    let branches = lance.list_branches().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("parent_branch", DataType::Utf8, true),
        Field::new("parent_version", DataType::UInt64, true),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]));
    let name_col: Arc<dyn Array> = Arc::new(StringArray::from(
        branches.iter().map(|b| b.name.as_str()).collect::<Vec<_>>(),
    ));
    let parent_col: Arc<dyn Array> = Arc::new(StringArray::from(
        branches
            .iter()
            .map(|b| b.parent_branch.as_deref())
            .collect::<Vec<_>>(),
    ));
    let parent_version_col: Arc<dyn Array> = Arc::new(UInt64Array::from(
        branches
            .iter()
            .map(|b| b.parent_version)
            .collect::<Vec<_>>(),
    ));
    let created_at_col: Arc<dyn Array> = Arc::new(
        TimestampMicrosecondArray::from(
            branches
                .iter()
                .map(|b| b.created_at.map(|t| t.timestamp_micros()))
                .collect::<Vec<_>>(),
        )
        .with_timezone("UTC"),
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![name_col, parent_col, parent_version_col, created_at_col],
    )?;

    let mut writer = make_stdout_writer(format, binary_format);
    writer.start(&schema)?;
    writer.write_batch(&batch)?;
    writer.finish()?;
    Ok(())
}
