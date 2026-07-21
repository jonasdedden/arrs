//! Helpers shared by in-crate unit tests. Compiled only under `cfg(test)`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::types::Int32Type;
use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lance::Dataset as InnerLance;

fn int_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
}

fn int_batch(ids: &[i32]) -> RecordBatch {
    RecordBatch::try_new(int_schema(), vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap()
}

/// Write a single-column (`id: Int32`) Lance dataset with one fragment per
/// slice in `fragments` — the first is the initial write, the rest are appends.
/// Multiple fragments make a scan yield multiple batches, which is what the
/// streaming `tail`/`sample` paths need to be exercised properly.
pub async fn write_int_fragments(dir: &Path, name: &str, fragments: &[&[i32]]) -> PathBuf {
    let path = dir.join(name);
    let uri = path.to_string_lossy().into_owned();

    let (first, rest) = fragments.split_first().expect("at least one fragment");
    let iter = RecordBatchIterator::new(vec![Ok(int_batch(first))].into_iter(), int_schema());
    let mut ds = InnerLance::write(iter, uri.as_str(), None).await.unwrap();
    for frag in rest {
        let iter = RecordBatchIterator::new(vec![Ok(int_batch(frag))].into_iter(), int_schema());
        ds.append(iter, None).await.unwrap();
    }
    path
}

/// Flatten the `id` column of a sequence of batches into a `Vec<i32>`, in order.
pub fn collect_ids(batches: &[RecordBatch]) -> Vec<i32> {
    let mut ids = Vec::new();
    for batch in batches {
        let col = batch.column_by_name("id").expect("id column");
        ids.extend(col.as_primitive::<Int32Type>().values().iter().copied());
    }
    ids
}
