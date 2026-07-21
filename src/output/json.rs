//! `--format json`: a single, well-formed JSON array streamed with constant
//! memory.
//!
//! A thin sibling of [`crate::output::jsonl`]: it serializes the *same*
//! per-row objects, but wraps them in one array — `[`, comma-separated objects,
//! `]` — instead of newline-delimiting them. Exactly one object is materialized
//! at a time, so memory stays flat regardless of row count. Empty input yields
//! `[]`. Output is identical whether stdout is a terminal or a pipe.

use std::io::Write;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use serde_json::{Map as JsonMap, Value};

use crate::Result;
use crate::output::{RenderOptions, RowWriter, value};

pub struct JsonRowWriter<W: Write> {
    writer: W,
    field_names: Vec<String>,
    render: RenderOptions,
    /// Whether at least one object has been written, so the next one is
    /// prefixed with a comma.
    wrote_any: bool,
}

impl<W: Write> JsonRowWriter<W> {
    pub fn new(writer: W, render: RenderOptions) -> Self {
        Self {
            writer,
            field_names: Vec::new(),
            render,
            wrote_any: false,
        }
    }
}

impl<W: Write> RowWriter for JsonRowWriter<W> {
    fn start(&mut self, schema: &SchemaRef) -> Result<()> {
        self.field_names = schema.fields().iter().map(|f| f.name().clone()).collect();
        self.writer.write_all(b"[")?;
        Ok(())
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        debug_assert_eq!(
            self.field_names.len(),
            batch.num_columns(),
            "start() must be called with the batch's schema"
        );
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();
        for row in 0..num_rows {
            if self.wrote_any {
                self.writer.write_all(b",")?;
            }
            let mut obj = JsonMap::with_capacity(num_cols);
            for col in 0..num_cols {
                let arr = batch.column(col);
                let v = value::json_value(arr.as_ref(), row, self.render)?;
                obj.insert(self.field_names[col].clone(), v);
            }
            serde_json::to_writer(&mut self.writer, &Value::Object(obj))?;
            self.wrote_any = true;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.writer.write_all(b"]\n")?;
        self.writer.flush()?;
        Ok(())
    }
}
