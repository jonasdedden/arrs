use std::io::Write;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use serde_json::{Map as JsonMap, Value};

use crate::Result;
use crate::cli::BinaryFormat;
use crate::output::{RowWriter, value};

pub struct JsonlRowWriter<W: Write> {
    writer: W,
    field_names: Vec<String>,
    binary_format: BinaryFormat,
}

impl<W: Write> JsonlRowWriter<W> {
    pub fn new(writer: W, binary_format: BinaryFormat) -> Self {
        Self {
            writer,
            field_names: Vec::new(),
            binary_format,
        }
    }
}

impl<W: Write> RowWriter for JsonlRowWriter<W> {
    fn start(&mut self, schema: &SchemaRef) -> Result<()> {
        self.field_names = schema.fields().iter().map(|f| f.name().clone()).collect();
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
            let mut obj = JsonMap::with_capacity(num_cols);
            for col in 0..num_cols {
                let arr = batch.column(col);
                let v = value::json_value(arr.as_ref(), row, self.binary_format)?;
                obj.insert(self.field_names[col].clone(), v);
            }
            serde_json::to_writer(&mut self.writer, &Value::Object(obj))?;
            self.writer.write_all(b"\n")?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}
