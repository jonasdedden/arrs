use std::io::Write;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::Result;
use crate::output::{RenderOptions, RowWriter, value};

pub struct CsvRowWriter<W: Write> {
    writer: Option<Box<csv::Writer<W>>>,
    render: RenderOptions,
}

impl<W: Write> CsvRowWriter<W> {
    pub fn new(writer: W, render: RenderOptions) -> Self {
        let inner = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(writer);
        Self {
            writer: Some(Box::new(inner)),
            render,
        }
    }
}

impl<W: Write> RowWriter for CsvRowWriter<W> {
    fn start(&mut self, schema: &SchemaRef) -> Result<()> {
        value::validate_csv_schema(schema)?;
        let w = self.writer.as_mut().expect("start() called twice");
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        w.write_record(&names)?;
        Ok(())
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let w = self.writer.as_mut().expect("writer already finished");
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();
        let mut row_buf: Vec<String> = Vec::with_capacity(num_cols);
        for row in 0..num_rows {
            row_buf.clear();
            for col in 0..num_cols {
                let arr = batch.column(col);
                row_buf.push(value::csv_cell(arr.as_ref(), row, self.render)?.unwrap_or_default());
            }
            w.write_record(&row_buf)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if let Some(w) = self.writer.take() {
            let mut inner = w.into_inner().map_err(|e| e.into_error())?;
            inner.flush()?;
        }
        Ok(())
    }
}
