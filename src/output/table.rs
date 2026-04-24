use std::io::Write;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use comfy_table::{Table, presets};

use crate::Result;
use crate::cli::BinaryFormat;
use crate::output::{RowWriter, value};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TableStyle {
    Pretty,
    Plain,
}

/// Writes a comfy-table-rendered grid to `W` once `finish()` is called.
///
/// Buffers every row from `write_batch` because the renderer needs the full
/// data set to compute column widths. This is fine for the metadata commands
/// (small row counts by construction), but is the reason `Format::Table` is
/// not the default for `cat`/`head`/etc., which want streaming behaviour.
pub struct TableRowWriter<W: Write> {
    writer: W,
    binary_format: BinaryFormat,
    table_style: TableStyle,
    schema: Option<SchemaRef>,
    rows: Vec<Vec<String>>,
}

impl<W: Write> TableRowWriter<W> {
    pub fn new(writer: W, binary_format: BinaryFormat, table_style: TableStyle) -> Self {
        Self {
            writer,
            binary_format,
            table_style,
            schema: None,
            rows: Vec::new(),
        }
    }
}

impl<W: Write> RowWriter for TableRowWriter<W> {
    fn start(&mut self, schema: &SchemaRef) -> Result<()> {
        self.schema = Some(schema.clone());
        Ok(())
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let rows = (0..batch.num_rows())
            .map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| value::table_cell(column.as_ref(), row, self.binary_format))
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        self.rows.extend(rows);
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        let schema = self
            .schema
            .as_ref()
            .expect("start() must be called before finish()");

        let mut table = Table::new();
        // Pretty borders only when stdout is a real terminal — pipelines and
        // captured-output test runs get an ASCII grid that's grep-friendly.
        let preset = match self.table_style {
            TableStyle::Plain => presets::ASCII_FULL,
            TableStyle::Pretty => presets::UTF8_FULL,
        };
        table.load_preset(preset);

        let headers: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        table.set_header(headers);

        table.add_rows(self.rows.drain(..));

        writeln!(self.writer, "{table}")?;
        self.writer.flush()?;
        Ok(())
    }
}
