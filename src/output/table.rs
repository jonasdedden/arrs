use std::io::Write;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use comfy_table::{Table, presets};

use crate::Result;
use crate::output::{RenderOptions, RowWriter, value};

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
    render: RenderOptions,
    table_style: TableStyle,
    schema: Option<SchemaRef>,
    rows: Vec<Vec<String>>,
}

impl<W: Write> TableRowWriter<W> {
    pub fn new(writer: W, render: RenderOptions, table_style: TableStyle) -> Self {
        Self {
            writer,
            render,
            table_style,
            schema: None,
            rows: Vec::new(),
        }
    }
}

/// Truncate a rendered cell to at most `max` characters, replacing the tail
/// with `…` when shortened. Character-based (never splits a multi-byte UTF-8
/// codepoint); the marker itself counts as one of the `max` characters, so
/// `max == 0` collapses any non-empty cell to a bare `…`. Applied only to data
/// cells — header cells (column names) are never truncated.
fn truncate_cell(s: String, max: usize) -> String {
    if s.chars().count() <= max {
        return s;
    }
    let keep = max.saturating_sub(1);
    let mut out: String = s.chars().take(keep).collect();
    out.push('…');
    out
}

impl<W: Write> RowWriter for TableRowWriter<W> {
    fn start(&mut self, schema: &SchemaRef) -> Result<()> {
        self.schema = Some(schema.clone());
        Ok(())
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let max_width = self.render.max_cell_width;
        let rows = (0..batch.num_rows())
            .map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        let cell = value::table_cell(column.as_ref(), row, self.render)?;
                        Ok(match max_width {
                            Some(max) => truncate_cell(cell, max),
                            None => cell,
                        })
                    })
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
