//! Output writers: CSV, JSONL, and table.
//!
//! Each writer consumes `RecordBatch`es whose schema has already been projected
//! by the caller. Type formatting rules live in `value`. The factory
//! `make_writer` dispatches `Format` → concrete writer; CLI-side stdout
//! convenience lives in `commands::common::make_stdout_writer`.

pub mod csv;
pub mod jsonl;
pub mod table;
pub mod value;

use std::io::Write;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::Result;
use crate::cli::{BinaryFormat, Format};
use crate::output::table::TableStyle;

pub trait RowWriter {
    fn start(&mut self, schema: &SchemaRef) -> Result<()>;
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}

/// Construct a `RowWriter` for `format`, writing to `out`. `table_style`
/// is consulted only for `Format::Table`; other formats ignore it.
pub fn make_writer<'w, W: Write + 'w>(
    format: Format,
    binary_format: BinaryFormat,
    table_style: TableStyle,
    out: W,
) -> Box<dyn RowWriter + 'w> {
    match format {
        Format::Csv => Box::new(csv::CsvRowWriter::new(out, binary_format)),
        Format::Jsonl => Box::new(jsonl::JsonlRowWriter::new(out, binary_format)),
        Format::Table => Box::new(table::TableRowWriter::new(out, binary_format, table_style)),
    }
}
