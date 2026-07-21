//! Output writers: CSV, JSONL, and table.
//!
//! Each writer consumes `RecordBatch`es whose schema has already been projected
//! by the caller. Type formatting rules live in `value`. The factory
//! `make_writer` dispatches `Format` → concrete writer; CLI-side stdout
//! convenience lives in `commands::common::make_stdout_writer`.

pub mod csv;
pub mod json;
pub mod jsonl;
pub mod table;
pub mod value;

use std::io::Write;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::Result;
use crate::cli::{BinaryFormat, Format};
use crate::output::table::TableStyle;

/// Bundle of user-facing rendering knobs threaded through every writer and the
/// `value` formatters. `binary_format` is folded in here (as the issue
/// sanctions) so a single `Copy` value flows where `BinaryFormat` used to,
/// instead of growing every signature with three more scalars. The truncation
/// and precision fields are `None` by default, which reproduces the original
/// output byte-for-byte.
#[derive(Debug, Copy, Clone)]
pub struct RenderOptions {
    /// How to encode binary/large-binary/fixed-size-binary/binary-view cells.
    pub binary_format: BinaryFormat,
    /// Max elements rendered per list level before a `… (K more)` marker.
    pub max_list_items: Option<usize>,
    /// Table-only: max characters per rendered cell before a trailing `…`.
    pub max_cell_width: Option<usize>,
    /// Fractional digits for f16/f32/f64 (NaN/inf unaffected).
    pub float_precision: Option<usize>,
}

impl RenderOptions {
    /// Options that only set the binary format; everything else defaults to the
    /// original (unlimited / full-precision) behavior.
    pub fn new(binary_format: BinaryFormat) -> Self {
        Self {
            binary_format,
            max_list_items: None,
            max_cell_width: None,
            float_precision: None,
        }
    }
}

impl Default for RenderOptions {
    fn default() -> Self {
        Self::new(BinaryFormat::None)
    }
}

impl From<BinaryFormat> for RenderOptions {
    fn from(binary_format: BinaryFormat) -> Self {
        Self::new(binary_format)
    }
}

pub trait RowWriter {
    fn start(&mut self, schema: &SchemaRef) -> Result<()>;
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}

/// Construct a `RowWriter` for `format`, writing to `out`. `table_style`
/// is consulted only for `Format::Table`; other formats ignore it.
///
/// `render` accepts anything convertible into [`RenderOptions`], so existing
/// call sites passing a bare `BinaryFormat` keep working unchanged.
pub fn make_writer<'w, W: Write + 'w, R: Into<RenderOptions>>(
    format: Format,
    render: R,
    table_style: TableStyle,
    out: W,
) -> Box<dyn RowWriter + 'w> {
    let render = render.into();
    match format {
        Format::Csv => Box::new(csv::CsvRowWriter::new(out, render)),
        Format::Jsonl => Box::new(jsonl::JsonlRowWriter::new(out, render)),
        Format::Json => Box::new(json::JsonRowWriter::new(out, render)),
        Format::Table => Box::new(table::TableRowWriter::new(out, render, table_style)),
    }
}
