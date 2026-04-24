use std::io::{BufWriter, IsTerminal, stdout};
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};

use crate::cli::{BinaryFormat, Format};
use crate::output::table::TableStyle;
use crate::output::{RowWriter, make_writer};

pub fn project_arrow_schema(schema: &Schema, projection: Option<&[String]>) -> SchemaRef {
    match projection {
        None => Arc::new(schema.clone()),
        Some(cols) => {
            let fields: Vec<_> = cols
                .iter()
                .map(|n| {
                    schema
                        .field_with_name(n)
                        .expect("projection validated against schema")
                        .clone()
                })
                .collect();
            Arc::new(Schema::new(fields))
        }
    }
}

/// Returns `Ok(())` when two schemas have the same fields (name, type, nullability) in order.
/// Otherwise returns the name of the first mismatched field (or a structural description).
pub fn schemas_match(a: &SchemaRef, b: &SchemaRef) -> std::result::Result<(), String> {
    if a.fields().len() != b.fields().len() {
        return Err(format!(
            "column count differs ({} vs {})",
            a.fields().len(),
            b.fields().len()
        ));
    }
    for (fa, fb) in a.fields().iter().zip(b.fields().iter()) {
        if fa.name() != fb.name()
            || fa.data_type() != fb.data_type()
            || fa.is_nullable() != fb.is_nullable()
        {
            return Err(fa.name().clone());
        }
    }
    Ok(())
}

/// Build a buffered writer over the locked stdout in the format requested by
/// the user. The `TableStyle` is chosen here — and only here — based on
/// whether stdout is actually a terminal, because this is the one call site
/// where we know the destination really is stdout. Other callers (notably
/// tests writing into a `Cursor`) use `make_writer` directly and pin the
/// style explicitly so their output is deterministic.
pub fn make_stdout_writer(format: Format, binary_format: BinaryFormat) -> Box<dyn RowWriter> {
    let table_style = if stdout().is_terminal() {
        TableStyle::Pretty
    } else {
        TableStyle::Plain
    };
    let out = BufWriter::new(stdout().lock());
    make_writer(format, binary_format, table_style, out)
}
