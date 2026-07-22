//! Arrow IPC streaming output.
//!
//! Unlike the text writers (`csv`/`jsonl`/`json`/`table`) this path bypasses
//! `value` entirely: each `RecordBatch` is handed straight to
//! [`arrow::ipc::writer::StreamWriter`], which writes the schema once and then
//! each batch, so output is lossless and fully streaming at constant memory.
//! [`RenderOptions`](crate::output::RenderOptions) therefore has nothing to act
//! on and is not threaded here — the command layer rejects the value-rendering
//! flags when `--format ipc` is selected (see [`crate::commands`]).

use std::io::Write;

use arrow::ipc::writer::StreamWriter;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::Result;
use crate::error::Error;
use crate::output::RowWriter;

/// The TTY guard predicate. Arrow IPC is a binary format; dumping it into a
/// terminal is never useful and only garbles the screen, so refuse it — the
/// same stance `git diff` takes about paging binary output. Factored out as a
/// pure function of "is stdout a terminal?" so it is unit-tested without a real
/// TTY; the single production caller passes `std::io::stdout().is_terminal()`.
pub fn guard_not_terminal(stdout_is_terminal: bool) -> Result<()> {
    if stdout_is_terminal {
        Err(Error::IpcToTerminal)
    } else {
        Ok(())
    }
}

/// Streams `RecordBatch`es out as an Arrow IPC stream. The `StreamWriter` needs
/// the schema up front, but the [`RowWriter`] contract only provides it in
/// `start`, so construction is deferred: `writer` holds the raw sink until
/// `start` consumes it into `stream`.
pub struct IpcRowWriter<W: Write> {
    writer: Option<W>,
    stream: Option<StreamWriter<W>>,
}

impl<W: Write> IpcRowWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer: Some(writer),
            stream: None,
        }
    }
}

impl<W: Write> RowWriter for IpcRowWriter<W> {
    fn start(&mut self, schema: &SchemaRef) -> Result<()> {
        // `try_new` writes the schema message immediately, so the stream header
        // lands before any batch — including for empty results, which still
        // produce a valid (schema + end-of-stream) IPC stream.
        let writer = self
            .writer
            .take()
            .expect("IpcRowWriter::start called more than once");
        self.stream = Some(StreamWriter::try_new(writer, schema.as_ref())?);
        Ok(())
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.stream
            .as_mut()
            .expect("write_batch before start")
            .write(batch)?;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if let Some(mut stream) = self.stream.take() {
            // Writes the end-of-stream marker, then flush the underlying sink so
            // a buffered stdout is fully drained before we return.
            stream.finish()?;
            stream.get_mut().flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::ipc::reader::StreamReader;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn batch(ids: &[i32]) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap()
    }

    #[test]
    fn guard_refuses_terminal_but_allows_pipe() {
        assert!(guard_not_terminal(false).is_ok());
        assert!(matches!(
            guard_not_terminal(true),
            Err(Error::IpcToTerminal)
        ));
    }

    #[test]
    fn round_trips_batches_and_schema() {
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut w = IpcRowWriter::new(&mut buf);
            w.start(&schema()).unwrap();
            w.write_batch(&batch(&[1, 2, 3])).unwrap();
            w.write_batch(&batch(&[4, 5])).unwrap();
            w.finish().unwrap();
        }

        let reader = StreamReader::try_new(buf.as_slice(), None).unwrap();
        assert_eq!(reader.schema(), schema());
        let read: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(read, vec![batch(&[1, 2, 3]), batch(&[4, 5])]);
    }

    #[test]
    fn empty_result_is_a_valid_readable_stream() {
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut w = IpcRowWriter::new(&mut buf);
            w.start(&schema()).unwrap();
            // No batches written at all.
            w.finish().unwrap();
        }

        let reader = StreamReader::try_new(buf.as_slice(), None).unwrap();
        assert_eq!(reader.schema(), schema());
        let read: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        assert!(read.is_empty());
    }
}
