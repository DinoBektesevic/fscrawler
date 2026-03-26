pub mod stdout;
pub mod postgres;
pub mod table;

use crate::types::DirResult;

// --- error type ---

/// Errors returned by writer backends.
#[derive(Debug)]
pub enum WriterError {
    Io(std::io::Error),
    Database(String),
    Encoding(String),
}

impl std::fmt::Display for WriterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriterError::Io(e)         => write!(f, "IO error: {}", e),
            WriterError::Database(msg) => write!(f, "Database error: {}", msg),
            WriterError::Encoding(msg) => write!(f, "Encoding error: {}", msg),
        }
    }
}

impl From<std::io::Error> for WriterError {
    fn from(e: std::io::Error) -> Self {
        WriterError::Io(e)
    }
}

impl From<sqlx::Error> for WriterError {
    fn from(e: sqlx::Error) -> Self {
        WriterError::Database(e.to_string())
    }
}

// --- traits ---

/// Writes [`DirResult`] batches to the destination as they arrive from worker threads.
///
/// Streaming writers process results incrementally as each worker thread completes a
/// directory. They are low latency and memory efficient, but output cannot be globally
/// formatted or sorted (e.g. see [`crate::writers::table::TableWriter`]).
pub trait StreamingWriter: Send + 'static {
    /// Process one batch of crawl results, writing them to the destination.
    fn write_batch(&mut self, result: DirResult) -> Result<(), WriterError>;

    /// Called once after all batches have been delivered. Flush any remaining
    /// buffered data and release resources.
    fn finish(&mut self) -> Result<(), WriterError>;
}

/// Accumulates all [`DirResult`] batches in memory, then renders them in one pass.
///
/// Buffering writers collect results from all worker threads before producing output.
/// They enable full control over output formatting (sorting, aligning, etc.) but
/// require all results to be held in memory.
pub trait BufferingWriter: Send + 'static {
    /// Accumulate one batch of crawl results into internal storage.
    fn accumulate(&mut self, result: DirResult);

    /// Called once all batches have been delivered. Consume the writer,
    /// process the accumulated results, and produce output.
    fn render(self) -> Result<(), WriterError>;
}

// --- thread runners ---


/// Runs a [`StreamingWriter`] on the receiving end of the result channel.
///
/// Drains [`DirResult`]s from `result_rx` and forwards each to [`StreamingWriter::write_batch`].
/// If a write error occurs, it is recorded and the channel continues to be drained so
/// worker threads are not blocked. The first error is returned at the end; if none
/// occurred, [`StreamingWriter::finish`] is called.
pub fn streaming_writer_thread<W: StreamingWriter>(
    result_rx: std::sync::mpsc::Receiver<DirResult>,
    mut writer: W,
) -> Result<(), WriterError> {
    let mut write_error: Option<WriterError> = None;

    while let Ok(result) = result_rx.recv() {
        if write_error.is_none() {
            if let Err(e) = writer.write_batch(result) {
                eprintln!("[writer] error during write_batch: {}", e);
                write_error = Some(e);
                // don't return — keep draining so workers don't panic
            }
        }
        // if we already have an error, just drain and discard
    }

    match write_error {
        Some(e) => Err(e),
        None    => writer.finish(),
    }
}


/// Runs a [`BufferingWriter`] on the receiving end of the result channel.
///
/// Drains all [`DirResult`]s from `result_rx` into the writer via [`BufferingWriter::accumulate`],
/// then calls [`BufferingWriter::render`] once the channel is closed.
pub fn buffering_writer_thread<W: BufferingWriter>(
    result_rx: std::sync::mpsc::Receiver<DirResult>,
    mut writer: W,
) -> Result<(), WriterError> {
    while let Ok(result) = result_rx.recv() {
        writer.accumulate(result);
    }
    writer.render()

}
