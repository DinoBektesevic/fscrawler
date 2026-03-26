pub mod stdout;
pub mod postgres;
pub mod table;

use crate::types::DirResult;

// --- error type ---

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

pub trait StreamingWriter: Send + 'static {
    fn write_batch(&mut self, result: DirResult) -> Result<(), WriterError>;
    fn finish(&mut self) -> Result<(), WriterError>;
}

pub trait BufferingWriter: Send + 'static {
    fn accumulate(&mut self, result: DirResult);
    fn render(self) -> Result<(), WriterError>;
}

// --- thread runners ---

//pub fn streaming_writer_thread<W: StreamingWriter>(
//    result_rx: std::sync::mpsc::Receiver<DirResult>,
//    mut writer: W,
//) -> Result<(), WriterError> {
//    while let Ok(result) = result_rx.recv() {
//        writer.write_batch(result)?;
//    }
//    writer.finish()
//}
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


pub fn buffering_writer_thread<W: BufferingWriter>(
    result_rx: std::sync::mpsc::Receiver<DirResult>,
    mut writer: W,
) -> Result<(), WriterError> {
    while let Ok(result) = result_rx.recv() {
        writer.accumulate(result);
    }
    writer.render()

}
