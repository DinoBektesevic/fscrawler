use crate::types::DirResult;
use crate::writers::{StreamingWriter, WriterError};

/// Writes crawl results to stdout.
///
/// Implements [`StreamingWriter`], printing each file's path, size, and owner uid
/// as it arrives. Prints a summary of total files, bytes, and errors on finish.
pub struct StdoutWriter {
    total_files:  u64,
    total_bytes:  u64,
    total_errors: u64,
}

impl StdoutWriter {
    pub fn new() -> Self {
        Self { total_files: 0, total_bytes: 0, total_errors: 0 }
    }
}

impl StreamingWriter for StdoutWriter {
    fn write_batch(&mut self, result: DirResult) -> Result<(), WriterError> {
        self.total_files  += result.batch.files.len() as u64;
        self.total_bytes  += result.batch.files.iter().map(|f| f.size_bytes).sum::<u64>();
        self.total_errors += result.errors.len() as u64;

        for f in &result.batch.files {
            println!("[FILE] {:?} size={} uid={}", f.path, f.size_bytes, f.owner_uid);
        }
        for e in &result.errors {
            eprintln!("[ERROR] {:?}", e);
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), WriterError> {
        println!("--- done ---");
        println!("files:  {}", self.total_files);
        println!("bytes:  {}", self.total_bytes);
        println!("errors: {}", self.total_errors);
        Ok(())
    }
}
