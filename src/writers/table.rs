use crate::types::{DirResult, FileRecord};
use crate::writers::{BufferingWriter, WriterError};

/// Sort order for the file listing in [`TableWriter`].
pub enum SortOrder { Path, Size, Owner }

/// Unit used to display file sizes in [`TableWriter`].
pub enum SizeUnit { Bytes, Kilobytes, Megabytes, Gigabytes }

impl SizeUnit {
    fn convert(&self, bytes: u64) -> f64 {
        match self {
            SizeUnit::Bytes     => bytes as f64,
            SizeUnit::Kilobytes => bytes as f64 / 1_024.0,
            SizeUnit::Megabytes => bytes as f64 / 1_048_576.0,
            SizeUnit::Gigabytes => bytes as f64 / 1_073_741_824.0,
        }
    }

    fn label(&self) -> &'static str {
        match self {
            SizeUnit::Bytes     => "B",
            SizeUnit::Kilobytes => "KB",
            SizeUnit::Megabytes => "MB",
            SizeUnit::Gigabytes => "GB",
        }
    }
}

/// Writes crawl results as a formatted, sorted table to stdout.
///
/// Implements [`BufferingWriter`], accumulating all results in memory before rendering.
/// Column widths, sort order, and size unit are configurable at construction time.
pub struct TableWriter {
    files:      Vec<FileRecord>,

    // processing summary statistics
    total_errors: u64,

    // printing configurables
    sort_order: SortOrder,
    size_unit:  SizeUnit,

    // a compile-time printing table formatting options
    col_path:   usize,
    col_size:   usize,
    col_uid:    usize,
}

impl TableWriter {
    /// Creates a new `TableWriter` with the given sort order and size unit.
    pub fn new(sort_order: SortOrder, size_unit: SizeUnit) -> Self {
        Self {
            files: Vec::new(),
            total_errors: 0,
            sort_order,
            size_unit,
            col_path: 60,
            col_size: 12,
            col_uid:  6,
        }
    }

    /// Formats a single row as a fixed-width string aligned to the configured column widths.
    fn format_row(&self, path: &str, size: f64, uid: u32) -> String {
        format!("{:<width_path$} {:>width_size$.2} {:>width_uid$}",
            path, size, uid,
            width_path = self.col_path,
            width_size = self.col_size,
            width_uid  = self.col_uid,
        )
    }

    /// Prints the table header row and a separator line.
    fn print_header(&self) {
        let size_label = format!("Size ({})", self.size_unit.label());
        println!("{:<width_path$} {:>width_size$} {:>width_uid$}",
            "Path", size_label, "UID",
            width_path = self.col_path,
            width_size = self.col_size,
            width_uid  = self.col_uid,
        );
        println!("{}", "-".repeat(self.col_path + self.col_size + self.col_uid + 2));
    }
}

impl BufferingWriter for TableWriter {
    fn accumulate(&mut self, result: DirResult) {
        self.total_errors += result.errors.len() as u64;
        self.files.extend(result.batch.files);
    }

    fn render(mut self) -> Result<(), WriterError> {
        match self.sort_order {
            SortOrder::Path  => self.files.sort_by(|a, b| a.path.cmp(&b.path)),
            SortOrder::Size  => self.files.sort_by(|a, b| b.size_bytes.cmp(&a.size_bytes)),
            SortOrder::Owner => self.files.sort_by(|a, b| a.owner_uid.cmp(&b.owner_uid)),
        }

        self.print_header();

        for f in &self.files {
            let size = self.size_unit.convert(f.size_bytes);
            let path = f.path.to_string_lossy();
            println!("{}", self.format_row(&path, size, f.owner_uid));
        }

        println!("{}", "-".repeat(self.col_path + self.col_size + self.col_uid + 2));
        println!("files:  {}", self.files.len());
        println!("bytes:  {}", self.files.iter().map(|f| f.size_bytes).sum::<u64>());
        println!("errors: {}", self.total_errors);
        Ok(())
    }
}
