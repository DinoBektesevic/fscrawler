use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use chrono::{DateTime, Utc};
use serde::Serialize;


// Global counters shared across all worker threads. Atomically incremented so
// every record gets a unique ID without querying the database. Seeded from the
// DB max at startup so re-runs do not produce primary key conflicts.
static FILE_ID_COUNTER: AtomicU64 = AtomicU64::new(1);
static DIR_ID_COUNTER:  AtomicU64 = AtomicU64::new(1);

/// Seeds the file ID counter from the highest ID already present in the database.
/// Call at startup before crawling to avoid primary key conflicts on re-run.
pub fn seed_file_id(max_seen: u64) {
    FILE_ID_COUNTER.store(max_seen + 1, Ordering::Relaxed);
}

/// Seeds the directory ID counter from the highest ID already present in the database.
/// Call at startup before crawling to avoid primary key conflicts on re-run.
pub fn seed_dir_id(max_seen: u64) {
    DIR_ID_COUNTER.store(max_seen + 1, Ordering::Relaxed);
}

/// Returns the next unique file ID, incrementing the global counter atomically.
///
/// IDs are unique within a single process. Call [`seed_file_id`] at startup
/// to avoid conflicts with IDs already present in the database.
pub fn next_file_id() -> u64 {
    FILE_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Returns the next unique directory ID, incrementing the global counter atomically.
///
/// IDs are unique within a single process. Call [`seed_dir_id`] at startup
/// to avoid conflicts with IDs already present in the database.
pub fn next_dir_id() -> u64 {
    DIR_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

// core data structs

/// Metadata record for a single regular file, populated by `statx(2)`.
/// Stored in [`CrawlBatch`] and written to the `files` table.
#[derive(Debug, Clone, Serialize)]
pub struct FileRecord {
    pub file_id:        u64,
    pub path:           PathBuf,
    pub dir_id:         u64,
    pub inode:          u64,
    pub device:         u64,
    pub size_bytes:     u64,
    pub owner_uid:      u32,
    pub owner_gid:      u32,
    pub atime:          DateTime<Utc>,
    pub mtime:          DateTime<Utc>,
    pub ctime:          DateTime<Utc>,
    pub hardlink_count: u32,
    pub is_symlink:     bool,
}

/// Metadata record for a single directory, populated by `statx(2)`.
/// Stored in [`CrawlBatch`] and written to the `directories` table.
#[derive(Debug, Clone, Serialize)]
pub struct DirRecord {
    pub dir_id:    u64,
    pub path:      PathBuf,
    pub parent_id: Option<u64>,
    pub inode:     u64,
    pub device:    u64,
    pub owner_uid: u32,
    pub mtime:     DateTime<Utc>,
}

/// A batch of directory and file records produced by processing one [`WorkItem`].
/// Sent from a worker thread to the writer thread via the result channel.
#[derive(Debug)]
pub struct CrawlBatch {
    pub dirs:  Vec<DirRecord>,
    pub files: Vec<FileRecord>,
}

/// A unit of work dispatched to a worker thread, describing a directory to scan.
#[derive(Debug, Clone)]
pub enum WorkItem {
    /// Full directory scan: stat every entry unconditionally.
    FullScan {
        path:      PathBuf,     // dir path
        dir_id:    u64,         // dir ID, see crawler.rs
        parent_id: Option<u64>, // parent dir ID, see crawler.rs
    },
    /// Incremental scan: skip entries whose mtime predates `last_seen_mtime`.
    DeltaScan {
        path:            PathBuf,
        dir_id:          u64,
        parent_id:       Option<u64>, // parent dir ID, see crawler.rs
        last_seen_mtime: DateTime<Utc>,
    },
    /// Good news, everyone! FileRefresh will be implemented in the bright bold distant future.
    FileRefresh {
        path:      PathBuf,
        dir_id:    u64,
        parent_id: Option<u64>, // parent dir ID, see crawler.rs
    },
}

/// Processing result for a single [`WorkItem`]. Contains a batch of scanned records
/// ready to be written, a collection of errors that were silenced so the crawl can
/// continue, and subdirectories queued for further scanning.
#[derive(Debug)]
pub struct DirResult {
    pub batch:   CrawlBatch,
    pub errors:  Vec<CrawlError>,
    pub subdirs: Vec<WorkItem>,
}

/// Errors that can occur while scanning a directory entry.
#[derive(Debug)]
pub enum CrawlError {
    /// The process lacks permission to read or stat this path.
    PermissionDenied(PathBuf),
    /// The entry disappeared between `getdents` and `statx` (TOCTOU race).
    NotFound(PathBuf),
    /// An unexpected IO error occurred. The [`PathBuf`] is the affected entry,
    /// the [`std::io::Error`] is the underlying OS error.
    IoError(PathBuf, std::io::Error),
    /// Symlink chain for this path exceeded the OS limit (ELOOP).
    TooManySymlinks(PathBuf),
}
