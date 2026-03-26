use std::path::PathBuf;
use std::time::SystemTime;
use std::sync::atomic::{AtomicU64, Ordering};


// these are my global shared atomic counters
// we need these to autoincrement uniquely, but not necessarily in order
// so that we can determine our foreign key restrictions before insert
// and not have to query for each individual one
static FILE_ID_COUNTER: AtomicU64 = AtomicU64::new(1);
static DIR_ID_COUNTER:  AtomicU64 = AtomicU64::new(1);

pub fn next_file_id() -> u64 {
    FILE_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub fn next_dir_id() -> u64 {
    DIR_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}


// core data structs
#[derive(Debug, Clone)]
pub struct FileRecord {
    pub file_id:        u64,
    pub path:           PathBuf,
    pub dir_id:         u64,
    pub inode:          u64,
    pub device:         u64,
    pub size_bytes:     u64,
    pub owner_uid:      u32,
    pub owner_gid:      u32,
    pub atime:          SystemTime,
    pub mtime:          SystemTime,
    pub ctime:          SystemTime,
    pub hardlink_count: u32,
    pub is_symlink:     bool,
}

#[derive(Debug, Clone)]
pub struct DirRecord {
    pub dir_id:        u64,
    pub path:       PathBuf,
    pub parent_id:  Option<u64>,
    pub inode:      u64,
    pub device:     u64,
    pub owner_uid:  u32,
    pub mtime:      SystemTime,
}

#[derive(Debug)]
pub struct CrawlBatch {
    pub dirs:  Vec<DirRecord>,
    pub files: Vec<FileRecord>,
}

#[derive(Debug, Clone)]
pub enum WorkItem {
    FullScan {
        path:   PathBuf, // dir path
        dir_id: u64,           // dir ID, see crawler.rs
        parent_id: Option<u64> // parent dir ID, see crawler.rs
    },
    DeltaScan {
        path:            PathBuf,
        dir_id:          u64,
        parent_id: Option<u64>, // parent dir ID, see crawler.rs
        last_seen_mtime: SystemTime,
    },
    /// Not implemented, maybe in a bright bold future something.
    FileRefresh{
        path:   PathBuf,
        dir_id: u64,
        parent_id: Option<u64> // parent dir ID, see crawler.rs

    },
}

#[derive(Debug)]
pub struct DirResult {
    pub batch:   CrawlBatch,
    pub errors:  Vec<CrawlError>,
    pub subdirs: Vec<WorkItem>,
}

#[derive(Debug)]
pub enum CrawlError {
    PermissionDenied(PathBuf),
    NotFound(PathBuf),
    IoError(PathBuf, std::io::Error),
    TooManySymlinks(PathBuf),
}
