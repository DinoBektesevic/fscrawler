use std::path::PathBuf;
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct FileRecord {
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
    FullScan(PathBuf),
    DeltaScan {
        path:            PathBuf,
        last_seen_mtime: SystemTime,
    },
    FileRefresh(PathBuf),
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
