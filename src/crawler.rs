use std::ffi::OsStr;
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use rustix::fs::{openat, statx, AtFlags, Dir, FileType, OFlags, Statx, StatxFlags};
use rustix::io::Errno;

use crate::types::{
    CrawlBatch, CrawlError, DirRecord, DirResult, FileRecord, WorkItem,
};

pub fn process_work_item(path: &Path) -> DirResult {
    let mut batch   = CrawlBatch { dirs: vec![], files: vec![] };
    let mut errors  = vec![];
    let mut subdirs = vec![];

    let dir_fd: OwnedFd = match openat(
        rustix::fs::CWD,
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(e) => {
            errors.push(CrawlError::IoError(path.to_owned(), e.into()));
            return DirResult { batch, errors, subdirs };
        }
    };

    let mut dir_iter = match Dir::read_from(&dir_fd) {
        Ok(iter) => iter,
        Err(e)   => {
            errors.push(CrawlError::IoError(path.to_owned(), e.into()));
            return DirResult { batch, errors, subdirs };
        }
    };

    while let Some(entry) = dir_iter.next() {
        let entry = match entry {
            Ok(e)  => e,
            Err(e) => { errors.push(CrawlError::IoError(path.to_owned(), e.into())); continue; }
        };

        let name = entry.file_name();

        if name.to_bytes() == b"." || name.to_bytes() == b".." {
            continue;
        }

        let name_osstr = OsStr::from_bytes(name.to_bytes());
        let entry_path = path.join(name_osstr);

        let sx: Statx = match statx(
            &dir_fd,
            name,
            AtFlags::NO_AUTOMOUNT | AtFlags::SYMLINK_NOFOLLOW,
            StatxFlags::SIZE
                | StatxFlags::INO
                | StatxFlags::ATIME
                | StatxFlags::MTIME
                | StatxFlags::CTIME
                | StatxFlags::UID
                | StatxFlags::GID
                | StatxFlags::NLINK
                | StatxFlags::TYPE,
        ) {
            Ok(s)             => s,
            Err(Errno::NOENT) => { errors.push(CrawlError::NotFound(entry_path)); continue; }
            Err(e)            => { errors.push(CrawlError::IoError(entry_path, e.into())); continue; }
        };

        let file_type = FileType::from_raw_mode(sx.stx_mode as _);

        match file_type {
            FileType::Directory => {
                subdirs.push(WorkItem::FullScan(entry_path.clone()));
                batch.dirs.push(DirRecord {
                    path:      entry_path,
                    parent_id: None,
                    inode:     sx.stx_ino,
                    device:    device_id(&sx),
                    owner_uid: sx.stx_uid,
                    mtime:     statx_time_to_system_time(&sx.stx_mtime),
                });
            }
            FileType::RegularFile => {
                batch.files.push(FileRecord {
                    path:           entry_path,
                    dir_id:         0,
                    inode:          sx.stx_ino,
                    device:         device_id(&sx),
                    size_bytes:     sx.stx_size,
                    owner_uid:      sx.stx_uid,
                    owner_gid:      sx.stx_gid,
                    atime:          statx_time_to_system_time(&sx.stx_atime),
                    mtime:          statx_time_to_system_time(&sx.stx_mtime),
                    ctime:          statx_time_to_system_time(&sx.stx_ctime),
                    hardlink_count: sx.stx_nlink,
                    is_symlink:     false,
                });
            }
            FileType::Symlink => {}
            _ => {}
        }
    }

    DirResult { batch, errors, subdirs }
}

pub fn device_id(sx: &Statx) -> u64 {
    ((sx.stx_dev_major as u64) << 32) | (sx.stx_dev_minor as u64)
}

pub fn statx_time_to_system_time(t: &rustix::fs::StatxTimestamp) -> std::time::SystemTime {
    std::time::UNIX_EPOCH + std::time::Duration::new(t.tv_sec as u64, t.tv_nsec)
}
