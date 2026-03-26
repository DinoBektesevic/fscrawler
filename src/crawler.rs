use std::ffi::OsStr;
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use rustix::fs::{
    openat,
    statx,
    AtFlags,
    Dir,
    FileType,
    OFlags,
    Statx,
    StatxFlags
};
use rustix::io::Errno;

use crate::types::{
    CrawlBatch,
    CrawlError,
    DirRecord,
    DirResult,
    FileRecord,
    WorkItem,
    next_file_id,
    next_dir_id
};


// The rest is just getting the items in the given path
// figure out if it's a directory, link or a file
// query the files for the data and punt the directories back
// to the worklist
pub fn process_work_item(path: &Path, current_dirid: u64, parent_dirid: Option<u64>) -> DirResult {
    let mut batch   = CrawlBatch { dirs: vec![], files: vec![] };
    let mut errors  = vec![];
    let mut subdirs = vec![];

    // open the dir pointed to by the path. This is a syscall and it costs when
    // we start hitting millions or 10's of millions inodes.
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

    // Then we will add this directory's information to the processed batch
    // pile. This is not a syscall cause the dir is already opened, the cost
    // is low.
    let self_sx = match statx(
        &dir_fd,
        rustix::cstr!("."),
        AtFlags::NO_AUTOMOUNT,
        StatxFlags::INO | StatxFlags::UID | StatxFlags::MTIME,
    ){
        Ok(s) => s,
        Err(e) => { // We don't want to stop exec if we hit an error
            errors.push(CrawlError::IoError(path.to_owned(), e.into()));
            return DirResult { batch, errors, subdirs };
        }
    };

    batch.dirs.push(DirRecord {
        dir_id:    current_dirid,
        path:      path.to_owned(),
        parent_id: parent_dirid,
        inode:     self_sx.stx_ino,
        device:    device_id(&self_sx),
        owner_uid: self_sx.stx_uid,
        mtime:     statx_time_to_system_time(&self_sx.stx_mtime),
    });

    // Then scan the directory items. Grab any subdirectories and push them back
    // onto the work pile (subdirs) and take any files and push their info onto
    // the processed (batch) pile. Symlinks get ignored.
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
                // So we ran into a subdirectory. Push its path back onto the
                // work queue, but figure out its dir ID **HERE**. This way we
                // can track the dir_id for files and subdirs.
                let new_dirid = next_dir_id();
                subdirs.push(WorkItem::FullScan{
                    path:   entry_path.clone(),
                    dir_id: new_dirid,
                    parent_id: Some(current_dirid)
                });
            }
            FileType::RegularFile => {
                batch.files.push(FileRecord {
                    file_id:        next_file_id(),
                    path:           entry_path,
                    dir_id:         current_dirid,
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
