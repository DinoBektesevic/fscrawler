use sqlx::postgres::{PgPoolCopyExt, PgPoolOptions};
use bytes::{BufMut, BytesMut};
use std::time::SystemTime;

use crate::types::{DirResult, FileRecord, DirRecord};
use crate::writers::{StreamingWriter, WriterError};

/// A lot of DBMS is required to set up appropriate tables
/// to work with the current ingestion patterns
/// Annoying, but unlike SQL it can't seemingly be emitted by a low-level sqlx
/// interface?
///
/// CREATE TABLE users (
///     user_id     BIGINT PRIMARY KEY,
///     username    TEXT NOT NULL,
///     uid         INT UNIQUE NOT NULL   -- OS-level UID
/// );
///
/// CREATE TABLE directories (
///     dir_id      BIGINT PRIMARY KEY,
///     path        TEXT NOT NULL,        -- full path, for human readability
///     parent_id   BIGINT REFERENCES directories(dir_id),
///     owner_uid   INT REFERENCES users(uid),
///     mtime       TIMESTAMPTZ,
///     last_seen   TIMESTAMPTZ NOT NULL  -- when crawler last visited
/// );
///
/// CREATE TABLE directory_closure (
///     ancestor_id   BIGINT REFERENCES directories(dir_id),
///     descendant_id BIGINT REFERENCES directories(dir_id),
///     depth         INT NOT NULL,
///     PRIMARY KEY (ancestor_id, descendant_id)
/// );
///
/// CREATE TABLE files (
///     file_id       BIGINT PRIMARY KEY,
///     dir_id        BIGINT REFERENCES directories(dir_id),
///     filename      TEXT NOT NULL,
///     size_bytes    BIGINT NOT NULL,
///     owner_uid     INT REFERENCES users(uid),
///     atime         TIMESTAMPTZ,
///     mtime         TIMESTAMPTZ,
///     ctime         TIMESTAMPTZ,
///     inode         BIGINT,
///     hardlink_count INT,
///     last_seen     TIMESTAMPTZ NOT NULL
/// );
///
/// // Closure tables
/// -- Cumulative size under any directory
///     SELECT SUM(f.size_bytes)
///     FROM directory_closure dc
///     JOIN files f ON f.dir_id = dc.descendant_id
/// WHERE dc.ancestor_id = :target_dir_id;
///
/// -- Per-user total disk usage
///     SELECT u.username, SUM(f.size_bytes) as total_bytes
///     FROM files f
///     JOIN users u ON u.uid = f.owner_uid
///     GROUP BY u.username
///     ORDER BY total_bytes DESC;
///
/// -- Directory tree breakdown per user
///     SELECT d.path, u.username, SUM(f.size_bytes) as subtree_bytes
///     FROM directory_closure dc
///     JOIN directories d ON d.dir_id = dc.ancestor_id
///     JOIN files f ON f.dir_id = dc.descendant_id
///     JOIN users u ON u.uid = f.owner_uid
///     GROUP BY d.path, u.username;
///
/// CREATE TABLE directory_stats (
///     dir_id           BIGINT PRIMARY KEY REFERENCES directories(dir_id),
///     direct_bytes     BIGINT,   -- files directly in this dir
///         subtree_bytes    BIGINT,   -- cumulative including all descendants
///         file_count       BIGINT,
///     subtree_count    BIGINT,
///     last_computed    TIMESTAMPTZ
/// );

const PG_COPY_SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";
const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800 * 1_000_000;

fn pg_binary_header() -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_slice(PG_COPY_SIGNATURE);
    buf.put_i32(0);
    buf.put_i32(0);
    buf
}

fn pg_binary_trailer(buf: &mut BytesMut) {
    buf.put_i16(-1);
}

fn put_field_bytes(buf: &mut BytesMut, data: &[u8]) {
    buf.put_i32(data.len() as i32);
    buf.put_slice(data);
}

fn put_field_i32(buf: &mut BytesMut, val: i32) {
    buf.put_i32(4);
    buf.put_i32(val);
}

fn put_field_i64(buf: &mut BytesMut, val: i64) {
    buf.put_i32(8);
    buf.put_i64(val);
}

fn put_field_null(buf: &mut BytesMut) {
    buf.put_i32(-1);
}

fn system_time_to_pg_timestamp(t: SystemTime) -> i64 {
    let unix_micros = t
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64;
    unix_micros - PG_EPOCH_OFFSET_MICROS
}

fn now_pg_timestamp() -> i64 {
    system_time_to_pg_timestamp(SystemTime::now())
}

// files schema:
// file_id, dir_id, filename, size_bytes, owner_uid,
// atime, mtime, ctime, inode, hardlink_count, last_seen
// = 11 fields
fn build_file_copy_buffer(files: &[FileRecord]) -> BytesMut {
    // Each binary buffer in a PSQL COPY is constructed as:
    // field length (4 bytes) + data (N bytes).
    // Each row starts with the field count (2 bytes). To save on allocation,
    // since we know the widths of most of our fields, we can make a reasonable
    // guess on the buffer capacity:
    // field count (2) + field_length + file_id (8) + field_length + dir_id (8)
    // + field_length + filename (variable) + ... + field_length + time +...
    // with the field length prefix it becomes:
    // 2 + (4+8) + (4+8) + (4+variable) + ... + (4*12) + ...
    // Say we think on average the path will fit into 20 characters.
    let mut buf = pg_binary_header();
    buf.reserve(files.len() * 164);

    let now = now_pg_timestamp();

    for file in files {
        buf.put_i16(11);

        put_field_i64(&mut buf, file.file_id as i64);         // file_id
        put_field_i64(&mut buf, file.dir_id as i64);          // dir_id

        // filename: just the last component, not the full path
        let filename = file.path
            .file_name()
            .map(|n| n.to_string_lossy())
            .unwrap_or_default();
        put_field_bytes(&mut buf, filename.as_bytes());        // filename

        put_field_i64(&mut buf, file.size_bytes as i64);      // size_bytes
        put_field_i64(&mut buf, file.owner_uid as i64);       // owner_uid
        put_field_i64(&mut buf, system_time_to_pg_timestamp(file.atime)); // atime
        put_field_i64(&mut buf, system_time_to_pg_timestamp(file.mtime)); // mtime
        put_field_i64(&mut buf, system_time_to_pg_timestamp(file.ctime)); // ctime
        put_field_i64(&mut buf, file.inode as i64);           // inode
        put_field_i32(&mut buf, file.hardlink_count as i32);  // hardlink_count
        put_field_i64(&mut buf, now);                         // last_seen
    }

    pg_binary_trailer(&mut buf);
    buf
}

// directories schema:
// dir_id, path, parent_id, owner_uid, mtime, last_seen
// = 6 fields
fn build_dir_copy_buffer(dirs: &[DirRecord]) -> BytesMut {
    // same as above. We know most widths, we don't know path length in bytes,
    // so guess 20 characters say, and guess to minimize allocations. The buffer
    // will grow if we push more onto it.
    let mut buf = pg_binary_header();
    buf.reserve(dirs.len() * 128);

    let now = now_pg_timestamp();

    for dir in dirs {
        buf.put_i16(6);

        put_field_i64(&mut buf, dir.dir_id as i64);           // dir_id

        let path_bytes = dir.path.to_string_lossy();
        put_field_bytes(&mut buf, path_bytes.as_bytes());     // path

        match dir.parent_id {
            Some(id) => put_field_i64(&mut buf, id as i64),  // parent_id
            None     => put_field_null(&mut buf),
        }

        put_field_i64(&mut buf, dir.owner_uid as i64);        // owner_uid
        put_field_i64(&mut buf, system_time_to_pg_timestamp(dir.mtime)); // mtime
        put_field_i64(&mut buf, now);                         // last_seen
    }

    pg_binary_trailer(&mut buf);
    buf
}

pub struct PostgresWriter {
    file_buffer: Vec<FileRecord>,
    dir_buffer:  Vec<DirRecord>,
    batch_size:  usize,
    rt:          tokio::runtime::Runtime,
    pool:        sqlx::PgPool,
}

impl PostgresWriter {
    pub fn new(database_url: String) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime");

        let pool = rt.block_on(
            PgPoolOptions::new()
                .max_connections(2)  // COPY uses one connection at a time
                .after_connect(|conn, _meta| Box::pin(async move {
                    sqlx::query("SET search_path TO crawler")
                        .execute(conn)
                        .await?;
                    Ok(())
                }))
                .connect(&database_url)
        ).unwrap_or_else(|e| {
            eprintln!("PostgresWriter failed to connect: {}", e);
            std::process::exit(1);
        });

        Self {
            file_buffer: Vec::new(),
            dir_buffer:  Vec::new(),
            batch_size:  10_000,
            rt,
            pool,
        }
    }

    fn flush(&mut self) -> Result<(), WriterError> {
        if self.file_buffer.is_empty() && self.dir_buffer.is_empty() {
            return Ok(());
        }

        let files = std::mem::take(&mut self.file_buffer);
        let dirs  = std::mem::take(&mut self.dir_buffer);

        let result = self.rt.block_on(async {
            Self::copy_dirs(&self.pool, &dirs).await?;   // dirs first — files FK to dirs
            Self::copy_files(&self.pool, &files).await?;
            Ok::<(), WriterError>(())
        });

        if let Err(ref e) = result {
            eprintln!("[flush error] {}", e);
        }

        result
    }

    async fn copy_files(pool: &sqlx::PgPool, files: &[FileRecord]) -> Result<(), WriterError> {
        let mut copy = pool
            .copy_in_raw(
                "COPY files (
                    file_id, dir_id, filename, size_bytes, owner_uid,
                    atime, mtime, ctime, inode, hardlink_count, last_seen
                ) FROM STDIN WITH (FORMAT binary)"
            )
            .await?;
        copy.send(build_file_copy_buffer(files)).await
            .map_err(|e| WriterError::Database(e.to_string()))?;
        copy.finish().await?;
        Ok(())
    }

    async fn copy_dirs(pool: &sqlx::PgPool, dirs: &[DirRecord]) -> Result<(), WriterError> {
        let mut copy = pool
            .copy_in_raw(
                "COPY directories (
                    dir_id, path, parent_id, owner_uid, mtime, last_seen
                ) FROM STDIN WITH (FORMAT binary)"
            )
            .await?;
        copy.send(build_dir_copy_buffer(dirs)).await
            .map_err(|e| WriterError::Database(e.to_string()))?;
        copy.finish().await?;
        Ok(())
    }
}

impl StreamingWriter for PostgresWriter {
    fn write_batch(&mut self, result: DirResult) -> Result<(), WriterError> {
        self.file_buffer.extend(result.batch.files);
        self.dir_buffer.extend(result.batch.dirs);

        if self.file_buffer.len() >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), WriterError> {
        self.flush()
    }
}
