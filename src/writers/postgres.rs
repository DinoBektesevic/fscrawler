use sqlx::postgres::PgPoolCopyExt;
use bytes::{BufMut, BytesMut};

use sqlx::postgres::PgCopyIn;
use sqlx::pool::PoolConnection;
use sqlx::Postgres;

use crate::types::{DirResult, FileRecord, DirRecord};
use crate::writers::{StreamingWriter, WriterError};



const PG_COPY_SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";
const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800 * 1_000_000;

fn pg_binary_header() -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_slice(PG_COPY_SIGNATURE);
    buf.put_i32(0); // flags
    buf.put_i32(0); // header extension length
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

fn put_field_bool(buf: &mut BytesMut, val: bool) {
    buf.put_i32(1);
    buf.put_u8(val as u8);
}

fn put_field_null(buf: &mut BytesMut) {
    buf.put_i32(-1);
}

fn system_time_to_pg_timestamp(t: std::time::SystemTime) -> i64 {
    let unix_micros = t
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64;
    unix_micros - PG_EPOCH_OFFSET_MICROS
}

fn build_binary_copy_buffer(files: &[FileRecord]) -> BytesMut {
    let mut buf = pg_binary_header();
    buf.reserve(files.len() * 128);

    for file in files {
        buf.put_i16(11); // number of fields — must match COPY column list

        let path_bytes = file.path.to_string_lossy();
        put_field_bytes(&mut buf, path_bytes.as_bytes());
        put_field_i64(&mut buf, file.inode as i64);
        put_field_i64(&mut buf, file.device as i64);
        put_field_i64(&mut buf, file.size_bytes as i64);
        put_field_i32(&mut buf, file.owner_uid as i32);
        put_field_i32(&mut buf, file.owner_gid as i32);
        put_field_i64(&mut buf, system_time_to_pg_timestamp(file.atime));
        put_field_i64(&mut buf, system_time_to_pg_timestamp(file.mtime));
        put_field_i64(&mut buf, system_time_to_pg_timestamp(file.ctime));
        put_field_i32(&mut buf, file.hardlink_count as i32);
        put_field_bool(&mut buf, file.is_symlink);
    }

    pg_binary_trailer(&mut buf);
    buf
}

fn build_dir_copy_buffer(dirs: &[DirRecord]) -> BytesMut {
    let mut buf = pg_binary_header();
    buf.reserve(dirs.len() * 64);

    for dir in dirs {
        buf.put_i16(6); // number of fields — must match COPY column list

        let path_bytes = dir.path.to_string_lossy();
        put_field_bytes(&mut buf, path_bytes.as_bytes());

        match dir.parent_id {
            Some(id) => put_field_i64(&mut buf, id as i64),
            None     => put_field_null(&mut buf),
        }

        put_field_i64(&mut buf, dir.inode as i64);
        put_field_i64(&mut buf, dir.device as i64);
        put_field_i32(&mut buf, dir.owner_uid as i32);
        put_field_i64(&mut buf, system_time_to_pg_timestamp(dir.mtime));
    }

    pg_binary_trailer(&mut buf);
    buf
}

pub struct PostgresWriter {
    #[expect(unused)]
    database_url: String,
    file_buffer:  Vec<FileRecord>,
    dir_buffer:   Vec<DirRecord>,
    batch_size:   usize,
    rt:           tokio::runtime::Runtime,
    pool:         sqlx::PgPool,
}

impl PostgresWriter {
    pub fn new(database_url: String) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime");

        let pool = rt.block_on(sqlx::PgPool::connect(&database_url))
            .expect("failed to connect to postgres");

        Self {
            database_url,
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

        self.rt.block_on(async {
            Self::copy_files(&self.pool, &files).await?;
            Self::copy_dirs(&self.pool, &dirs).await?;
            Ok::<(), WriterError>(())
        })
    }

    async fn copy_files(pool: &sqlx::PgPool, files: &[FileRecord]) -> Result<(), WriterError> {
        let mut copy: PgCopyIn<PoolConnection<Postgres>> = pool
            .copy_in_raw("COPY files (path, inode, device, size_bytes, owner_uid, owner_gid, atime, mtime, ctime, hardlink_count, is_symlink) FROM STDIN WITH (FORMAT binary)")
            .await?;
        copy.send(build_binary_copy_buffer(files)).await
            .map_err(|e| WriterError::Database(e.to_string()))?;
        copy.finish().await?;
        Ok(())
    }

    async fn copy_dirs(pool: &sqlx::PgPool, dirs: &[DirRecord]) -> Result<(), WriterError> {
        let mut copy = pool
            .copy_in_raw("COPY directories (path, parent_id, inode, device, owner_uid, mtime) FROM STDIN WITH (FORMAT binary)")
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
