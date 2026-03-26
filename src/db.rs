use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use crate::writers::WriterError;


/// The database expects the crawler_manager_role to
/// exists. Briefly, configure the database as follows:
///
/// -- ironically roles are users in PSQL, but default to NOLOGIN whereas
/// -- users default to login. So we want one nologin role and one login user
/// -- per the nologin role, so that if you need one more later on you have
/// -- somewhere to assign them to.
/// CREATE ROLE crawler_admin_role;
/// CREATE ROLE crawler_manager_role;
/// CREATE ROLE crawler_readonly_role NOLOGIN;
///
/// CREATE USER crawler_admin   WITH PASSWORD 'pass' IN ROLE crawler_admin_role;
/// CREATE USER crawler_manager WITH PASSWORD 'word' IN ROLE crawler_manager_role;
/// CREATE USER crawler_readonly in ROLE crawler_readonly_role;
///
/// CREATE DATABASE crawler_db OWNER crawler_admin;
///
/// Manager can connect but cannot drop the database, admin is a person
///
/// GRANT CONNECT ON DATABASE crawler_db TO crawler_manager;
/// GRANT CONNECT ON DATABASE crawler_db TO crawler_readonly;
///
/// This mostly sorts out all the permissions and roles.
/// So now we handle the schemas and permissions:
///
/// CREATE SCHEMA IF NOT EXISTS crawler AUTHORIZATION crawler_admin_role
///
/// Set up manager privileges on the schema. Manager can do everything
/// except drop the actual schema and database.
///
/// GRANT USAGE ON SCHEMA crawler TO crawler_manager;
/// GRANT USAGE ON SCHEMA crawler TO crawler_readonly;
/// GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
///     ON ALL TABLES IN SCHEMA crawler
///     TO crawler_manager;
///
/// Readonly role is self-evident.
///
/// GRANT SELECT
///     ON ALL TABLES IN SCHEMA crawler
///     TO crawler_readonly;
///
/// We want to auto-add these same permissions to any tables that are added to
/// the schema in the future by default.
///
/// ALTER DEFAULT PRIVILEGES IN SCHEMA crawler
///     GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
///     ON TABLES TO crawler_manager;
///
/// ALTER DEFAULT PRIVILEGES IN SCHEMA crawler
///     GRANT SELECT
///     ON TABLES TO crawler_readonly;
///
/// -- needed for BIGSERIAL inserts
/// GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA crawler TO crawler_manager;
///
/// ALTER DEFAULT PRIVILEGES IN SCHEMA crawler
///     GRANT USAGE, SELECT ON SEQUENCES TO crawler_manager;
///
/// These are provided as SQL executable scripts in the fscrawler/sql directory.
/// Run them in order as a user with sufficient privileges using a client.
/// For example for PSQL:
///
/// psql postgresql_url -f fscrawler/sql/01_roles_database.sql
/// psql postgresql_url_as_crawler_admin -f fscrawler/sql/02_schema_permissions.sql
///
/// or if already inside an interactive PSQL session
///
/// \i fscrawler/sql/01_roles_database.sql
/// \i fscrawler/sql/02_schema_permissions.sql
///

/// Opens a connection pool to the PostgreSQL database at `database_url`.
///
/// Sets the `search_path` to the `crawler` schema on every new connection,
/// so table names do not need to be schema-qualified in queries.
pub async fn async_connect(database_url: &str) -> Result<PgPool, WriterError> {
    let pool = PgPoolOptions::new()
        .after_connect(
            |conn, _meta| Box::pin(async move {
                sqlx::query("SET search_path to crawler")
                    .execute(conn)
                    .await?;
                Ok(())
            }))
        .connect(database_url)
        .await
        .map_err(|e| WriterError::Database(e.to_string()))?;

    Ok(pool)
}

/// Synchronous wrapper around [`async_connect`] for use in non-async contexts.
///
/// Builds a single-threaded Tokio runtime, runs [`async_connect`] to completion,
/// and returns the pool. The runtime is dropped after the call.
pub fn sync_connect(database_url: &str) -> Result<PgPool, WriterError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async_connect(database_url))
}

/// Creates all crawler tables and indices in the `crawler` schema.
///
/// Foreign key constraints are intentionally omitted here and added post-crawl
/// by [`post_crawl`] to allow fast constraint-free bulk COPY ingestion.
///
/// ```sql
/// CREATE TABLE users (
///     uid      BIGINT PRIMARY KEY,
///     username TEXT,
///     gid      BIGINT
/// );
/// CREATE TABLE directories (
///     dir_id    BIGINT PRIMARY KEY,
///     path      TEXT NOT NULL,
///     parent_id BIGINT,
///     owner_uid BIGINT,
///     mtime     TIMESTAMPTZ,
///     last_seen TIMESTAMPTZ NOT NULL
/// );
/// CREATE TABLE files (
///     file_id        BIGINT PRIMARY KEY,
///     dir_id         BIGINT,
///     filename       TEXT NOT NULL,
///     size_bytes     BIGINT NOT NULL,
///     owner_uid      BIGINT,
///     atime          TIMESTAMPTZ,
///     mtime          TIMESTAMPTZ,
///     ctime          TIMESTAMPTZ,
///     inode          BIGINT,
///     hardlink_count INT,
///     last_seen      TIMESTAMPTZ NOT NULL
/// );
/// CREATE TABLE directory_stats (
///     dir_id        BIGINT PRIMARY KEY,
///     direct_bytes  BIGINT,
///     subtree_bytes BIGINT,
///     file_count    BIGINT,
///     subtree_count BIGINT,
///     last_computed TIMESTAMPTZ NOT NULL
/// );
/// CREATE TABLE directory_closure (
///     ancestor_id   BIGINT,
///     descendant_id BIGINT,
///     depth         INT NOT NULL,
///     PRIMARY KEY (ancestor_id, descendant_id)
/// );
/// ```
pub async fn create_tables(pool: &PgPool) -> Result<(), WriterError>{
    // set search path for this connection so we don't have to
    // qualify every table name with crawler.tablename
    sqlx::query("SET search_path TO crawler")
        .execute(pool)
        .await
        .map_err(|e| WriterError::Database(e.to_string()))?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS users (
             uid      BIGINT PRIMARY KEY,
             username TEXT,
             gid      BIGINT
         )"
    )
        .execute(pool)
        .await
        .map_err(|e| WriterError::Database(e.to_string()))?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS directories (
            dir_id      BIGINT PRIMARY KEY,
            path        TEXT NOT NULL,
            parent_id   BIGINT,          -- the FK REFERENCES directories(dir_id) is added post-crawl
            owner_uid   BIGINT,          -- The FK REFERENCES users(uid) is added post-crawl
            mtime       TIMESTAMPTZ,
            last_seen   TIMESTAMPTZ NOT NULL
        )"
    )
        .execute(pool)
        .await
        .map_err(|e| WriterError::Database(e.to_string()))?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS files(
            file_id       BIGINT PRIMARY KEY,
            dir_id        BIGINT,         -- the REFERENCES directories(dir_id) is in post-crawl
            filename      TEXT NOT NULL,
            size_bytes    BIGINT NOT NULL,
            owner_uid     BIGINT,         -- the FK REFERENCES dir_id is added post-crawl
            atime         TIMESTAMPTZ,
            mtime         TIMESTAMPTZ,
            ctime         TIMESTAMPTZ,
            inode         BIGINT,
            hardlink_count INT,
            last_seen     TIMESTAMPTZ NOT NULL
       )"
    )
        .execute(pool)
        .await
        .map_err(|e| WriterError::Database(e.to_string()))?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS directory_stats(
            dir_id        BIGINT PRIMARY KEY, -- the REFERENCES directories(dir_id) is in post-crawl,
            direct_bytes  BIGINT,
            subtree_bytes BIGINT,
            file_count    BIGINT,
            subtree_count BIGINT,
            last_computed TIMESTAMPTZ NOT NULL
       )"
    )
        .execute(pool)
        .await
        .map_err(|e| WriterError::Database(e.to_string()))?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS directory_closure(
            ancestor_id   BIGINT REFERENCES directories(dir_id),
            descendant_id BIGINT REFERENCES directories(dir_id),
            depth         INT NOT NULL,
            PRIMARY KEY (ancestor_id, descendant_id)
        )"
    )
        .execute(pool)
        .await
        .map_err(|e| WriterError::Database(e.to_string()))?;

    let indices: [&'static str; 6] = [
        "CREATE INDEX IF NOT EXISTS idx_files_owner    ON files(owner_uid)",
        "CREATE INDEX IF NOT EXISTS idx_files_dir      ON files(dir_id)",
        "CREATE INDEX IF NOT EXISTS idx_files_mtime    ON files(mtime)",
        "CREATE INDEX IF NOT EXISTS idx_files_atime    ON files(atime)",
        "CREATE INDEX IF NOT EXISTS idx_closure_anc    ON directory_closure(ancestor_id)",
        "CREATE INDEX IF NOT EXISTS idx_closure_desc   ON directory_closure(descendant_id)"
    ];
    for &query in indices.iter(){
        sqlx::query(query)
            .execute(pool)
            .await
            .map_err(|e| WriterError::Database(e.to_string()))?;
    }

    Ok(())
}

/// Synchronous wrapper: connects to `database_url` and runs [`create_tables`].
pub fn run_create(database_url: &str) -> Result<PgPool, WriterError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async {
        let pool = async_connect(database_url).await?;
        create_tables(&pool).await?;
        Ok(pool)
    })
}

/// Drops all foreign key constraints and truncates all crawler tables.
///
/// Constraints are dropped with `IF EXISTS` so this is safe to call on a freshly
/// created schema with no constraints yet. Typically followed by [`create_tables`]
/// to restore the schema to a clean state — see [`run_clear`].
///
/// Note: `IF NOT EXISTS` in [`create_tables`] means schema changes are not applied
/// to existing tables. If the schema has been updated, tables must be dropped manually
/// before running [`run_clear`].
pub async fn clear_tables(pool: &PgPool) -> Result<(), WriterError> {
    for stmt in [
        "ALTER TABLE files           DROP CONSTRAINT IF EXISTS fk_files_owner",
        "ALTER TABLE files           DROP CONSTRAINT IF EXISTS fk_files_dir",
        "ALTER TABLE directories     DROP CONSTRAINT IF EXISTS fk_directories_owner",
        "ALTER TABLE directories     DROP CONSTRAINT IF EXISTS fk_directories_parent",
        "ALTER TABLE directory_stats DROP CONSTRAINT IF EXISTS fk_dir_stats_owner",
    ] {
        sqlx::query(stmt)
            .execute(pool)
            .await
            .map_err(|e| WriterError::Database(e.to_string()))?;
    }
    sqlx::query("TRUNCATE files, directory_closure, directories, users, directory_stats")
        .execute(pool)
        .await
        .map_err(|e| WriterError::Database(e.to_string()))?;
    Ok(())
}

/// Synchronous wrapper: connects to `database_url`, runs [`clear_tables`] then [`create_tables`].
pub fn run_clear(database_url: &str) -> Result<PgPool, WriterError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    rt.block_on(async {
        let pool = async_connect(database_url).await?;
        clear_tables(&pool).await?;
        create_tables(&pool).await?;
        Ok(pool)
    })
}

/// Finalises the crawl by populating the `users` table and adding foreign key constraints.
///
/// Runs after all COPY ingestion is complete. Performs two steps:
///
/// - Populates `users` with all distinct `owner_uid` values seen in `files` and `directories`
/// - Adds foreign key constraints to `directories`, `files`, and `directory_stats`
///   that were deliberately omitted during ingestion for performance
pub async fn post_crawl(pool: &PgPool) -> Result<(), WriterError> {
    // 1. populate users from distinct uids seen in crawl
    sqlx::query(
        "INSERT INTO users (uid)
         SELECT DISTINCT owner_uid
         FROM files
         WHERE owner_uid IS NOT NULL
         UNION
         SELECT DISTINCT owner_uid
         FROM directories
         WHERE owner_uid IS NOT NULL
         ON CONFLICT (uid) DO NOTHING"
    )
        .execute(pool)
        .await
        .map_err(|e| WriterError::Database(e.to_string()))?;

    sqlx::query(
        "ALTER TABLE directories
         ADD CONSTRAINT fk_directories_owner  FOREIGN KEY (owner_uid) REFERENCES users(uid),
         ADD CONSTRAINT fk_directories_parent FOREIGN KEY (parent_id) REFERENCES directories(dir_id)"
    )
        .execute(pool)
        .await
        .map_err(|e| WriterError::Database(e.to_string()))?;

    sqlx::query(
        "ALTER TABLE files
         ADD CONSTRAINT fk_files_owner FOREIGN KEY (owner_uid) REFERENCES users(uid),
         ADD CONSTRAINT fk_files_dir   FOREIGN KEY    (dir_id) REFERENCES directories(dir_id)"
    )
        .execute(pool)
        .await
        .map_err(|e| WriterError::Database(e.to_string()))?;


    sqlx::query(
        "ALTER TABLE directory_stats
         ADD CONSTRAINT fk_dir_stats_owner FOREIGN KEY (dir_id) REFERENCES directories(dir_id)"
    )
        .execute(pool)
        .await
        .map_err(|e| WriterError::Database(e.to_string()))?;

    Ok(())
}

/// Synchronous wrapper: connects to `database_url` and runs [`post_crawl`].
pub fn run_post_crawl(database_url: &str) -> Result<(), WriterError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async {
        let pool = async_connect(database_url).await?;
        post_crawl(&pool).await
    })
}

/// Builds the directory closure table and computes per-directory size statistics.
///
/// Runs after [`post_crawl`]. Performs two steps:
///
/// - Populates `directory_closure` using a recursive CTE that walks the directory
///   tree, recording every ancestor-descendant pair and its depth
/// - Populates `directory_stats` with direct and subtree byte counts and file counts
///   for every directory, derived from the closure table and `files`
pub async fn finish(pool: &PgPool) -> Result<(), WriterError>{
    sqlx::query(
        "WITH RECURSIVE closure(ancestor_id, descendant_id, depth) AS (
            -- every directory is its own ancestor at depth 0
                SELECT dir_id, dir_id, 0
                FROM directories
            UNION ALL
                -- extend: for each known (ancestor→descendant), follow descendant's children
                SELECT c.ancestor_id, d.dir_id, c.depth + 1
                FROM closure c
                JOIN directories d ON d.parent_id = c.descendant_id
        )
            INSERT INTO directory_closure (ancestor_id, descendant_id, depth)
            SELECT ancestor_id, descendant_id, depth FROM closure"
    )
        .execute(pool)
        .await
        .map_err(|e| WriterError::Database(e.to_string()))?;

    sqlx::query(
        "INSERT INTO directory_stats (
             dir_id,
             direct_bytes,
             subtree_bytes,
             file_count,
             subtree_count,
             last_computed
         )
         SELECT
             d.dir_id,
             COALESCE(direct.direct_bytes,   0),
             COALESCE(subtree.subtree_bytes, 0),
             COALESCE(direct.file_count,     0),
             COALESCE(subtree.subtree_count, 0),
             NOW()
         FROM directories d
         LEFT JOIN (
             SELECT dir_id,
                 SUM(size_bytes) AS direct_bytes,
                 COUNT(*)        AS file_count
             FROM files
             GROUP BY dir_id
         ) direct ON direct.dir_id = d.dir_id
         LEFT JOIN (
             SELECT dc.ancestor_id AS dir_id,
                 SUM(f.size_bytes) AS subtree_bytes,
                 COUNT(f.file_id)  AS subtree_count
            FROM directory_closure dc
            JOIN files f ON f.dir_id = dc.descendant_id
            GROUP BY dc.ancestor_id
        ) subtree ON subtree.dir_id = d.dir_id"
    )
        .execute(pool)
        .await
        .map_err(|e| WriterError::Database(e.to_string()))?;

    Ok(())
}

/// Synchronous wrapper: connects to `database_url` and runs [`finish`].
pub fn run_finish(database_url: &str) -> Result<(), WriterError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async {
        let pool = async_connect(database_url).await?;
        finish(&pool).await
    })
}


// ############################################################
//                             TODO
// ############################################################
// Probably best to implement as a standalone outside of db.rs, idk

/// Reads `/etc/passwd` and returns a list of `(uid, username, gid)` entries.
///
/// Parses the colon-delimited passwd format. Lines that cannot be parsed are silently skipped.
///
/// Currently unused — intended for use with [`add_usernames`].
#[allow(dead_code)]
fn read_passwd_file() -> Result<Vec<(u32, String, u32)>, WriterError> {
    use std::io::{BufRead, BufReader};
    use std::fs::File;

    let file = File::open("/etc/passwd")
        .map_err(|e| WriterError::Io(e))?;

    let reader = BufReader::new(file);
    let mut entries = Vec::new();

    for line in reader.lines() {
        let line = line.map_err(|e| WriterError::Io(e))?;
        // passwd format: username:password:uid:gid:gecos:home:shell
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() >= 4 {
            if let (Ok(uid), Ok(gid)) = (fields[2].parse::<u32>(), fields[3].parse::<u32>()) {
                entries.push((uid, fields[0].to_string(), gid));
            }
        }
    }
    Ok(entries)
}

/// Updates the `users` table with usernames and GIDs read from `/etc/passwd`.
///
/// Matches on `uid` and sets `username` and `gid` for any users already present
/// in the table. Users not found in `/etc/passwd` are left unchanged.
///
/// Currently unused — call after [`post_crawl`] to enrich the `users` table.
#[allow(dead_code)]
pub async fn add_usernames(pool: &PgPool) -> Result<(), WriterError>{
    // add usernames from /etc/passwd via a temporary table
    //  we read the OS passwd entries and update the users table
    let passwd_entries = read_passwd_file()?;

    for (uid, username, gid) in passwd_entries {
        sqlx::query(
            "UPDATE users SET username = $1, gid = $3 WHERE uid = $2"
        )
            .bind(username)
            .bind(uid as i64)
            .bind(gid as i64)
            .execute(pool)
            .await
            .map_err(|e| WriterError::Database(e.to_string()))?;
    }
    Ok(())
}
