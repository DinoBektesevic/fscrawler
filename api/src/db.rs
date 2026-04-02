use sqlx::{FromRow, PgPool};

#[derive(FromRow)]
pub struct FilesystemRow {
    pub dir_id:        i64,
    pub path:          String,
    pub subtree_bytes: i64,
    pub subtree_count: i64,
}

#[derive(FromRow)]
pub struct UserRow {
    pub uid:         i64,
    pub display_name: String,
    pub total_bytes: i64,
    pub file_count:  i64,
}

#[derive(FromRow)]
pub struct UserFsBreakdownRow {
    pub filesystem: String,
    pub bytes:      i64,
    pub file_count: i64,
    pub dir_id:     i64,
}

#[derive(FromRow)]
pub struct DirChildRow {
    pub dir_id:        i64,
    pub path:          String,
    pub subtree_bytes: i64,
    pub subtree_count: i64,
}

#[derive(FromRow)]
pub struct UserDirChildRow {
    pub dir_id: i64,
    pub path: String,
    pub user_bytes: i64,
    pub user_files: i64,
    pub last_mtime: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(FromRow)]
pub struct UserTreeRow {
    pub dir_id:     i64,
    pub path:       String,
    pub user_bytes: i64,
    pub user_files: i64,
    pub last_mtime: Option<chrono::DateTime<chrono::Utc>>,
}

pub async fn get_filesystems(pool: &PgPool) -> Result<Vec<FilesystemRow>, sqlx::Error> {
    sqlx::query_as::<_, FilesystemRow>(
        "SELECT d.dir_id,
                d.path,
                COALESCE(ds.subtree_bytes, 0) AS subtree_bytes,
                COALESCE(ds.subtree_count, 0) AS subtree_count
         FROM directories d
         LEFT JOIN directory_stats ds ON ds.dir_id = d.dir_id
         WHERE d.parent_id IS NULL
         ORDER BY d.path",
    )
    .fetch_all(pool)
    .await
}

pub async fn get_users(pool: &PgPool) -> Result<Vec<UserRow>, sqlx::Error> {
    sqlx::query_as::<_, UserRow>(
        "SELECT u.uid,
                COALESCE(u.username, u.uid::text)       AS display_name,
                COALESCE(SUM(f.size_bytes), 0)::bigint  AS total_bytes,
                COUNT(f.file_id)                        AS file_count
         FROM users u
         LEFT JOIN files f ON f.owner_uid = u.uid
         GROUP BY u.uid, u.username
         ORDER BY total_bytes DESC",
    )
    .fetch_all(pool)
    .await
}

pub async fn get_user_breakdown(
    pool: &PgPool,
    uid: i64,
) -> Result<Vec<UserFsBreakdownRow>, sqlx::Error> {
    sqlx::query_as::<_, UserFsBreakdownRow>(
        "SELECT us.dir_id,
        d.path         AS filesystem,
        us.total_bytes AS bytes,
        us.file_count
            FROM user_stats us
            JOIN directories d ON d.dir_id = us.dir_id
        WHERE us.uid = $1
            ORDER BY us.total_bytes DESC"
    )
    .bind(uid)
    .fetch_all(pool)
    .await
}

pub async fn get_table_counts(pool: &PgPool) -> Result<Vec<(String, i64)>, sqlx::Error> {
    let tables = ["files", "directories", "directory_stats", "directory_closure", "users"];
    let mut counts = Vec::new();
    for table in tables {
        let n: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {}", table))
            .fetch_one(pool)
            .await?;
        counts.push((table.to_string(), n));
    }
    Ok(counts)
}

pub async fn get_dir_children(
    pool: &PgPool,
    dir_id: i64,
) -> Result<Vec<DirChildRow>, sqlx::Error> {
    sqlx::query_as::<_, DirChildRow>(
        "SELECT d.dir_id,
                d.path,
                COALESCE(ds.subtree_bytes, 0) AS subtree_bytes,
                COALESCE(ds.subtree_count, 0) AS subtree_count
         FROM directories d
         LEFT JOIN directory_stats ds ON ds.dir_id = d.dir_id
         WHERE d.parent_id = $1
         ORDER BY subtree_bytes DESC",
    )
    .bind(dir_id)
    .fetch_all(pool)
    .await
}

pub async fn get_user_dir_children(
    pool: &PgPool,
    dir_id: i64,
    uid: i64) -> Result<Vec<UserDirChildRow>, sqlx::Error>{
    sqlx::query_as::<_, UserDirChildRow>(
        "SELECT d.dir_id, d.path,
         COALESCE(SUM(f.size_bytes), 0)::bigint AS user_bytes,
         COUNT(f.file_id)::bigint               AS user_files,
         MAX(f.mtime)                           AS last_mtime
         FROM directories d
         JOIN directory_closure dc ON dc.ancestor_id = d.dir_id
         JOIN files f              ON f.dir_id = dc.descendant_id AND f.owner_uid = $2
         WHERE d.parent_id = $1
         GROUP BY d.dir_id, d.path
         ORDER BY user_bytes DESC"
    )
    .bind(dir_id)
    .bind(uid)
    .fetch_all(pool)
    .await
}

pub async fn get_user_tree(
    pool: &PgPool,
    uid: i64) -> Result<Vec<UserTreeRow>, sqlx::Error>{
    sqlx::query_as::<_, UserTreeRow>(
        "SELECT d.dir_id, d.path,
         COALESCE(SUM(f.size_bytes), 0)::bigint AS user_bytes,
         COUNT(f.file_id)::bigint               AS user_files,
         MAX(f.mtime)                           AS last_mtime
         FROM files f
         JOIN directories d ON d.dir_id = f.dir_id
         WHERE f.owner_uid = $1
         GROUP BY d.dir_id, d.path
         ORDER BY last_mtime DESC NULLS LAST
         LIMIT 200"
    )
        .bind(uid)
        .fetch_all(pool)
        .await
}
