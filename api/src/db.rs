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
}

#[derive(FromRow)]
pub struct DirChildRow {
    pub dir_id:        i64,
    pub path:          String,
    pub subtree_bytes: i64,
    pub subtree_count: i64,
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
        "SELECT d_root.path                            AS filesystem,
                COALESCE(SUM(f.size_bytes), 0)::bigint AS bytes,
                COUNT(f.file_id)                       AS file_count
         FROM files f
         JOIN directory_closure dc ON dc.descendant_id = f.dir_id
         JOIN directories d_root   ON d_root.dir_id = dc.ancestor_id
                                  AND d_root.parent_id IS NULL
         WHERE f.owner_uid = $1
         GROUP BY d_root.path
         ORDER BY bytes DESC",
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
