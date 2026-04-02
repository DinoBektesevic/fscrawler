use axum::{
    extract::{Path, State},
    response::Html,
    Json,
};
use sqlx::PgPool;
use chrono_tz::America::Los_Angeles;

use crate::db;

fn fmt_bytes(bytes: i64) -> String {
    match bytes {
        b if b >= 1_073_741_824 => format!("{:.1} GB", b as f64 / 1_073_741_824.0),
        b if b >= 1_048_576     => format!("{:.1} MB", b as f64 / 1_048_576.0),
        b if b >= 1_024         => format!("{:.1} KB", b as f64 / 1_024.0),
        b                       => format!("{} B", b),
    }
}

fn fmt_mtime(t: &Option<chrono::DateTime<chrono::Utc>>) -> String {
    match t {
        Some(dt) => dt.with_timezone(&Los_Angeles).format("%Y-%m-%d %H:%M").to_string(),
        None     => "—".to_string(),
    }
}

pub async fn health(State(_): State<PgPool>) -> &'static str {
    "ok"
}

pub async fn debug(State(pool): State<PgPool>) -> Html<String> {
    let counts = match db::get_table_counts(&pool).await {
        Ok(c)  => c,
        Err(e) => return Html(format!("<pre>error: {}</pre>", e)),
    };
    let mut html = String::from("<pre>");
    for (table, count) in &counts {
        html.push_str(&format!("{:<25} {}\n", table, count));
    }
    html.push_str("</pre>");
    Html(html)
}

pub async fn filesystems(State(pool): State<PgPool>) -> Html<String> {
    let rows = match db::get_filesystems(&pool).await {
        Ok(r)  => r,
        Err(e) => return Html(format!("<tr><td colspan='3'>Error: {}</td></tr>", e)),
    };

    let html = rows.iter().map(|row| {
        format!(
            r#"<tr class="clickable" data-dir-id="{dir_id}" data-bytes="{bytes}" data-files="{files}" data-name="{path}">
                 <td>{path}</td>
                 <td class="num"><span class="num-val">{size}</span><span class="bar-wrap"><span class="bar"></span></span></td>
                 <td class="num">{files}</td>
               </tr>"#,
            dir_id = row.dir_id,
            path   = row.path,
            bytes  = row.subtree_bytes,
            size   = fmt_bytes(row.subtree_bytes),
            files  = row.subtree_count,
        )
    }).collect();

    Html(html)
}

pub async fn users(State(pool): State<PgPool>) -> Html<String> {
    let rows = match db::get_users(&pool).await {
        Ok(r)  => r,
        Err(e) => return Html(format!("<tr><td colspan='3'>Error: {}</td></tr>", e)),
    };

    let html = rows.iter().map(|row| {
        format!(
            r#"<tr class="clickable" data-uid="{uid}" data-bytes="{bytes}" data-files="{files}" data-name="{name}">
                 <td>{name}</td>
                 <td class="num"><span class="num-val">{size}</span><span class="bar-wrap"><span class="bar"></span></span></td>
                 <td class="num">{files}</td>
                 <td class="dim"><a class="mydisk-link" href="/mydisk/{uid}">↗</a></td>
               </tr>"#,
            uid   = row.uid,
            name  = row.display_name,
            bytes = row.total_bytes,
            size  = fmt_bytes(row.total_bytes),
            files = row.file_count,
        )
    }).collect();

    Html(html)
}

pub async fn user_detail(
    State(pool): State<PgPool>,
    Path(uid): Path<i64>,
) -> Html<String> {
    let rows = match db::get_user_breakdown(&pool, uid).await {
        Ok(r)  => r,
        Err(e) => return Html(format!("<tr><td colspan='3' class='error'>Error: {}</td></tr>", e)),
    };

    let html = rows.iter().map(|row| {
        format!(
            r#"<tr class="clickable" data-dir-id="{dir_id}" data-bytes="{bytes}" data-files="{files}" data-name="{fs}">
                 <td>{fs}</td>
                 <td class="num"><span class="num-val">{size}</span><span class="bar-wrap"><span class="bar"></span></span></td>
                 <td class="num">{files}</td>
               </tr>"#,
            dir_id = row.dir_id,
            fs     = row.filesystem,
            bytes  = row.bytes,
            size   = fmt_bytes(row.bytes),
            files  = row.file_count,
        )
    }).collect();

    Html(html)
}

pub async fn dir_children(
    State(pool): State<PgPool>,
    Path(dir_id): Path<i64>,
) -> Html<String> {
    let rows = match db::get_dir_children(&pool, dir_id).await {
        Ok(r)  => r,
        Err(e) => return Html(format!("<tr><td colspan='3' class='error'>Error: {}</td></tr>", e)),
    };

    let html = rows.iter().map(|row| {
        let name = row.path.split('/').next_back().unwrap_or(&row.path);
        format!(
            r#"<tr class="clickable" data-dir-id="{dir_id}" data-bytes="{bytes}" data-files="{files}" data-name="{name}" data-path="{path}">
                 <td>{name}</td>
                 <td class="num"><span class="num-val">{size}</span><span class="bar-wrap"><span class="bar"></span></span></td>
                 <td class="num">{files}</td>
               </tr>"#,
            dir_id = row.dir_id,
            name   = name,
            path   = row.path,
            bytes  = row.subtree_bytes,
            size   = fmt_bytes(row.subtree_bytes),
            files  = row.subtree_count,
        )
    }).collect();

    Html(html)
}

pub async fn user_dir_children(
    State(pool): State<PgPool>,
    Path((uid, dir_id)): Path<(i64, i64)>,
) -> Html<String> {
    let rows = match db::get_user_dir_children(&pool, dir_id, uid).await {
        Ok(r)  => r,
        Err(e) => return Html(format!("<tr><td colspan='4' class='error'>Error: {}</td></tr>", e)),
    };

    let html = rows.iter().map(|row| {
        let name  = row.path.split('/').next_back().unwrap_or(&row.path);
        let mtime = fmt_mtime(&row.last_mtime);
        format!(
            r#"<tr class="clickable" data-dir-id="{dir_id}" data-bytes="{bytes}" data-files="{files}" data-name="{name}" data-path="{path}" data-mtime="{mtime}">
                 <td>{name}</td>
                 <td class="num"><span class="num-val">{size}</span><span class="bar-wrap"><span class="bar"></span></span></td>
                 <td class="num">{files}</td>
                 <td class="dim">{mtime}</td>
               </tr>"#,
            dir_id = row.dir_id,
            name   = name,
            path   = row.path,
            bytes  = row.user_bytes,
            size   = fmt_bytes(row.user_bytes),
            files  = row.user_files,
            mtime  = mtime,
        )
    }).collect();

    Html(html)
}

pub async fn user_tree(
    State(pool): State<PgPool>,
    Path(uid): Path<i64>,
) -> Html<String> {
    let rows = match db::get_user_tree(&pool, uid).await {
        Ok(r)  => r,
        Err(e) => return Html(format!("<tr><td colspan='4' class='error'>Error: {}</td></tr>", e)),
    };

    let html = rows.iter().map(|row| {
        let mtime = fmt_mtime(&row.last_mtime);
        format!(
            r#"<tr class="clickable" data-dir-id="{dir_id}" data-bytes="{bytes}" data-files="{files}" data-name="{path}" data-mtime="{mtime}">
                 <td class="cell-path">{path}</td>
                 <td class="num"><span class="num-val">{size}</span><span class="bar-wrap"><span class="bar"></span></span></td>
                 <td class="num">{files}</td>
                 <td class="dim">{mtime}</td>
               </tr>"#,
            dir_id = row.dir_id,
            path   = row.path,
            bytes  = row.user_bytes,
            size   = fmt_bytes(row.user_bytes),
            files  = row.user_files,
            mtime  = mtime,
        )
    }).collect();

    Html(html)
}

pub async fn mydisk_page(Path(_uid): Path<i64>) -> Html<&'static str> {
    Html(include_str!("../static/mydisk.html"))
}

pub async fn last_crawled(State(pool): State<PgPool>) -> Html<String> {
    match db::get_last_crawled(&pool).await {
        Ok(Some(dt)) => Html(
            dt.with_timezone(&Los_Angeles)
              .format("data as of %Y-%m-%d")
              .to_string()
        ),
        _ => Html(String::new()),
    }
}

pub async fn staleness(State(pool): State<PgPool>) -> Json<Vec<db::StalenessPoint>> {
    match db::get_staleness(&pool).await {
        Ok(v)  => Json(v),
        Err(_) => Json(vec![]),
    }
}

pub async fn user_staleness(
    State(pool): State<PgPool>,
    Path(uid): Path<i64>,
) -> Json<Vec<db::StalenessPoint>> {
    match db::get_user_staleness(&pool, uid).await {
        Ok(v)  => Json(v),
        Err(_) => Json(vec![]),
    }
}

pub async fn user_summary(
    State(pool): State<PgPool>,
    Path(uid): Path<i64>,
) -> Json<Option<db::UserSummaryRow>> {
    match db::get_user_summary(&pool, uid).await {
        Ok(v)  => Json(v),
        Err(_) => Json(None),
    }
}
