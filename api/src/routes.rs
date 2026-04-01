use axum::{
    extract::{Path, State},
    response::Html,
};
use sqlx::PgPool;

use crate::db;

fn fmt_bytes(bytes: i64) -> String {
    match bytes {
        b if b >= 1_073_741_824 => format!("{:.1} GB", b as f64 / 1_073_741_824.0),
        b if b >= 1_048_576     => format!("{:.1} MB", b as f64 / 1_048_576.0),
        b if b >= 1_024         => format!("{:.1} KB", b as f64 / 1_024.0),
        b                       => format!("{} B", b),
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
            r##"<tr class="clickable"
                    hx-get="/api/dirs/{dir_id}"
                    hx-target="#dir-view"
                    hx-swap="innerHTML">
                  <td>{path}</td>
                  <td class="num">{size}</td>
                  <td class="num">{count}</td>
                </tr>"##,
            dir_id = row.dir_id,
            path   = row.path,
            size   = fmt_bytes(row.subtree_bytes),
            count  = row.subtree_count,
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
            r##"<tr class="clickable"
                    hx-get="/api/users/{uid}/detail"
                    hx-target="#user-detail"
                    hx-swap="innerHTML">
                  <td>{name}</td>
                  <td class="num">{size}</td>
                  <td class="num">{count}</td>
                </tr>"##,
            uid   = row.uid,
            name  = row.display_name,
            size  = fmt_bytes(row.total_bytes),
            count = row.file_count,
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
        Err(e) => return Html(format!("<p class='error'>Error: {}</p>", e)),
    };

    let mut html = String::from(
        "<table>\
           <thead><tr><th>Filesystem</th><th>Size</th><th>Files</th></tr></thead>\
           <tbody>",
    );
    for row in &rows {
        html.push_str(&format!(
            "<tr>\
               <td>{fs}</td>\
               <td class='num'>{size}</td>\
               <td class='num'>{count}</td>\
             </tr>",
            fs    = row.filesystem,
            size  = fmt_bytes(row.bytes),
            count = row.file_count,
        ));
    }
    html.push_str("</tbody></table>");
    Html(html)
}

pub async fn dir_children(
    State(pool): State<PgPool>,
    Path(dir_id): Path<i64>,
) -> Html<String> {
    let rows = match db::get_dir_children(&pool, dir_id).await {
        Ok(r)  => r,
        Err(e) => return Html(format!("<p class='error'>Error: {}</p>", e)),
    };

    let mut html = String::from(
        "<table>\
           <thead><tr><th>Directory</th><th>Size</th><th>Files</th></tr></thead>\
           <tbody>",
    );
    for row in &rows {
        let name = row.path.split('/').next_back().unwrap_or(&row.path);
        html.push_str(&format!(
            r##"<tr class="clickable"
                    hx-get="/api/dirs/{dir_id}"
                    hx-target="#dir-view"
                    hx-swap="innerHTML">
                  <td>{name}</td>
                  <td class="num">{size}</td>
                  <td class="num">{count}</td>
                </tr>"##,
            dir_id = row.dir_id,
            name   = name,
            size   = fmt_bytes(row.subtree_bytes),
            count  = row.subtree_count,
        ));
    }
    html.push_str("</tbody></table>");
    Html(html)
}
