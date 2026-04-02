use axum::{Router, routing::get};
use clap::Parser;
use tower_http::services::ServeDir;

use fscrawler::config::{read_config, Config};

mod db;
mod routes;

const DEFAULT_ADDR:       &str = "127.0.0.1:8585";
const DEFAULT_STATIC_DIR: &str = "static";

/// HTTP API for fscrawler.
///
/// Serves both the JSON/HTML API and the static frontend from one port.
/// Reads configuration from `--config-path` (same TOML as the crawler) or
/// falls back to the `DATABASE_URL` environment variable.
///
/// Example:
///   api --config-path /etc/fscrawler/config.toml
///   DATABASE_URL=postgresql://... api
#[derive(Parser)]
#[command(name = "api", about = "fscrawler HTTP API")]
struct Cli {
    /// Path to fscrawler TOML config file (reads [db] and [api] sections)
    #[arg(long)]
    config_path: Option<String>,

    /// Address and port to listen on (overrides [api] addr in config)
    #[arg(long)]
    addr: Option<String>,

    /// Directory to serve static files from (overrides [api] static_dir in config)
    #[arg(long)]
    static_dir: Option<String>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let config = cli.config_path.as_deref().map(|path| {
        read_config(path).unwrap_or_else(|e| {
            eprintln!("error reading config file: {}", e);
            std::process::exit(1);
        })
    });

    let api_cfg = config.as_ref().and_then(|c| c.api.as_ref());

    let db_url     = resolve_db_url(&cli, config.as_ref());
    let addr       = resolve_opt(&cli.addr,       api_cfg.and_then(|a| a.addr.as_deref()),       DEFAULT_ADDR);
    let static_dir = resolve_opt(&cli.static_dir, api_cfg.and_then(|a| a.static_dir.as_deref()), DEFAULT_STATIC_DIR);

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .after_connect(|conn, _meta| Box::pin(async move {
            sqlx::query("SET search_path TO crawler")
                .execute(conn)
                .await?;
            Ok(())
        }))
        .connect(&db_url)
        .await
        .expect("failed to connect to database");

    let app = Router::new()
        .route("/health",                 get(routes::health))
        .route("/api/debug",              get(routes::debug))
        .route("/api/filesystems",        get(routes::filesystems))
        .route("/api/users",              get(routes::users))
        .route("/api/users/{uid}/detail",          get(routes::user_detail))
        .route("/api/users/{uid}/dirs/{dir_id}",   get(routes::user_dir_children))
        .route("/api/users/{uid}/tree",            get(routes::user_tree))
        .route("/api/dirs/{dir_id}",               get(routes::dir_children))
        .route("/api/last_crawled",                get(routes::last_crawled))
        .route("/api/staleness",                   get(routes::staleness))
        .route("/api/users/{uid}/staleness",       get(routes::user_staleness))
        .route("/api/users/{uid}/summary",         get(routes::user_summary))
        .route("/mydisk/{uid}",                    get(routes::mydisk_page))
        .fallback_service(ServeDir::new(&static_dir))
        .with_state(pool);

    let listener = tokio::net::TcpListener::bind(&addr).await
        .unwrap_or_else(|e| { eprintln!("failed to bind {}: {}", addr, e); std::process::exit(1); });

    println!("listening on http://{}", addr);

    if let Err(e) = axum::serve(listener, app).await {
        eprintln!("server error: {}", e);
    }
}

fn resolve_db_url(_cli: &Cli, config: Option<&Config>) -> String {
    if let Some(config) = config {
        return config.db.url.clone().unwrap_or_else(|| {
            eprintln!("error: [db] url missing from config file");
            std::process::exit(1);
        });
    }
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        eprintln!("error: --config-path or DATABASE_URL required");
        std::process::exit(1);
    })
}

fn resolve_opt(cli_flag: &Option<String>, config_val: Option<&str>, default: &str) -> String {
    cli_flag.as_deref().or(config_val).unwrap_or(default).to_string()
}
