use clap::Parser;
use crate::config::{
    Config,
    FilesystemConfig,
    OutputMode,
    ServerConfig,
    ConfigError,
    read_config
};

/// Command-line interface for fscrawler.
///
/// Supports two operating modes:
///
/// **Single-root** — pass a positional `root` path. Worker count and database
/// URL are taken from `--workers` and `--database-url`.
///
/// **Multi-root** — pass `--config-path` pointing at a TOML file. The file
/// specifies all filesystem roots, per-root worker counts, and the database
/// URL. `--workers` and `--database-url` are rejected in this mode.
///
/// Example TOML config file:
/// ```toml
/// [server]
/// connection_string = "postgresql://crawler:pass@localhost/crawler_db"
///
/// [[filesystem]]
/// name    = "fsone"
/// root    = "/file/system/one"
/// workers = 32
///
/// [[filesystem]]
/// name    = "fstwo"
/// root    = "/file/system/two"
/// workers = 8
/// ```
///
/// `--create-tables` and `--clear` are standalone operations: they connect to
/// the database, perform their action, and exit without crawling.
#[derive(Parser)]
#[command(name = "fscrawler", about = "Filesystem metadata crawler")]
pub struct Cli {
    /// Root path to crawl (single-root mode; mutually exclusive with --config-path)
    pub root: Option<String>,

    /// Output backend
    #[arg(long, value_enum, default_value = "stdout")]
    pub output: OutputMode,

    /// Create crawler tables in the database then exit (requires --database-url)
    #[arg(long)]
    pub create_tables: bool,

    /// Truncate all crawler tables and re-initialise the schema then exit (requires --database-url)
    #[arg(long)]
    pub clear: bool,

    /// Postgres connection URL (required for --output=postgres, --create-tables, --clear)
    #[arg(long)]
    pub database_url: Option<String>,

    /// Number of worker threads (single-root mode only; defaults to available parallelism)
    #[arg(long)]
    pub workers: Option<usize>,

    /// Path to a TOML config file (multi-root mode; mutually exclusive with root)
    #[arg(long)]
    pub config_path: Option<String>
}


impl Cli {
    /// Checks that flag combinations are self-consistent.
    ///
    /// Called by [`Cli::resolve`] before mode detection. Errors if a database
    /// URL is required but not provided.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.create_tables && self.database_url.is_none() {
            return Err("--database-url required when --create-tables provided");
        }
        if matches!(self.output, OutputMode::Postgres) && self.database_url.is_none() {
            return Err("--database-url required when --output=postgres");
        }
        if self.clear && self.database_url.is_none() {
            return Err("--database-url required with --clear");
        }
        Ok(())
    }

    /// Validates CLI flags and resolves them into a [`Config`].
    ///
    /// In single-root mode a [`Config`] is synthesised from the positional
    /// `root`, `--workers`, and `--database-url` flags. In multi-root mode
    /// the [`Config`] is deserialised from the TOML file at `--config-path`.
    /// In both cases `config.output` is set to the effective output mode —
    /// multi-root always resolves to `Postgres`, single-root uses `--output`.
    ///
    /// Returns `Err` if flags are inconsistent or the config file cannot be
    /// read or parsed.
    pub fn resolve(&self) -> Result<Config, String> {
        let nwrks = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8);

        self.validate()?;

        match (&self.config_path, &self.root) {
            (Some(_),  Some(_)) => Err("--config-path and root are mutually exclusive".to_string()),
            (None,     None)    => Err("either a root path or --config-path is required".to_string()),
            (Some(p),  None)    => {
                if self.database_url.is_some() {
                    return Err("--database-url conflicts with --config-path".to_string());
                }
                if self.workers.is_some() {
                    return Err(
                        "--workers conflicts with --config-path (set per-filesystem in config)"
                            .to_string()
                    );
                }
                let mut config = read_config(&p).map_err(|e: ConfigError| e.to_string())?;
                config.output = OutputMode::Postgres;
                Ok(config)
            }
            (None, Some(root)) => Ok(Config {
                server: ServerConfig { connection_string: self.database_url.clone() },
                filesystems: vec![FilesystemConfig {
                    name: root.clone(),
                    root: root.clone(),
                    workers: Some(self.workers.unwrap_or(nwrks) as i32),
                }],
                output: self.output.clone(),
            }),
        }
    }
}
