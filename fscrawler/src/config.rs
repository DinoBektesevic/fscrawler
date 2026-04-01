use serde::Deserialize;
use std::fmt;
use clap::ValueEnum;

/// Where crawl results are written.
///
/// Part of the resolved [`Config`] — set by [`crate::cli::Cli::resolve`] based
/// on CLI flags. `Stdout` is the default for single-root mode. Multi-root mode
/// (TOML config) always resolves to `Postgres`.
///
/// Stored on [`Config`] so that all execution decisions live in one place after
/// CLI parsing is complete. The field is skipped during TOML deserialisation
/// and filled in programmatically by `resolve()`.
#[derive(ValueEnum, Clone, Default)]
pub enum OutputMode {
    #[default]
    Stdout,
    Table,
    Postgres,
}

/// Top-level configuration, produced by [`crate::cli::Cli::resolve`].
///
/// In multi-root mode this is deserialised from a TOML file and then
/// `output` is set to `Postgres`. In single-root mode the struct is
/// synthesised entirely from CLI flags.
///
/// Contains one `[db]` section and one or more `[[filesystem]]` entries.
#[derive(Deserialize)]
pub struct Config {
    pub db: DbConfig,
    // TOML entries look better as [[filesystem]] not [[filesystems]]
    // but the code reads better as filesystemS, so we rename here
    #[serde(rename = "filesystem")]
    pub filesystems: Vec<FilesystemConfig>,
    // Ignored by fscrawler, used by the api binary
    #[serde(default)]
    pub api: Option<ApiConfig>,
    // Not read from TOML — set programmatically by resolve()
    #[serde(skip, default)]
    pub output: OutputMode,
}

/// Database connection settings from the `[db]` TOML section.
///
/// `url` is optional so that single-root CLI mode (stdout/table output)
/// can produce a [`Config`] without a database URL.
#[derive(Deserialize)]
pub struct DbConfig {
    pub url: Option<String>,
}

/// API server hosting settings from the `[api]` TOML section.
///
/// All fields are optional — absent values fall back to the hardcoded
/// defaults in the `api` binary.
#[derive(Deserialize)]
pub struct ApiConfig {
    pub addr:       Option<String>,
    pub static_dir: Option<String>,
}

/// Per-filesystem crawl settings from a `[[filesystem]]` TOML entry.
///
/// `workers` is optional — if absent, `crawl_filesystem` falls back to the
/// number of logical CPUs available on the machine.
#[derive(Deserialize)]
pub struct FilesystemConfig {
    /// Short label used in logs and future reporting.
    pub name: String,
    /// Absolute path to the directory root to crawl.
    pub root: String,
    /// Number of worker threads for this filesystem. `None` → auto-detect.
    pub workers: Option<i32>,
}

/// Errors that can occur while loading a TOML config file.
#[derive(Debug)]
pub enum ConfigError {
    IOError(std::io::Error),
    ParsingError(toml::de::Error),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::IOError(e)      => write!(f, "could not read config file: {}", e),
            ConfigError::ParsingError(e) => write!(f, "could not parse config file: {}", e),
        }
    }
}

/// Reads and deserialises a TOML config file from `path`.
pub fn read_config(path: &str) -> Result<Config, ConfigError> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| ConfigError::IOError(e))?;

    let config = toml::from_str(&raw)
        .map_err(|e| ConfigError::ParsingError(e))?;
    Ok(config)
}
