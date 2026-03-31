use clap::{Parser, ValueEnum};
use fscrawler::config::{
    Config,
    FilesystemConfig,
    ServerConfig,
    ConfigError,
    read_config
};


#[derive(ValueEnum, Clone)]
pub enum OutputMode {
    Stdout,
    Table,
    Postgres,
}

#[derive(Parser)]
#[command(name = "fscrawler", about = "Filesystem metadata crawler")]
pub struct Cli {
    /// Root path to crawl
    pub root: Option<String>,

    /// Output backend
    #[arg(long, value_enum, default_value = "stdout")]
    pub output: OutputMode,

    /// Executes the table creation statements
    #[arg(long)]
    pub create_tables: bool,

    /// Clears all crawler tables and re-initialises the schema (requires --database-url)
    #[arg(long)]
    pub clear: bool,

    /// Postgres connection URL (required when --output=postgres and/or create-tables)
    #[arg(long)]
    pub database_url: Option<String>,

    /// Number of worker threads (defaults to available parallelism)
    #[arg(long)]
    pub workers: Option<usize>,

    /// Config file path
    #[arg(long)]
    pub config_path: Option<String>
}


impl Cli{
    pub fn validate(&self) -> Result<(), &'static str> {
        // validate postgres url is provided when needed
        match (&self.create_tables, &self.database_url){
            (true, None) => return Err("error: --database-url required when --create-tables provided"),
            (_, _) => Ok(()),
        };

        // validate postgres url is provided when needed
        match (&self.output, &self.database_url){
            (OutputMode::Postgres, None) => return Err("error: --database-url required when --output=postgres"),
            (_, _) => Ok(()),
        };

        // validate postgres url is provided when needed
        match (&self.clear, &self.database_url){
            (true, None) => return Err("error: --database-url required with --clear "),
            (_, _) => Ok(()),
        };

        Ok(())
    }

    pub fn resolve(&self) -> Result<Config, String> {
        let nwrks = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8);

        self.validate()?;

        match (&self.config_path, &self.root) {
            (Some(_),  Some(_)) => Err("--config-path and root are mutually exclusive".to_string()),
            (None,     None)    => Err("either a root path or --config-path is required".to_string()),
            (Some(p),  None)    => {
                // reject conflicting flags
                if self.database_url.is_some() {
                    return Err("--database-url conflicts with --config-path".to_string());
                }
                if self.workers.is_some() {
                    return Err("--workers conflicts with --config-path (set per-filesystem in config)".to_string());
                }
                if !matches!(&self.output, OutputMode::Postgres) {
                    return Err("--config-path implies --output=postgres".to_string());
                }
                Ok(read_config(&p).map_err(|e: ConfigError| e.to_string())?)
            }
            (None, Some(root)) => Ok(Config {
                  server: ServerConfig { connection_string: self.database_url.clone() },
                  filesystems: vec![FilesystemConfig {
                      name: root.clone(),
                      root: root.clone(),
                      workers: self.workers.unwrap_or_else(|| nwrks),
                  }],
              }),
        }
    }
}
