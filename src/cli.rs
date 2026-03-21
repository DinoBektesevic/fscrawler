use clap::{Parser, ValueEnum};

#[derive(ValueEnum, Clone)]
pub enum OutputMode {
    Stdout,
    Table,
    Postgres,
}

#[derive(Parser)]
#[command(name = "fs_crawler", about = "Filesystem metadata crawler")]
pub struct Cli {
    /// Root path to crawl
    pub root: String,

    /// Output backend
    #[arg(long, value_enum, default_value = "stdout")]
    pub output: OutputMode,

    /// Postgres connection URL (required when --output=postgres)
    #[arg(long)]
    pub database_url: Option<String>,

    /// Number of worker threads (defaults to available parallelism)
    #[arg(long)]
    pub workers: Option<usize>,
}
