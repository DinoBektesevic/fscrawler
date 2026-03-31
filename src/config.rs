use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    server: ServerConfig,
    filesystems: Vec<FilesystemConfig>
}

#[derive(Deserialize)]
struct ServerConfig{
    connection_string: String,
}

#[derive(Deserialize)]
struct FilesystemConfig{
    name: String,
    root: String,
    workers: Option<i32>,
}

#[derive(Debug)]
pub enum ConfigError{
    IOError(std::io::Error),
    ParsingError(toml::de::Error),
}

pub fn read_config(path: &str) -> Result<Config, ConfigError> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| ConfigError::IOError(e))?;

    let config = toml::from_str(&raw)
        .map_err(|e| ConfigError::ParsingError(e))?;
    Ok(config)
}
