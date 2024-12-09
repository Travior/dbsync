use crate::querygen::QueryGen;
use serde::Deserialize;
use serde_yaml::from_str;
use std::fs;

#[derive(Debug, Deserialize)]
pub struct SyncEntry {
    pub catalog: String,
    pub pinned_catalogs: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct SyncConfig {
    pub catalogs: Vec<SyncEntry>,
    pub host: String,
    pub pat: String,
    pub generation_config: QueryGen,
}

pub fn load_config(path: &str) -> Result<SyncConfig, Box<dyn std::error::Error>> {
    let config_string = fs::read_to_string(path)?;
    let config: SyncConfig = from_str(&config_string)?;
    Ok(config)
}
