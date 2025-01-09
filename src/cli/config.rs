//! Configuration Management
//!
//! Handles global and database-specific configuration.

#![allow(dead_code)]

use anyhow::Result;
use colored::Colorize;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Configuration structure
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    /// Default output format (table, json, csv)
    #[serde(default)]
    pub default_format: String,

    /// Default query limit
    #[serde(default)]
    pub default_limit: Option<usize>,

    /// Additional key-value settings
    #[serde(default)]
    pub settings: BTreeMap<String, String>,

    /// Path to config file
    #[serde(skip)]
    path: Option<PathBuf>,
}

impl Config {
    /// Load configuration from default location
    pub fn load() -> Result<Self> {
        let config_path = Self::config_path()?;

        if config_path.exists() {
            let content = std::fs::read_to_string(&config_path)?;
            let mut config: Config = toml::from_str(&content)?;
            config.path = Some(config_path);
            Ok(config)
        } else {
            let config = Config {
                path: Some(config_path),
                ..Default::default()
            };
            Ok(config)
        }
    }

    /// Save configuration
    pub fn save(&self) -> Result<()> {
        if let Some(ref path) = self.path {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            let content = toml::to_string_pretty(self)?;
            std::fs::write(path, content)?;
        }
        Ok(())
    }

    /// Get config file path
    fn config_path() -> Result<PathBuf> {
        let config_dir = if cfg!(target_os = "macos") {
            std::env::var("HOME").map(|h| PathBuf::from(h).join(".config/aresadb"))?
        } else if cfg!(target_os = "linux") {
            std::env::var("XDG_CONFIG_HOME")
                .map(PathBuf::from)
                .or_else(|_| {
                    std::env::var("HOME").map(|h| PathBuf::from(h).join(".config/aresadb"))
                })?
        } else if cfg!(target_os = "windows") {
            std::env::var("APPDATA").map(|p| PathBuf::from(p).join("aresadb"))?
        } else {
            PathBuf::from(".")
        };

        Ok(config_dir.join("config.toml"))
    }

    /// Set a configuration value
    pub fn set(&self, key: &str, value: &str) -> Result<()> {
        let mut config = self.clone();

        match key {
            "default_format" => config.default_format = value.to_string(),
            "default_limit" => config.default_limit = value.parse().ok(),
            _ => {
                config.settings.insert(key.to_string(), value.to_string());
            }
        }

        config.save()
    }

    /// Get a configuration value
    pub fn get(&self, key: &str) -> Option<String> {
        match key {
            "default_format" => Some(self.default_format.clone()),
            "default_limit" => self.default_limit.map(|l| l.to_string()),
            _ => self.settings.get(key).cloned(),
        }
    }

    /// Print all configuration
    pub fn print_all(&self) -> Result<()> {
        println!("{}", "Configuration:".bright_yellow().bold());
        println!();

        println!("{}", "General:".bright_cyan());
        println!(
            "  default_format: {}",
            if self.default_format.is_empty() {
                "table"
            } else {
                &self.default_format
            }
        );
        println!(
            "  default_limit: {}",
            self.default_limit
                .map(|l| l.to_string())
                .unwrap_or_else(|| "1000".to_string())
        );

        if !self.settings.is_empty() {
            println!();
            println!("{}", "Custom:".bright_cyan());
            for (key, value) in &self.settings {
                println!("  {}: {}", key, value);
            }
        }

        Ok(())
    }
}
