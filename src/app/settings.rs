use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::app::types::LockMode;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneralSettings {
    pub default_base_path: PathBuf,
    pub check_for_updates: bool,
    pub language: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestSettings {
    pub buffer_size_kb: usize,
    pub parallel_files: usize,
    pub verify_after_copy: bool,
    pub separate_raw_jpg: bool,
    pub include_proxy_files: bool,
    pub skip_hidden_files: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafetySettings {
    pub lock_mode: LockMode,
    pub auto_lock_on_complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UiSettings {
    pub theme: String,
    pub show_speed_indicator: bool,
    pub confirm_before_start: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ManufacturerSettings {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    pub general: GeneralSettings,
    pub ingest: IngestSettings,
    pub safety: SafetySettings,
    pub ui: UiSettings,
    #[serde(default)]
    pub manufacturers: ManufacturerSettings,
}

#[derive(Debug, Clone)]
pub struct AppPaths {
    pub root_dir: PathBuf,
    pub logs_dir: PathBuf,
    pub backups_dir: PathBuf,
    pub settings_file: PathBuf,
    pub db_file: PathBuf,
    pub lock_file: PathBuf,
}

impl AppPaths {
    pub fn discover() -> Result<Self> {
        let base = dirs::config_dir().context("unable to locate app config directory")?;
        let root_dir = base.join("FastTrack");
        Ok(Self {
            logs_dir: root_dir.join("logs"),
            backups_dir: root_dir.join("backups"),
            settings_file: root_dir.join("settings.toml"),
            db_file: root_dir.join("fasttrack.db"),
            lock_file: root_dir.join(".lock"),
            root_dir,
        })
    }

    pub fn ensure(&self) -> Result<()> {
        fs::create_dir_all(&self.root_dir)?;
        fs::create_dir_all(&self.logs_dir)?;
        fs::create_dir_all(&self.backups_dir)?;
        Ok(())
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            general: GeneralSettings {
                default_base_path: PathBuf::from(r"D:\Projects"),
                check_for_updates: false,
                language: "en".into(),
            },
            ingest: IngestSettings {
                buffer_size_kb: 4096,
                parallel_files: 1,
                verify_after_copy: true,
                separate_raw_jpg: true,
                include_proxy_files: false,
                skip_hidden_files: true,
            },
            safety: SafetySettings {
                lock_mode: LockMode::Attribute,
                auto_lock_on_complete: true,
            },
            ui: UiSettings {
                theme: "system".into(),
                show_speed_indicator: true,
                confirm_before_start: true,
            },
            manufacturers: ManufacturerSettings::default(),
        }
    }
}

impl Settings {
    pub fn load_or_create(paths: &AppPaths) -> Result<Self> {
        paths.ensure()?;
        if paths.settings_file.exists() {
            let raw = fs::read_to_string(&paths.settings_file)?;
            Ok(toml::from_str(&raw)?)
        } else {
            let settings = Self::default();
            settings.save(paths)?;
            Ok(settings)
        }
    }

    pub fn save(&self, paths: &AppPaths) -> Result<()> {
        let raw = toml::to_string_pretty(self)?;
        fs::write(&paths.settings_file, raw)?;
        Ok(())
    }
}
