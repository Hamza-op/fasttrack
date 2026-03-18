use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::app::types::{FileRecordSummary, FileStatus, MANIFEST_VERSION};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub client: String,
    pub event: String,
    pub ingested_at: String,
    pub source_drive: String,
    pub source_card_label: String,
    pub files: Vec<ManifestEntry>,
    pub total_files: usize,
    pub verified_files: usize,
    pub failed_files: usize,
    pub locked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub filename: String,
    pub relative_path: String,
    pub size_bytes: u64,
    pub xxh3_128: Option<String>,
    pub source_modified: String,
    pub status: String,
}

impl Manifest {
    pub fn from_records(
        client: &str,
        event: &str,
        source_drive: &Path,
        source_card_label: &str,
        files: Vec<FileRecordSummary>,
        locked: bool,
    ) -> Self {
        let verified_files = files
            .iter()
            .filter(|record| matches!(record.status, FileStatus::Verified))
            .count();
        let failed_files = files
            .iter()
            .filter(|record| matches!(record.status, FileStatus::Failed))
            .count();
        let total_files = files.len();
        Self {
            version: MANIFEST_VERSION,
            client: client.to_string(),
            event: event.to_string(),
            ingested_at: Utc::now().to_rfc3339(),
            source_drive: source_drive.display().to_string(),
            source_card_label: source_card_label.to_string(),
            files: files
                .into_iter()
                .map(|record| ManifestEntry {
                    filename: record.filename,
                    relative_path: record.relative_path,
                    size_bytes: record.size_bytes,
                    xxh3_128: record.xxh3_hash,
                    source_modified: record.source_modified,
                    status: record.status.db_value().to_string(),
                })
                .collect(),
            total_files,
            verified_files,
            failed_files,
            locked,
        }
    }
}

pub fn write_manifest(event_root: &Path, manifest: &Manifest) -> Result<PathBuf> {
    let path = event_root.join(".mooningest_manifest.json");
    let raw = serde_json::to_string_pretty(manifest)?;
    fs::write(&path, raw)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use crate::app::types::{FileRecordSummary, FileStatus};

    use super::Manifest;

    #[test]
    fn builds_manifest_from_records() {
        let manifest = Manifest::from_records(
            "Client",
            "Barat",
            std::path::Path::new(r"E:\"),
            "E:\\",
            vec![FileRecordSummary {
                filename: "clip.mp4".into(),
                relative_path: "Videos/Cam/clip.mp4".into(),
                size_bytes: 10,
                xxh3_hash: Some("abc".into()),
                source_modified: "2025-01-01T00:00:00Z".into(),
                status: FileStatus::Verified,
            }],
            true,
        );
        assert_eq!(manifest.version, 1);
        assert_eq!(manifest.verified_files, 1);
        assert!(manifest.locked);
    }
}
