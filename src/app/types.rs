use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::app::errors::MoonError;

pub const EVENT_SEQUENCE: [&str; 5] = ["Ubtan", "Mehndi", "Barat", "Walima", "Portraits"];
pub const MANIFEST_VERSION: u32 = 1;
pub const PATH_SOFT_LIMIT: usize = 240;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MediaType {
    PhotoRaw,
    PhotoJpg,
    Video,
    Audio,
}

impl MediaType {
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext.to_ascii_lowercase().as_str() {
            "arw" | "sr2" | "srf" | "cr2" | "cr3" | "crw" | "nef" | "nrw" | "raf" | "dng"
            | "rw2" | "orf" | "pef" | "srw" | "iiq" => {
                Some(Self::PhotoRaw)
            }
            "jpg" | "jpeg" | "jpe" | "hif" | "heif" | "heic" | "tiff" | "tif" => {
                Some(Self::PhotoJpg)
            }
            "mp4" | "mov" | "mxf" | "avi" | "mts" | "m2ts" | "mpg" | "mpeg" | "crm" | "m4v"
            | "3gp" | "3g2" => {
                Some(Self::Video)
            }
            "wav" | "mp3" | "bwf" | "aac" | "m4a" => Some(Self::Audio),
            _ => None,
        }
    }

    pub fn db_value(self) -> &'static str {
        match self {
            Self::PhotoRaw => "photo_raw",
            Self::PhotoJpg => "photo_jpg",
            Self::Video => "video",
            Self::Audio => "audio",
        }
    }

}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FileStatus {
    Pending,
    Copying,
    Verifying,
    Verified,
    Failed,
    SkippedDuplicate,
}

impl FileStatus {
    pub fn db_value(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Copying => "copying",
            Self::Verifying => "verifying",
            Self::Verified => "verified",
            Self::Failed => "failed",
            Self::SkippedDuplicate => "skipped_duplicate",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value {
            "copying" => Self::Copying,
            "verifying" => Self::Verifying,
            "verified" => Self::Verified,
            "failed" => Self::Failed,
            "skipped_duplicate" => Self::SkippedDuplicate,
            _ => Self::Pending,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Partial,
}

impl EventStatus {
    pub fn db_value(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Partial => "partial",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value {
            "in_progress" => Self::InProgress,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "partial" => Self::Partial,
            _ => Self::Pending,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LockMode {
    None,
    Attribute,
    Acl,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CameraMetadata {
    pub make: Option<String>,
    pub model: Option<String>,
    pub serial_number: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScannedFile {
    pub source_path: PathBuf,
    pub relative_source_path: PathBuf,
    pub filename: String,
    pub size_bytes: u64,
    pub source_modified: String,
    pub source_created: String,
    pub media_type: MediaType,
    pub camera_model: Option<String>,
    pub camera_serial: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ScanSummary {
    pub manufacturer: String,
    pub drive_root: PathBuf,
    pub card_label: Option<String>,
    pub total_size_bytes: u64,
    pub total_files: usize,
    pub raw_count: usize,
    pub jpg_count: usize,
    pub video_count: usize,
    pub audio_count: usize,
    pub skipped_count: usize,
    pub files: Vec<ScannedFile>,
}

impl ScanSummary {
    pub fn summary_text(&self) -> String {
        format!(
            "{} {} files · RAW {} · JPG {} · Video {} · Audio {} · {}",
            self.manufacturer,
            self.total_files,
            self.raw_count,
            self.jpg_count,
            self.video_count,
            self.audio_count,
            format_bytes(self.total_size_bytes)
        )
    }
}

#[derive(Debug, Clone)]
pub struct SessionRequest {
    pub source_drive: PathBuf,
    pub client_name: String,
    pub event_name: String,
    pub camera_label_override: Option<String>,
    pub base_path: PathBuf,
    pub scan: ScanSummary,
}

#[derive(Debug, Clone)]
pub struct PreflightResult {
    pub destination_root: PathBuf,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct IngestProgress {
    pub message: String,
    pub files_done: usize,
    pub total_files: usize,
    pub bytes_done: u64,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRecordSummary {
    pub filename: String,
    pub relative_path: String,
    pub size_bytes: u64,
    pub xxh3_hash: Option<String>,
    pub source_modified: String,
    pub status: FileStatus,
}

#[derive(Debug, Clone)]
pub struct DuplicateRecord {
    pub event_name: String,
    pub status: FileStatus,
    pub xxh3_hash: Option<String>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct StoredFileRecord {
    pub dest_path: PathBuf,
    pub status: FileStatus,
    pub hash: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FinalReport {
    pub destination_root: PathBuf,
    pub total_files: usize,
    pub verified_files: usize,
    pub failed_files: usize,
    pub locked: bool,
    pub warnings: Vec<String>,
    pub elapsed: Duration,
}

#[derive(Debug, Clone)]
pub struct ResumableEvent {
    pub client_name: String,
    pub event_name: String,
    pub status: EventStatus,
    pub verified_files: usize,
    pub total_files: usize,
    pub destination_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct HistoryEvent {
    pub client_name: String,
    pub event_name: String,
    pub status: EventStatus,
    pub verified_files: usize,
    pub total_files: usize,
    pub locked: bool,
    pub completed_at: Option<String>,
    pub destination_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct ClientSummary {
    pub name: String,
    pub base_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct EventOption {
    pub name: String,
    pub completed: bool,
}

impl FinalReport {
    pub fn to_text(&self) -> String {
        let mut text = format!(
            "Verified {}/{} files\nDestination: {}\nElapsed: {:?}\nLocked: {}\n",
            self.verified_files,
            self.total_files,
            self.destination_root.display(),
            self.elapsed,
            if self.locked { "yes" } else { "no" }
        );

        if self.failed_files > 0 {
            text.push_str(&format!("Failed files: {}\n", self.failed_files));
        }
        if !self.warnings.is_empty() {
            text.push_str("Warnings:\n");
            for warning in &self.warnings {
                text.push_str(&format!("- {warning}\n"));
            }
        }

        text
    }
}

pub fn validate_folder_name(value: &str, max_len: usize) -> Result<String, MoonError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(MoonError::InvalidClientName {
            reason: "value cannot be empty".into(),
        });
    }
    if trimmed.len() > max_len {
        return Err(MoonError::InvalidClientName {
            reason: format!("value must be {max_len} characters or fewer"),
        });
    }
    if trimmed
        .chars()
        .any(|c| matches!(c, '\\' | '/' | ':' | '*' | '?' | '"' | '<' | '>' | '|'))
    {
        return Err(MoonError::InvalidClientName {
            reason: "value contains Windows-invalid path characters".into(),
        });
    }
    Ok(trimmed.to_string())
}

pub fn enforce_path_limit(path: &Path) -> Result<(), MoonError> {
    let path_string = path.display().to_string();
    let length = path_string.chars().count();
    if length > PATH_SOFT_LIMIT {
        return Err(MoonError::PathTooLong {
            path: path_string,
            length,
            max: PATH_SOFT_LIMIT,
        });
    }
    Ok(())
}

pub fn next_event_suggestion(completed_events: &[String]) -> String {
    for event in EVENT_SEQUENCE {
        if !completed_events.iter().any(|item| item.eq_ignore_ascii_case(event)) {
            return event.to_string();
        }
    }
    "Custom...".to_string()
}

pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

impl fmt::Display for IngestProgress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} ({}/{}, {} / {})",
            self.message,
            self.files_done,
            self.total_files,
            format_bytes(self.bytes_done),
            format_bytes(self.total_bytes)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{enforce_path_limit, next_event_suggestion, validate_folder_name};
    use std::path::Path;

    #[test]
    fn validates_folder_names() {
        assert!(validate_folder_name("Ahmed Khan", 100).is_ok());
        assert!(validate_folder_name("bad:name", 100).is_err());
        assert!(validate_folder_name(" ", 100).is_err());
    }

    #[test]
    fn suggests_next_event() {
        assert_eq!(next_event_suggestion(&[]), "Ubtan");
        assert_eq!(
            next_event_suggestion(&["Ubtan".into(), "Mehndi".into()]),
            "Barat"
        );
        assert_eq!(
            next_event_suggestion(
                &["Ubtan".into(), "Mehndi".into(), "Barat".into(), "Walima".into(), "Portraits".into()]
            ),
            "Custom..."
        );
    }

    #[test]
    fn enforces_soft_path_limit() {
        let long_path = "C:\\".to_string() + &"a".repeat(241);
        assert!(enforce_path_limit(Path::new(&long_path)).is_err());
    }
}
