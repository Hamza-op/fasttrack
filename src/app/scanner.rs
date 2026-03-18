use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::time::SystemTime;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use walkdir::{DirEntry, WalkDir};

use crate::app::errors::MoonError;
use crate::app::metadata::detect_camera_metadata;
use crate::app::types::{MediaType, ScanSummary, ScannedFile};

const SKIP_DIRS: &[&str] = &["THMBNL", "MISC", "CANONMSC", "BACKUP"];
const SKIP_FILES: &[&str] = &["MEDIAPRO.XML", "STATUS.BIN", "AESSION.BIN", "INDEX.BDM"];
const SKIP_EXTS: &[&str] = &["xml", "bim", "dat", "ctg", "thm", "tid", "ind", "bdm", "cpi", "mpl"];

pub fn scan_drive(root: &Path, include_proxy_files: bool, skip_hidden: bool) -> Result<ScanSummary> {
    if !root.exists() {
        return Err(MoonError::NoMediaFound {
            drive: root.display().to_string(),
        }
        .into());
    }

    let manufacturer = detect_manufacturer(root);
    let manufacturer_profile = ManufacturerProfile::from_name(&manufacturer);
    let mut files = Vec::new();
    let mut total_size = 0u64;
    let mut skipped_count = 0usize;

    let walker = WalkDir::new(root).into_iter();
    for entry in walker.filter_entry(|entry| should_enter(entry, include_proxy_files)) {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };

        if !entry.file_type().is_file() {
            continue;
        }

        let path = entry.path();
        if skip_hidden && is_hidden_system(path) {
            skipped_count += 1;
            continue;
        }

        let Some(filename) = path.file_name().and_then(|value| value.to_str()) else {
            skipped_count += 1;
            continue;
        };

        if SKIP_FILES.iter().any(|value| value.eq_ignore_ascii_case(filename)) {
            skipped_count += 1;
            continue;
        }

        let ext = path
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .to_ascii_lowercase();

        if SKIP_EXTS.iter().any(|value| value == &ext) {
            skipped_count += 1;
            continue;
        }

        let Some(media_type) = MediaType::from_extension(&ext) else {
            skipped_count += 1;
            continue;
        };

        let relative_source_path = path
            .strip_prefix(root)
            .unwrap_or(path)
            .to_path_buf();

        if !manufacturer_profile.accepts_path(&relative_source_path, &ext, include_proxy_files) {
            skipped_count += 1;
            continue;
        }

        let metadata = fs::metadata(path).with_context(|| format!("unable to read {}", path.display()))?;
        if metadata.len() == 0 {
            skipped_count += 1;
            continue;
        }

        let camera = detect_camera_metadata(path);
        let source_modified = file_time_string(metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH));
        let source_created = file_time_string(
            metadata
                .created()
                .or_else(|_| metadata.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH),
        );

        files.push(ScannedFile {
            source_path: path.to_path_buf(),
            relative_source_path,
            filename: filename.to_string(),
            size_bytes: metadata.len(),
            source_modified,
            source_created,
            media_type,
            camera_model: camera.model,
            camera_serial: camera.serial_number,
        });
        total_size += metadata.len();
    }

    if files.is_empty() {
        return Err(MoonError::NoMediaFound {
            drive: root.display().to_string(),
        }
        .into());
    }

    let mut summary = ScanSummary {
        manufacturer,
        drive_root: root.to_path_buf(),
        card_label: Some(root.display().to_string()),
        total_size_bytes: total_size,
        total_files: files.len(),
        raw_count: 0,
        jpg_count: 0,
        video_count: 0,
        audio_count: 0,
        skipped_count,
        files,
    };

    for file in &summary.files {
        match file.media_type {
            MediaType::PhotoRaw => summary.raw_count += 1,
            MediaType::PhotoJpg => summary.jpg_count += 1,
            MediaType::Video => summary.video_count += 1,
            MediaType::Audio => summary.audio_count += 1,
        }
    }

    Ok(summary)
}

fn should_enter(entry: &DirEntry, include_proxy_files: bool) -> bool {
    if entry.depth() == 0 {
        return true;
    }

    let name = entry.file_name().to_string_lossy();
    if name.starts_with('.') {
        return false;
    }
    if !include_proxy_files && name.eq_ignore_ascii_case("SUB") {
        return false;
    }
    !SKIP_DIRS.iter().any(|value| name.eq_ignore_ascii_case(value))
}

pub fn detect_manufacturer(root: &Path) -> String {
    if root.join("PRIVATE").join("M4ROOT").exists() {
        return "Sony".into();
    }

    let dcim = root.join("DCIM");
    if dcim.exists() {
        let names: HashSet<String> = WalkDir::new(&dcim)
            .min_depth(1)
            .max_depth(2)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().to_ascii_uppercase())
            .collect();

        if names.iter().any(|name| name.contains("CANON")) {
            return "Canon".into();
        }
        if names.iter().any(|name| name.contains("NIKON")) {
            return "Nikon".into();
        }
        if names.iter().any(|name| name.contains("MSDCF")) {
            return "Sony".into();
        }
    }

    "Generic".into()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManufacturerProfile {
    Sony,
    Canon,
    Nikon,
    Generic,
}

impl ManufacturerProfile {
    fn from_name(value: &str) -> Self {
        if value.eq_ignore_ascii_case("Sony") {
            Self::Sony
        } else if value.eq_ignore_ascii_case("Canon") {
            Self::Canon
        } else if value.eq_ignore_ascii_case("Nikon") {
            Self::Nikon
        } else {
            Self::Generic
        }
    }

    fn accepts_path(self, relative_path: &Path, ext: &str, include_proxy_files: bool) -> bool {
        if matches!(self, Self::Generic) {
            return true;
        }
        let parts_upper = relative_path
            .components()
            .map(|component| component.as_os_str().to_string_lossy().to_ascii_uppercase())
            .collect::<Vec<_>>();
        let root = parts_upper.first().map(|value| value.as_str()).unwrap_or_default();

        match self {
            Self::Sony => {
                if root == "DCIM" {
                    return true;
                }
                if root == "MP_ROOT" {
                    return Self::is_video_extension(ext);
                }
                if root == "PRIVATE" {
                    if parts_upper.iter().any(|part| part == "SUB") && !include_proxy_files {
                        return false;
                    }
                    if parts_upper.iter().any(|part| part == "CLIP" || part == "STREAM") {
                        return Self::is_video_extension(ext);
                    }
                    return true;
                }
                false
            }
            Self::Canon => {
                if root == "DCIM" {
                    return true;
                }
                if root == "PRIVATE"
                    && parts_upper.len() >= 4
                    && parts_upper[1] == "AVCHD"
                    && parts_upper[2] == "BDMV"
                    && parts_upper[3] == "STREAM"
                {
                    return Self::is_video_extension(ext);
                }
                if root == "CONTENTS" && parts_upper.iter().any(|part| part.starts_with("CLIPS")) {
                    return Self::is_video_extension(ext);
                }
                if root == "XFROOT" {
                    return Self::is_video_extension(ext);
                }
                false
            }
            Self::Nikon => {
                root == "DCIM"
            }
            Self::Generic => true,
        }
    }

    fn is_video_extension(ext: &str) -> bool {
        matches!(
            ext,
            "mp4" | "mov" | "mxf" | "avi" | "mts" | "m2ts" | "mpg" | "mpeg" | "crm" | "m4v"
                | "3gp" | "3g2"
        )
    }
}

fn file_time_string(value: SystemTime) -> String {
    let date_time: DateTime<Utc> = value.into();
    date_time.to_rfc3339()
}

#[cfg(windows)]
fn is_hidden_system(path: &Path) -> bool {
    use std::os::windows::fs::MetadataExt;

    fs::metadata(path)
        .map(|metadata| {
            let attrs = metadata.file_attributes();
            (attrs & 0x2 != 0) || (attrs & 0x4 != 0)
        })
        .unwrap_or(false)
}

#[cfg(not(windows))]
fn is_hidden_system(path: &Path) -> bool {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(|value| value.starts_with('.'))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{detect_manufacturer, scan_drive};

    #[test]
    fn detects_manufacturer_from_layout() {
        let temp = tempdir().unwrap();
        fs::create_dir_all(temp.path().join("PRIVATE").join("M4ROOT").join("CLIP")).unwrap();
        assert_eq!(detect_manufacturer(temp.path()), "Sony");
    }

    #[test]
    fn scans_supported_media_and_skips_sidecars() {
        let temp = tempdir().unwrap();
        let clip = temp.path().join("DCIM").join("100MSDCF");
        fs::create_dir_all(&clip).unwrap();
        fs::write(clip.join("A001.ARW"), b"raw").unwrap();
        fs::write(clip.join("A001.XML"), b"sidecar").unwrap();
        let summary = scan_drive(temp.path(), false, false).unwrap();
        assert_eq!(summary.total_files, 1);
        assert_eq!(summary.raw_count, 1);
        assert_eq!(summary.skipped_count, 1);
    }

    #[test]
    fn sony_profile_skips_proxy_by_default() {
        let temp = tempdir().unwrap();
        let clip = temp.path().join("PRIVATE").join("M4ROOT").join("CLIP");
        let sub = temp.path().join("PRIVATE").join("M4ROOT").join("SUB");
        fs::create_dir_all(&clip).unwrap();
        fs::create_dir_all(&sub).unwrap();
        fs::write(clip.join("C0001.MP4"), b"video").unwrap();
        fs::write(sub.join("C0001.MP4"), b"proxy").unwrap();

        let summary = scan_drive(temp.path(), false, false).unwrap();
        assert_eq!(summary.video_count, 1);
        assert_eq!(summary.total_files, 1);
    }

    #[test]
    fn canon_profile_skips_non_canon_dcim_folders() {
        let temp = tempdir().unwrap();
        let canon = temp.path().join("DCIM").join("100CANON");
        let other = temp.path().join("DCIM").join("200MISCX");
        fs::create_dir_all(&canon).unwrap();
        fs::create_dir_all(&other).unwrap();
        fs::write(canon.join("A001.CR3"), b"raw").unwrap();
        fs::write(other.join("A001.CR3"), b"raw").unwrap();

        let summary = scan_drive(temp.path(), false, false).unwrap();
        assert_eq!(summary.total_files, 1);
    }
}
