use std::path::PathBuf;

use anyhow::Result;
use fs4::available_space;

#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
#[cfg(windows)]
use windows::core::PCWSTR;
#[cfg(windows)]
use windows::Win32::Storage::FileSystem::{GetDriveTypeW, GetLogicalDrives};

const DRIVE_REMOVABLE: u32 = 2;
const DRIVE_FIXED: u32 = 3;

#[derive(Debug, Clone)]
pub struct DetectedDrive {
    pub root: PathBuf,
    pub label: String,
    pub free_bytes: u64,
    pub is_removable: bool,
}

pub fn detect_drives() -> Result<Vec<DetectedDrive>> {
    #[cfg(windows)]
    {
        let bitmask = unsafe { GetLogicalDrives() };
        let mut drives = Vec::new();

        for index in 0..26 {
            if bitmask & (1 << index) == 0 {
                continue;
            }
            let drive_letter = (b'A' + index as u8) as char;
            let root = PathBuf::from(format!("{drive_letter}:\\"));
            let wide: Vec<u16> = root
                .as_os_str()
                .encode_wide()
                .chain(std::iter::once(0))
                .collect();
            let drive_type = unsafe { GetDriveTypeW(PCWSTR(wide.as_ptr())) };
            let has_camera_signature = root.join("DCIM").exists() || root.join("PRIVATE").exists();
            if drive_type != DRIVE_REMOVABLE && (drive_type != DRIVE_FIXED || !has_camera_signature)
            {
                continue;
            }

            drives.push(DetectedDrive {
                free_bytes: available_space(&root).unwrap_or_default(),
                is_removable: drive_type == DRIVE_REMOVABLE,
                label: root.display().to_string(),
                root,
            });
        }

        Ok(drives)
    }

    #[cfg(target_os = "macos")]
    {
        let mut drives = Vec::new();
        if let Ok(entries) = std::fs::read_dir("/Volumes") {
            for entry in entries.flatten() {
                let root = entry.path();
                let label = root
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string();
                drives.push(DetectedDrive {
                    free_bytes: available_space(&root).unwrap_or_default(),
                    is_removable: true,
                    label: root.display().to_string(),
                    root,
                });
            }
        }
        Ok(drives)
    }

    #[cfg(target_os = "linux")]
    {
        let mut drives = Vec::new();
        let user = whoami::username();
        let paths_to_check = vec![
            PathBuf::from(format!("/media/{}", user)),
            PathBuf::from(format!("/run/media/{}", user)),
            PathBuf::from("/mnt".to_string()),
        ];

        for base in paths_to_check {
            if let Ok(entries) = std::fs::read_dir(&base) {
                for entry in entries.flatten() {
                    let root = entry.path();
                    let label = root
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_string();
                    drives.push(DetectedDrive {
                        free_bytes: available_space(&root).unwrap_or_default(),
                        is_removable: true,
                        label: root.display().to_string(),
                        root,
                    });
                }
            }
        }
        Ok(drives)
    }

    #[cfg(not(any(windows, target_os = "macos", target_os = "linux")))]
    {
        Ok(Vec::new())
    }
}
