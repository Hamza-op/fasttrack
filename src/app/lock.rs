use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;

#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;
#[cfg(windows)]
use windows::core::PCWSTR;
#[cfg(windows)]
use windows::Win32::Storage::FileSystem::{SetFileAttributesW, FILE_FLAGS_AND_ATTRIBUTES};

use crate::app::errors::MoonError;

pub fn apply_readonly_recursive(root: &Path) -> Vec<(PathBuf, MoonError)> {
    let mut errors = Vec::new();
    let mut entries: Vec<PathBuf> = walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path().to_path_buf())
        .collect();
    entries.sort_by_key(|path| std::cmp::Reverse(path.components().count()));

    for entry in entries {
        if let Err(error) = set_readonly(&entry) {
            errors.push((entry, error));
        }
    }

    errors
}

fn set_readonly(path: &Path) -> Result<(), MoonError> {
    let metadata = fs::metadata(path).map_err(|source| MoonError::AttributeError {
        path: path.to_path_buf(),
        source,
    })?;
    let mut permissions = metadata.permissions();
    permissions.set_readonly(true);
    fs::set_permissions(path, permissions).map_err(|source| MoonError::AttributeError {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(())
}

#[cfg(windows)]
pub fn hide_manifest(path: &Path) -> Result<()> {
    let metadata = fs::metadata(path)?;
    let attrs = metadata.file_attributes() | 0x2 | 0x4;
    let wide: Vec<u16> = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    unsafe {
        SetFileAttributesW(PCWSTR(wide.as_ptr()), FILE_FLAGS_AND_ATTRIBUTES(attrs))?;
    }
    Ok(())
}

#[cfg(not(windows))]
pub fn hide_manifest(_path: &Path) -> Result<()> {
    Ok(())
}

