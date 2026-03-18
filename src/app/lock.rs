use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;
#[cfg(windows)]
use windows::core::PCWSTR;
#[cfg(windows)]
use windows::Win32::Foundation::{ERROR_SUCCESS, HLOCAL, LocalFree};
#[cfg(windows)]
use windows::Win32::Security::Authorization::{
    BuildTrusteeWithSidW, GetExplicitEntriesFromAclW, GetNamedSecurityInfoW, GetTrusteeFormW,
    GetTrusteeNameW, SetEntriesInAclW, SetNamedSecurityInfoW, DENY_ACCESS, EXPLICIT_ACCESS_W,
    SE_FILE_OBJECT, TRUSTEE_IS_SID,
};
#[cfg(windows)]
use windows::Win32::Security::{
    CreateWellKnownSid, ACL, ACE_FLAGS, DACL_SECURITY_INFORMATION, IsWellKnownSid,
    PSECURITY_DESCRIPTOR, PSID, SECURITY_MAX_SID_SIZE, WinWorldSid,
};
#[cfg(windows)]
use windows::Win32::Storage::FileSystem::{
    SetFileAttributesW, DELETE, FILE_ATTRIBUTE_HIDDEN, FILE_ATTRIBUTE_READONLY,
    FILE_ATTRIBUTE_SYSTEM, FILE_DELETE_CHILD, FILE_FLAGS_AND_ATTRIBUTES,
};

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
        if let Err(error) = set_readonly_state(&entry, true) {
            errors.push((entry, error));
        }
    }

    errors
}

pub fn clear_readonly_recursive(root: &Path) -> Vec<(PathBuf, MoonError)> {
    let mut errors = Vec::new();
    let mut entries: Vec<PathBuf> = walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path().to_path_buf())
        .collect();
    entries.sort_by_key(|path| path.components().count());

    for entry in entries {
        if let Err(error) = set_readonly_state(&entry, false) {
            errors.push((entry, error));
        }
    }

    errors
}

fn set_readonly_state(path: &Path, readonly: bool) -> Result<(), MoonError> {
    let metadata = fs::metadata(path).map_err(|source| MoonError::AttributeError {
        path: path.to_path_buf(),
        source,
    })?;
    let mut permissions = metadata.permissions();
    permissions.set_readonly(readonly);
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

#[cfg(windows)]
pub fn prepare_manifest_for_write(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }

    let metadata = fs::metadata(path)?;
    let mut permissions = metadata.permissions();
    permissions.set_readonly(false);
    fs::set_permissions(path, permissions)?;

    let attrs = metadata.file_attributes()
        & !(FILE_ATTRIBUTE_READONLY.0 | FILE_ATTRIBUTE_HIDDEN.0 | FILE_ATTRIBUTE_SYSTEM.0);
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
pub fn prepare_manifest_for_write(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(windows)]
pub fn mark_folder_system(path: &Path) -> Result<()> {
    let metadata = fs::metadata(path)?;
    let attrs = metadata.file_attributes() | FILE_ATTRIBUTE_SYSTEM.0;
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
pub fn mark_folder_system(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(windows)]
pub fn protect_folder_from_delete(path: &Path) -> Result<()> {
    let wide: Vec<u16> = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let mut security_descriptor = PSECURITY_DESCRIPTOR::default();
    let mut existing_dacl: *mut ACL = std::ptr::null_mut();
    let mut new_dacl: *mut ACL = std::ptr::null_mut();

    let result = (|| -> Result<()> {
        let mut sid_size = SECURITY_MAX_SID_SIZE;
        let mut world_sid = vec![0u8; sid_size as usize];
        let world_sid_ptr = PSID(world_sid.as_mut_ptr().cast());
        unsafe {
            CreateWellKnownSid(WinWorldSid, None, Some(world_sid_ptr), &mut sid_size)
                .context("failed to build Everyone SID for delete protection")?;
        }

        let mut trustee = Default::default();
        unsafe {
            BuildTrusteeWithSidW(&mut trustee, Some(world_sid_ptr));
        }

        let deny_delete = [EXPLICIT_ACCESS_W {
            grfAccessPermissions: DELETE.0 | FILE_DELETE_CHILD.0,
            grfAccessMode: DENY_ACCESS,
            grfInheritance: ACE_FLAGS(0),
            Trustee: trustee,
        }];

        let security_status = unsafe {
            GetNamedSecurityInfoW(
                PCWSTR(wide.as_ptr()),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                None,
                None,
                Some(&mut existing_dacl),
                None,
                &mut security_descriptor,
            )
        };
        ensure_win32_success(
            security_status,
            &format!(
                "failed to read current delete protection ACL for {}",
                path.display()
            ),
        )?;

        if delete_protection_present(existing_dacl) {
            return Ok(());
        }

        let acl_status =
            unsafe { SetEntriesInAclW(Some(&deny_delete), Some(existing_dacl), &mut new_dacl) };
        ensure_win32_success(
            acl_status,
            &format!(
                "failed to build delete protection ACL for {}",
                path.display()
            ),
        )?;

        let apply_status = unsafe {
            SetNamedSecurityInfoW(
                PCWSTR(wide.as_ptr()),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                None,
                None,
                Some(new_dacl.cast_const()),
                None,
            )
        };
        ensure_win32_success(
            apply_status,
            &format!(
                "failed to apply delete protection ACL to {}",
                path.display()
            ),
        )?;

        Ok(())
    })();

    unsafe {
        if !new_dacl.is_null() {
            let _ = LocalFree(Some(HLOCAL(new_dacl.cast())));
        }
        if !security_descriptor.0.is_null() {
            let _ = LocalFree(Some(HLOCAL(security_descriptor.0)));
        }
    }

    result
}

#[cfg(not(windows))]
pub fn protect_folder_from_delete(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(windows)]
fn ensure_win32_success(status: windows::Win32::Foundation::WIN32_ERROR, context: &str) -> Result<()> {
    if status == ERROR_SUCCESS {
        return Ok(());
    }

    anyhow::bail!("{context} (win32: {})", status.0);
}

#[cfg(windows)]
fn delete_protection_present(existing_dacl: *mut ACL) -> bool {
    if existing_dacl.is_null() {
        return false;
    }

    let mut entry_count = 0u32;
    let mut entries: *mut EXPLICIT_ACCESS_W = std::ptr::null_mut();
    let status = unsafe { GetExplicitEntriesFromAclW(existing_dacl, &mut entry_count, &mut entries) };
    if status != ERROR_SUCCESS || entries.is_null() {
        return false;
    }

    let result = unsafe {
        std::slice::from_raw_parts(entries, entry_count as usize)
            .iter()
            .any(|entry| {
                if entry.grfAccessMode != DENY_ACCESS {
                    return false;
                }

                let delete_mask = DELETE.0 | FILE_DELETE_CHILD.0;
                if entry.grfAccessPermissions & delete_mask != delete_mask {
                    return false;
                }

                if GetTrusteeFormW(&entry.Trustee) != TRUSTEE_IS_SID {
                    return false;
                }

                let trustee_name = GetTrusteeNameW(&entry.Trustee);
                if trustee_name.0.is_null() {
                    return false;
                }

                IsWellKnownSid(PSID(trustee_name.0.cast()), WinWorldSid).as_bool()
            })
    };

    unsafe {
        let _ = LocalFree(Some(HLOCAL(entries.cast())));
    }

    result
}
