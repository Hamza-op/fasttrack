use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::Utc;
use fs4::available_space;
use serde::{Deserialize, Serialize};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::{info, warn};
use xxhash_rust::xxh3::Xxh3;

use crate::app::db::Database;
use crate::app::errors::MoonError;
use crate::app::lock::{
    apply_readonly_recursive, clear_readonly_recursive, hide_manifest, mark_folder_system,
    prepare_manifest_for_write, protect_folder_from_delete,
};
use crate::app::manifest::{write_manifest, Manifest};
use crate::app::settings::{AppPaths, Settings};
use crate::app::types::{
    enforce_path_limit, validate_folder_name, EventStatus, FileStatus, FinalReport, IngestProgress,
    LockMode, MediaType, PreflightResult, ScannedFile, SessionRequest, StoredFileRecord,
};

const MIN_COPY_BUFFER_BYTES: usize = 256 * 1024;
const PROGRESS_EMIT_MIN_BYTES: u64 = 8 * 1024 * 1024;
const PROGRESS_EMIT_MIN_INTERVAL_MS: u128 = 180;
const DUPLICATE_HASH_WINDOW_BYTES: u64 = 64 * 1024;

#[derive(Clone)]
pub struct IngestEngine {
    db: Database,
    settings: Settings,
    paths: AppPaths,
}

impl IngestEngine {
    pub fn new(db: Database, settings: Settings, paths: AppPaths) -> Self {
        Self {
            db,
            settings,
            paths,
        }
    }

    pub async fn run_session<F>(
        &self,
        request: SessionRequest,
        cancel_flag: Arc<AtomicBool>,
        mut on_progress: F,
    ) -> Result<FinalReport>
    where
        F: FnMut(IngestProgress) + Send,
    {
        self.db.backup_database()?;
        let started = Instant::now();
        let camera_folder = self.camera_folder_label(&request)?;
        let split_photo_folders = self.should_split_photo_folders(&request);
        let preflight = self.preflight(&request, camera_folder.as_deref(), split_photo_folders)?;
        let _session_lock = self.acquire_session_lock(&request, &preflight.destination_root)?;
        let client_id = self
            .db
            .upsert_client(&request.client_name, &request.base_path, None)?;
        self.db.remember_custom_event(&request.event_name)?;

        let event_id =
            self.db
                .prepare_event(client_id, &request.event_name, &preflight.destination_root)?;
        let existing = self.db.load_event_files(event_id)?;

        let total_bytes = request.scan.total_size_bytes;
        let total_files = request.scan.files.len();
        let mut bytes_done = 0u64;
        let mut verified_files = 0usize;
        let mut failed_files = 0usize;
        let mut skipped_files = 0usize;
        let mut warnings = preflight.warnings;
        if !existing.is_empty() {
            warnings.push(format!(
                "Resuming existing ingest with {} tracked files at {}.",
                existing.len(),
                preflight.destination_root.display()
            ));
        }

        let mut ordered_files = request.scan.files.clone();
        ordered_files.sort_by(|left, right| {
            copy_priority(left)
                .cmp(&copy_priority(right))
                .then_with(|| left.relative_source_path.cmp(&right.relative_source_path))
        });

        for file in &ordered_files {
            if cancel_flag.load(Ordering::Relaxed) {
                self.db.mark_event(
                    event_id,
                    EventStatus::Partial,
                    &preflight.destination_root,
                    verified_files,
                    failed_files,
                    total_files,
                    total_bytes,
                    false,
                )?;
                return Err(MoonError::Cancelled {
                    copied: verified_files as u32,
                    total: total_files as u32,
                }
                .into());
            }

            let tracked_record = existing.get(&file.source_path);
            let reuse_tracked_destination = tracked_record
                .is_some_and(|record| tracked_record_matches_file(record, file));
            if let Some(record) = tracked_record {
                if matches!(record.status, FileStatus::Verified) && record.dest_path.exists() {
                    if reuse_tracked_destination {
                        verified_files += 1;
                        bytes_done += file.size_bytes;
                        on_progress(IngestProgress {
                            message: format!("Skipping already verified {}", file.filename),
                            files_done: verified_files + failed_files + skipped_files,
                            total_files,
                            bytes_done,
                            total_bytes,
                        });
                        continue;
                    }

                    warnings.push(format!(
                        "Tracked file {} changed since the previous ingest record. Copying it as a new file.",
                        file.filename
                    ));
                }
            }

            let destination = self.resolve_destination(
                &preflight.destination_root,
                file,
                &existing,
                reuse_tracked_destination,
                camera_folder.as_deref(),
                split_photo_folders,
            )?;
            self.db
                .ensure_file_row(event_id, file, &destination, FileStatus::Pending)?;

            let duplicate_candidates = self
                .db
                .find_duplicate_candidates_for_client(&request.client_name, file)?;
            if !duplicate_candidates.is_empty() {
                let mut source_edge_hash: Option<String> = None;
                let mut matched_duplicate: Option<crate::app::types::DuplicateRecord> = None;

                for duplicate in duplicate_candidates {
                    let same_event = duplicate
                        .event_name
                        .eq_ignore_ascii_case(&request.event_name);
                    if !same_event && !request.skip_already_copied {
                        continue;
                    }
                    if !matches!(duplicate.status, FileStatus::Verified) {
                        continue;
                    }
                    if !duplicate.dest_path.exists() {
                        warnings.push(format!(
                            "Duplicate record for {} ignored because destination file is missing.",
                            file.filename
                        ));
                        continue;
                    }

                    let source_hash = match &source_edge_hash {
                        Some(hash) => hash.clone(),
                        None => match hash_file_partial(&file.source_path).await {
                            Ok(hash) => {
                                source_edge_hash = Some(hash.clone());
                                hash
                            }
                            Err(error) => {
                                warn!(
                                    "duplicate source hash failed for {}: {error:#}",
                                    file.source_path.display()
                                );
                                warnings.push(format!(
                                    "Could not confirm duplicate for {} because source file could not be read: {}",
                                    file.filename, error
                                ));
                                continue;
                            }
                        },
                    };
                    let duplicate_hash = match hash_file_partial(&duplicate.dest_path).await {
                        Ok(hash) => hash,
                        Err(error) => {
                            warn!(
                                "duplicate destination hash failed for {}: {error:#}",
                                duplicate.dest_path.display()
                            );
                            warnings.push(format!(
                                "Duplicate record for {} ignored because previous copy could not be read at {}: {}",
                                file.filename,
                                duplicate.dest_path.display(),
                                error
                            ));
                            continue;
                        }
                    };
                    if source_hash == duplicate_hash {
                        matched_duplicate = Some(duplicate);
                        break;
                    }
                }

                if let Some(duplicate) = matched_duplicate {
                    self.db.update_file_status(
                        event_id,
                        &file.source_path,
                        &destination,
                        FileStatus::SkippedDuplicate,
                        duplicate.xxh3_hash.as_deref(),
                        Some("skipped duplicate"),
                    )?;
                    warnings.push(format!("Skipped duplicate {}", file.filename));
                    bytes_done += file.size_bytes;
                    skipped_files += 1;
                    on_progress(IngestProgress {
                        message: format!("Skipping duplicate {}", file.filename),
                        files_done: verified_files + failed_files + skipped_files,
                        total_files,
                        bytes_done,
                        total_bytes,
                    });
                    continue;
                }
            }

            self.db.update_file_status(
                event_id,
                &file.source_path,
                &destination,
                FileStatus::Copying,
                None,
                None,
            )?;

            on_progress(IngestProgress {
                message: format!("Copying {}", file.filename),
                files_done: verified_files + failed_files + skipped_files,
                total_files,
                bytes_done,
                total_bytes,
            });

            match copy_with_verification(
                file,
                &destination,
                self.settings
                    .ingest
                    .buffer_size_kb
                    .saturating_mul(1024)
                    .max(MIN_COPY_BUFFER_BYTES),
                self.settings.ingest.verify_after_copy,
                cancel_flag.clone(),
                |file_done| {
                    on_progress(IngestProgress {
                        message: format!(
                            "Copying {} ({})",
                            file.filename,
                            crate::app::types::format_bytes(file_done)
                        ),
                        files_done: verified_files + failed_files + skipped_files,
                        total_files,
                        bytes_done: bytes_done + file_done,
                        total_bytes,
                    });
                },
            )
            .await
            {
                Ok(hash) => {
                    if self.settings.ingest.verify_after_copy {
                        self.db.update_file_status(
                            event_id,
                            &file.source_path,
                            &destination,
                            FileStatus::Verifying,
                            None,
                            None,
                        )?;
                    }
                    self.db.update_file_status(
                        event_id,
                        &file.source_path,
                        &destination,
                        FileStatus::Verified,
                        Some(&hash),
                        None,
                    )?;
                    verified_files += 1;
                    bytes_done += file.size_bytes;
                }
                Err(error) => {
                    if error
                        .downcast_ref::<MoonError>()
                        .is_some_and(|value| matches!(value, MoonError::Cancelled { .. }))
                    {
                        self.db.mark_event(
                            event_id,
                            EventStatus::Partial,
                            &preflight.destination_root,
                            verified_files,
                            failed_files,
                            total_files,
                            total_bytes,
                            false,
                        )?;
                        return Err(MoonError::Cancelled {
                            copied: verified_files as u32,
                            total: total_files as u32,
                        }
                        .into());
                    }
                    warn!("copy failed for {}: {error:#}", file.filename);
                    self.db.update_file_status(
                        event_id,
                        &file.source_path,
                        &destination,
                        FileStatus::Failed,
                        None,
                        Some(&error.to_string()),
                    )?;
                    failed_files += 1;
                    warnings.push(format!("Failed {}: {}", file.filename, error));
                }
            }
        }

        let mut locked = false;
        if failed_files == 0 && self.settings.safety.auto_lock_on_complete {
            match self.settings.safety.lock_mode {
                LockMode::None => {}
                LockMode::Attribute | LockMode::Acl => {
                    locked = apply_event_lock(&preflight.destination_root, &mut warnings);
                }
            }
        }

        let manifest_records = self
            .db
            .manifest_records(event_id, &preflight.destination_root)?;
        let manifest = Manifest::from_records(
            &request.client_name,
            &request.event_name,
            &request.source_drive,
            request
                .scan
                .card_label
                .as_deref()
                .unwrap_or(request.source_drive.to_string_lossy().as_ref()),
            manifest_records,
            locked,
        );
        let manifest_path = preflight.destination_root.join(".mooningest_manifest.json");
        prepare_manifest_for_write(&manifest_path).with_context(|| {
            format!(
                "Could not reopen manifest at '{}'.",
                manifest_path.display()
            )
        })?;
        let manifest_path = write_manifest(&preflight.destination_root, &manifest).with_context(|| {
            format!(
                "Could not write manifest in '{}'.",
                preflight.destination_root.display()
            )
        })?;
        let _ = hide_manifest(&manifest_path);
        info!("wrote manifest {}", manifest_path.display());

        let status = if failed_files > 0 {
            EventStatus::Failed
        } else {
            EventStatus::Completed
        };
        self.db.mark_event(
            event_id,
            status,
            &preflight.destination_root,
            verified_files,
            failed_files,
            total_files,
            total_bytes,
            locked,
        )?;
        self.db.record_audit(
            "ingest_completed",
            &format!(
                "{} / {} -> {}",
                request.source_drive.display(),
                request.client_name,
                preflight.destination_root.display()
            ),
        )?;

        Ok(FinalReport {
            destination_root: preflight.destination_root,
            total_files,
            verified_files,
            failed_files,
            locked,
            warnings,
            elapsed: started.elapsed(),
        })
    }

    fn preflight(
        &self,
        request: &SessionRequest,
        camera_folder: Option<&str>,
        split_photo_folders: bool,
    ) -> Result<PreflightResult> {
        validate_folder_name(&request.client_name, 100)?;
        validate_folder_name(&request.event_name, 100)?;
        let mut warnings = Vec::new();

        let destination_root = request
            .base_path
            .join(&request.client_name)
            .join(&request.event_name);
        enforce_path_limit(&destination_root)?;

        if !request.source_drive.exists() {
            return Err(MoonError::NoMediaFound {
                drive: request.source_drive.display().to_string(),
            }
            .into());
        }
        if !request.source_drive.is_dir() {
            return Err(anyhow::anyhow!(
                "Source '{}' is not a readable directory.",
                request.source_drive.display()
            ));
        }
        let _ = std::fs::read_dir(&request.source_drive).with_context(|| {
            format!(
                "Source '{}' is not readable.",
                request.source_drive.display()
            )
        })?;

        std::fs::create_dir_all(&destination_root).with_context(|| {
            format!(
                "Could not prepare destination folder '{}'.",
                destination_root.display()
            )
        })?;

        if let Some(existing_event) = self
            .db
            .latest_event_for_client_event(&request.client_name, &request.event_name)?
        {
            if existing_event.locked
                && same_path(&existing_event.destination_path, &destination_root)
                && destination_root.exists()
            {
                let unlock_errors = clear_readonly_recursive(&destination_root);
                if unlock_errors.is_empty() {
                    warnings.push(
                        "Existing locked event folder was reopened so new files can be added."
                            .into(),
                    );
                } else {
                    for (path, error) in unlock_errors {
                        warnings.push(format!(
                            "Could not fully reopen {} before ingest: {}",
                            path.display(),
                            error
                        ));
                    }
                }
            }
        }

        let probe = destination_root.join(".moon_write_probe.tmp");
        std::fs::write(&probe, b"probe").with_context(|| {
            format!(
                "Destination '{}' is not writable.",
                destination_root.display()
            )
        })?;
        let _ = std::fs::remove_file(&probe);

        let required = (request.scan.total_size_bytes as f64 * 1.05) as u64;
        let available = available_space(&destination_root)
            .or_else(|_| available_space(&request.base_path))
            .unwrap_or_default();
        if available < required {
            return Err(MoonError::DiskFull {
                needed: required,
                available,
            }
            .into());
        }

        self.db.check_integrity()?;

        for file in &request.scan.files {
            let (media_root, maybe_subdir) =
                Self::route_segments_for_file(file, split_photo_folders);
            let mut planned = destination_root.join(media_root);
            if let Some(camera_folder) = camera_folder {
                planned = planned.join(camera_folder);
            }
            if let Some(subdir) = maybe_subdir {
                planned = planned.join(subdir);
            }
            planned = planned.join(&file.filename);
            enforce_path_limit(&planned)?;
        }

        if self.settings.safety.auto_lock_on_complete {
            match self.settings.safety.lock_mode {
                LockMode::None => warnings.push(
                    "Auto-lock is enabled but lock mode is set to none; destination will remain writable."
                        .to_string(),
                ),
                LockMode::Attribute | LockMode::Acl => warnings.push(
                    "Event folder will be locked after verification and warn on delete.".into(),
                ),
            }
        } else {
            warnings.push(
                "Auto-lock is disabled; destination will remain writable after ingest.".into(),
            );
        }
        if request.skip_already_copied {
            warnings.push(
                "Previously copied files for this client will be skipped when matches are found."
                    .into(),
            );
        }

        Ok(PreflightResult {
            destination_root,
            warnings,
        })
    }

    fn resolve_destination(
        &self,
        event_root: &Path,
        file: &ScannedFile,
        existing: &HashMap<PathBuf, StoredFileRecord>,
        reuse_tracked_destination: bool,
        camera_folder: Option<&str>,
        split_photo_folders: bool,
    ) -> Result<PathBuf> {
        if reuse_tracked_destination {
            if let Some(record) = existing.get(&file.source_path) {
                return Ok(record.dest_path.clone());
            }
        }

        let (media_root, maybe_subdir) = Self::route_segments_for_file(file, split_photo_folders);

        let mut destination = event_root.join(media_root);
        if let Some(camera_folder) = camera_folder {
            destination = destination.join(camera_folder);
        }
        if let Some(subdir) = maybe_subdir {
            destination = destination.join(subdir);
        }
        std::fs::create_dir_all(&destination)?;

        let resolved = unique_destination(destination.join(&file.filename))?;
        enforce_path_limit(&resolved)?;
        Ok(resolved)
    }

    fn should_split_photo_folders(&self, request: &SessionRequest) -> bool {
        self.settings.ingest.separate_raw_jpg
            && request.scan.raw_count > 0
            && request.scan.jpg_count > 0
    }

    fn route_segments_for_file(
        file: &ScannedFile,
        split_photo_folders: bool,
    ) -> (&'static str, Option<&'static str>) {
        match file.media_type {
            MediaType::PhotoRaw => ("Photos", split_photo_folders.then_some("RAW")),
            MediaType::PhotoJpg => ("Photos", split_photo_folders.then_some("JPG")),
            MediaType::Video => ("Videos", None),
            MediaType::Audio => ("Audio", None),
        }
    }

    fn camera_folder_label(&self, request: &SessionRequest) -> Result<Option<String>> {
        if let Some(value) = request.camera_label_override.as_deref() {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            validate_folder_name(trimmed, 100)?;

            let same_client_event_exists = self
                .db
                .latest_event_for_client_event(&request.client_name, &request.event_name)?
                .is_some();
            if !same_client_event_exists {
                return Ok(None);
            }

            return Ok(Some(trimmed.to_string()));
        }
        Ok(None)
    }

    fn acquire_session_lock(
        &self,
        request: &SessionRequest,
        destination_root: &Path,
    ) -> Result<SessionLockGuard> {
        if self.paths.lock_file.exists() {
            let existing = std::fs::read_to_string(&self.paths.lock_file).unwrap_or_default();
            if let Ok(lock) = serde_json::from_str::<SessionLock>(&existing) {
                if !process_is_running(lock.pid) {
                    let _ = std::fs::remove_file(&self.paths.lock_file);
                } else {
                    return Err(anyhow::anyhow!(
                        "Another Moon Ingest session appears active (PID {}). Close it before starting a new ingest.",
                        lock.pid
                    ));
                }
            } else {
                // Malformed lock payload is treated as stale to avoid permanently blocking ingest.
                let _ = std::fs::remove_file(&self.paths.lock_file);
            }
        }

        let lock = SessionLock {
            pid: std::process::id(),
            client_name: request.client_name.clone(),
            event_name: request.event_name.clone(),
            destination_path: destination_root.display().to_string(),
            created_at: Utc::now().to_rfc3339(),
        };
        let payload = serde_json::to_string_pretty(&lock)?;
        std::fs::write(&self.paths.lock_file, payload).with_context(|| {
            format!(
                "Could not create session lock at '{}'.",
                self.paths.lock_file.display()
            )
        })?;
        Ok(SessionLockGuard {
            path: self.paths.lock_file.clone(),
        })
    }
}

fn apply_event_lock(destination_root: &Path, warnings: &mut Vec<String>) -> bool {
    let lock_errors = apply_readonly_recursive(destination_root);
    let mut locked = lock_errors.is_empty();

    for (path, error) in lock_errors {
        warnings.push(format!("Could not lock {}: {}", path.display(), error));
    }

    if let Err(error) = protect_folder_from_delete(destination_root) {
        warnings.push(format!(
            "Could not protect {} from delete operations: {}",
            destination_root.display(),
            error
        ));
        locked = false;
    }

    if let Err(error) = mark_folder_system(destination_root) {
        warnings.push(format!(
            "Could not mark lock warning on {}: {}",
            destination_root.display(),
            error
        ));
    }

    locked
}

#[derive(Debug, Serialize, Deserialize)]
struct SessionLock {
    pid: u32,
    client_name: String,
    event_name: String,
    destination_path: String,
    created_at: String,
}

fn process_is_running(pid: u32) -> bool {
    if pid == std::process::id() {
        return true;
    }
    #[cfg(windows)]
    {
        let output = Command::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
            .output();
        if let Ok(output) = output {
            let text = String::from_utf8_lossy(&output.stdout);
            for line in text.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with("INFO:") {
                    continue;
                }
                let mut fields = line.trim_matches('"').split("\",\"");
                let _image_name = fields.next();
                let pid_field = fields.next().unwrap_or_default();
                if pid_field.parse::<u32>().ok() == Some(pid) {
                    return true;
                }
            }
        }
        false
    }
    #[cfg(not(windows))]
    {
        Path::new(&format!("/proc/{pid}")).exists()
    }
}

struct SessionLockGuard {
    path: PathBuf,
}

impl Drop for SessionLockGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn unique_destination(path: PathBuf) -> Result<PathBuf> {
    if !path.exists() {
        return Ok(path);
    }

    let parent = path.parent().map(Path::to_path_buf).unwrap_or_default();
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("file");
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("");

    for index in 1..10000 {
        let candidate = if extension.is_empty() {
            parent.join(format!("{stem}_{index}"))
        } else {
            parent.join(format!("{stem}_{index}.{extension}"))
        };
        if !candidate.exists() {
            return Ok(candidate);
        }
    }

    Err(anyhow::anyhow!("Could not find a unique destination path for {}", path.display()))
}

fn copy_priority(file: &ScannedFile) -> u8 {
    match file.media_type {
        crate::app::types::MediaType::PhotoRaw => 0,
        crate::app::types::MediaType::PhotoJpg => 1,
        crate::app::types::MediaType::Video => 2,
        crate::app::types::MediaType::Audio => 3,
    }
}

async fn copy_with_verification<F>(
    file: &ScannedFile,
    destination: &Path,
    buffer_size: usize,
    verify_after_copy: bool,
    cancel_flag: Arc<AtomicBool>,
    mut on_chunk: F,
) -> Result<String>
where
    F: FnMut(u64),
{
    let mut last_error: Option<anyhow::Error> = None;
    let mut buffer = vec![0u8; buffer_size.max(MIN_COPY_BUFFER_BYTES)];

    for _attempt in 0..=2 {
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).await?;
        }
        if destination.exists() {
            let _ = fs::remove_file(destination).await;
        }

        let mut source =
            File::open(&file.source_path)
                .await
                .map_err(|source| MoonError::SourceReadError {
                    path: file.source_path.clone(),
                    source,
                })?;
        let mut dest =
            File::create(destination)
                .await
                .map_err(|source| MoonError::DestWriteError {
                    path: destination.to_path_buf(),
                    source,
                })?;

        let mut hasher = Xxh3::new();
        let mut total = 0u64;
        let mut emitted_total = 0u64;
        let mut last_emit = Instant::now();

        loop {
            if cancel_flag.load(Ordering::Relaxed) {
                return Err(MoonError::Cancelled {
                    copied: 0,
                    total: 0,
                }
                .into());
            }
            let read =
                source
                    .read(&mut buffer)
                    .await
                    .map_err(|source| MoonError::SourceReadError {
                        path: file.source_path.clone(),
                        source,
                    })?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            dest.write_all(&buffer[..read])
                .await
                .map_err(|source| MoonError::DestWriteError {
                    path: destination.to_path_buf(),
                    source,
                })?;
            total += read as u64;

            let now = Instant::now();
            let should_emit = total == file.size_bytes
                || total.saturating_sub(emitted_total) >= PROGRESS_EMIT_MIN_BYTES
                || now.duration_since(last_emit).as_millis() >= PROGRESS_EMIT_MIN_INTERVAL_MS;
            if should_emit {
                emitted_total = total;
                last_emit = now;
                on_chunk(total);
            }
        }
        if total != emitted_total {
            on_chunk(total);
        }
        dest.flush().await?;
        drop(dest);

        let source_hash = format!("{:032x}", hasher.digest128());
        if !verify_after_copy {
            return Ok(source_hash);
        }
        let dest_hash = hash_file(destination, buffer_size).await?;
        if source_hash == dest_hash {
            return Ok(source_hash);
        }

        last_error = Some(
            MoonError::ChecksumMismatch {
                filename: file.filename.clone(),
                source_hash,
                dest_hash,
            }
            .into(),
        );
    }

    Err(last_error.context("copy verification failed after retries")?)
}

async fn hash_file(path: &Path, buffer_size: usize) -> Result<String> {
    let mut file = File::open(path).await?;
    let mut hasher = Xxh3::new();
    let mut buffer = vec![0u8; buffer_size.max(MIN_COPY_BUFFER_BYTES)];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:032x}", hasher.digest128()))
}

fn tracked_record_matches_file(record: &StoredFileRecord, file: &ScannedFile) -> bool {
    record.size_bytes == file.size_bytes
        && record.source_created == file.source_created
        && record.source_modified == file.source_modified
        && record.media_type == file.media_type
}

fn same_path(left: &Path, right: &Path) -> bool {
    #[cfg(windows)]
    {
        left.display().to_string().eq_ignore_ascii_case(&right.display().to_string())
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

async fn hash_file_partial(path: &Path) -> Result<String> {
    let mut file = File::open(path).await?;
    let file_len = file.metadata().await?.len();
    let mut hasher = Xxh3::new();
    hasher.update(&file_len.to_le_bytes());

    if file_len <= DUPLICATE_HASH_WINDOW_BYTES * 2 {
        let mut buffer = vec![0u8; file_len as usize];
        if !buffer.is_empty() {
            file.read_exact(&mut buffer).await?;
            hasher.update(&buffer);
        }
        return Ok(format!("{:032x}", hasher.digest128()));
    }

    let mut head = vec![0u8; DUPLICATE_HASH_WINDOW_BYTES as usize];
    file.read_exact(&mut head).await?;
    hasher.update(&head);

    file.seek(SeekFrom::Start(
        file_len.saturating_sub(DUPLICATE_HASH_WINDOW_BYTES),
    ))
    .await?;
    let mut tail = vec![0u8; DUPLICATE_HASH_WINDOW_BYTES as usize];
    file.read_exact(&mut tail).await?;
    hasher.update(&tail);

    Ok(format!("{:032x}", hasher.digest128()))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::{atomic::AtomicBool, Arc};

    use tempfile::tempdir;
    use tokio::runtime::Builder;

    use crate::app::db::Database;
    use crate::app::manifest::Manifest;
    use crate::app::scanner::scan_drive;
    use crate::app::settings::{AppPaths, Settings};
    use crate::app::types::{LockMode, MediaType, ScanSummary, ScannedFile, SessionRequest};

    use super::IngestEngine;

    fn test_paths(root: &Path) -> AppPaths {
        let app_root = root.join("MoonIngest");
        AppPaths {
            root_dir: app_root.clone(),
            logs_dir: app_root.join("logs"),
            backups_dir: app_root.join("backups"),
            settings_file: app_root.join("settings.toml"),
            db_file: app_root.join("moon_ingest.db"),
            lock_file: app_root.join(".lock"),
        }
    }

    fn make_settings(base_path: &Path) -> Settings {
        let mut settings = Settings::default();
        settings.general.default_base_path = base_path.to_path_buf();
        settings.safety.auto_lock_on_complete = false;
        settings
    }

    fn make_scanned_file(path: &Path, media_type: MediaType) -> ScannedFile {
        let filename = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("file.bin")
            .to_string();
        ScannedFile {
            source_path: path.to_path_buf(),
            relative_source_path: PathBuf::from(&filename),
            filename,
            size_bytes: fs::metadata(path)
                .map(|metadata| metadata.len())
                .unwrap_or(0),
            source_modified: "2026-01-01T00:00:00Z".into(),
            source_created: "2026-01-01T00:00:00Z".into(),
            media_type,
            camera_model: Some("Sony A7IV".into()),
            camera_serial: Some("SN12345".into()),
        }
    }

    #[test]
    fn full_ingest_writes_manifest_and_completed_event() {
        let temp = tempdir().expect("temp dir");
        let source = temp.path().join("card");
        let base = temp.path().join("dest");
        fs::create_dir_all(source.join("DCIM").join("100MSDCF")).expect("create source");
        fs::create_dir_all(&base).expect("create base");
        fs::write(
            source.join("DCIM").join("100MSDCF").join("A001.ARW"),
            b"rawdata",
        )
        .expect("write raw");
        fs::write(
            source.join("DCIM").join("100MSDCF").join("A001.JPG"),
            b"jpgdata",
        )
        .expect("write jpg");

        let summary = scan_drive(&source, false, false).expect("scan");
        let paths = test_paths(temp.path());
        paths.ensure().expect("ensure paths");
        let db = Database::open(&paths).expect("db open");
        let settings = make_settings(&base);
        let engine = IngestEngine::new(db.clone(), settings, paths);

        let request = SessionRequest {
            source_drive: source.clone(),
            client_name: "Client One".into(),
            event_name: "Barat".into(),
            camera_label_override: Some("Cam A".into()),
            skip_already_copied: false,
            base_path: base.clone(),
            scan: summary.clone(),
        };

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let report = runtime
            .block_on(engine.run_session(request, Arc::new(AtomicBool::new(false)), |_| {}))
            .expect("run session");

        assert_eq!(report.failed_files, 0);
        assert_eq!(report.verified_files, summary.total_files);
        assert!(
            !report.locked,
            "auto-lock disabled should keep destination writable"
        );
        let manifest_path = base
            .join("Client One")
            .join("Barat")
            .join(".mooningest_manifest.json");
        assert!(manifest_path.exists());
        let raw = fs::read_to_string(&manifest_path).expect("read manifest");
        let manifest: Manifest = serde_json::from_str(&raw).expect("parse manifest");
        assert_eq!(manifest.total_files, summary.total_files);
        assert_eq!(manifest.verified_files, summary.total_files);

        let events = db.recent_events(1).expect("events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_name, "Barat");
    }

    #[test]
    fn locked_event_can_accept_more_files_with_attribute_lock() {
        let temp = tempdir().expect("temp dir");
        let source_one = temp.path().join("card_one");
        let source_two = temp.path().join("card_two");
        let base = temp.path().join("dest");
        fs::create_dir_all(&source_one).expect("create source one");
        fs::create_dir_all(&source_two).expect("create source two");
        fs::create_dir_all(&base).expect("create base");

        let src_one = source_one.join("A001.ARW");
        let src_two = source_two.join("A002.ARW");
        fs::write(&src_one, b"rawdata-one").expect("write first source file");
        fs::write(&src_two, b"rawdata-two").expect("write second source file");

        let file_one = make_scanned_file(&src_one, MediaType::PhotoRaw);
        let file_two = make_scanned_file(&src_two, MediaType::PhotoRaw);
        let summary_one = ScanSummary {
            manufacturer: "Sony".into(),
            drive_root: source_one.clone(),
            card_label: Some(source_one.display().to_string()),
            total_size_bytes: file_one.size_bytes,
            total_files: 1,
            raw_count: 1,
            jpg_count: 0,
            video_count: 0,
            audio_count: 0,
            skipped_count: 0,
            files: vec![file_one],
        };
        let summary_two = ScanSummary {
            manufacturer: "Sony".into(),
            drive_root: source_two.clone(),
            card_label: Some(source_two.display().to_string()),
            total_size_bytes: file_two.size_bytes,
            total_files: 1,
            raw_count: 1,
            jpg_count: 0,
            video_count: 0,
            audio_count: 0,
            skipped_count: 0,
            files: vec![file_two],
        };

        let paths = test_paths(temp.path());
        paths.ensure().expect("ensure paths");
        let db = Database::open(&paths).expect("db open");
        let mut settings = make_settings(&base);
        settings.safety.auto_lock_on_complete = true;
        settings.safety.lock_mode = LockMode::Attribute;
        let engine = IngestEngine::new(db, settings, paths);

        let request_one = SessionRequest {
            source_drive: source_one.clone(),
            client_name: "Client Lock".into(),
            event_name: "Barat".into(),
            camera_label_override: None,
            skip_already_copied: false,
            base_path: base.clone(),
            scan: summary_one,
        };
        let request_two = SessionRequest {
            source_drive: source_two.clone(),
            client_name: "Client Lock".into(),
            event_name: "Barat".into(),
            camera_label_override: None,
            skip_already_copied: false,
            base_path: base.clone(),
            scan: summary_two,
        };

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime
            .block_on(engine.run_session(request_one, Arc::new(AtomicBool::new(false)), |_| {}))
            .expect("first run session");
        let second = runtime
            .block_on(engine.run_session(request_two, Arc::new(AtomicBool::new(false)), |_| {}));

        assert!(
            second.is_ok(),
            "locked event should accept new files, got {second:?}"
        );
    }

    #[cfg(windows)]
    #[test]
    fn locked_event_can_accept_more_files_with_acl_lock() {
        let temp = tempdir().expect("temp dir");
        let source_one = temp.path().join("card_one");
        let source_two = temp.path().join("card_two");
        let base = temp.path().join("dest");
        fs::create_dir_all(&source_one).expect("create source one");
        fs::create_dir_all(&source_two).expect("create source two");
        fs::create_dir_all(&base).expect("create base");

        let src_one = source_one.join("A001.ARW");
        let src_two = source_two.join("A002.ARW");
        fs::write(&src_one, b"rawdata-one").expect("write first source file");
        fs::write(&src_two, b"rawdata-two").expect("write second source file");

        let file_one = make_scanned_file(&src_one, MediaType::PhotoRaw);
        let file_two = make_scanned_file(&src_two, MediaType::PhotoRaw);
        let summary_one = ScanSummary {
            manufacturer: "Sony".into(),
            drive_root: source_one.clone(),
            card_label: Some(source_one.display().to_string()),
            total_size_bytes: file_one.size_bytes,
            total_files: 1,
            raw_count: 1,
            jpg_count: 0,
            video_count: 0,
            audio_count: 0,
            skipped_count: 0,
            files: vec![file_one],
        };
        let summary_two = ScanSummary {
            manufacturer: "Sony".into(),
            drive_root: source_two.clone(),
            card_label: Some(source_two.display().to_string()),
            total_size_bytes: file_two.size_bytes,
            total_files: 1,
            raw_count: 1,
            jpg_count: 0,
            video_count: 0,
            audio_count: 0,
            skipped_count: 0,
            files: vec![file_two],
        };

        let paths = test_paths(temp.path());
        paths.ensure().expect("ensure paths");
        let db = Database::open(&paths).expect("db open");
        let mut settings = make_settings(&base);
        settings.safety.auto_lock_on_complete = true;
        settings.safety.lock_mode = LockMode::Acl;
        let engine = IngestEngine::new(db, settings, paths);

        let request_one = SessionRequest {
            source_drive: source_one.clone(),
            client_name: "Client Lock".into(),
            event_name: "Barat".into(),
            camera_label_override: None,
            skip_already_copied: false,
            base_path: base.clone(),
            scan: summary_one,
        };
        let request_two = SessionRequest {
            source_drive: source_two.clone(),
            client_name: "Client Lock".into(),
            event_name: "Barat".into(),
            camera_label_override: None,
            skip_already_copied: false,
            base_path: base.clone(),
            scan: summary_two,
        };

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime
            .block_on(engine.run_session(request_one, Arc::new(AtomicBool::new(false)), |_| {}))
            .expect("first run session");
        let second = runtime
            .block_on(engine.run_session(request_two, Arc::new(AtomicBool::new(false)), |_| {}));

        assert!(
            second.is_ok(),
            "ACL locked event should accept new files, got {second:?}"
        );
    }

    #[test]
    fn changed_file_at_same_source_path_is_copied_as_new_variant() {
        let temp = tempdir().expect("temp dir");
        let source = temp.path().join("card");
        let base = temp.path().join("dest");
        fs::create_dir_all(&source).expect("create source");
        fs::create_dir_all(&base).expect("create base");

        let src = source.join("A001.ARW");
        fs::write(&src, b"rawdata-new-version").expect("write source file");

        let mut file = make_scanned_file(&src, MediaType::PhotoRaw);
        file.size_bytes = 19;
        file.source_modified = "2026-01-02T00:00:00Z".into();

        let summary = ScanSummary {
            manufacturer: "Sony".into(),
            drive_root: source.clone(),
            card_label: Some(source.display().to_string()),
            total_size_bytes: file.size_bytes,
            total_files: 1,
            raw_count: 1,
            jpg_count: 0,
            video_count: 0,
            audio_count: 0,
            skipped_count: 0,
            files: vec![file.clone()],
        };

        let paths = test_paths(temp.path());
        paths.ensure().expect("ensure paths");
        let db = Database::open(&paths).expect("db open");
        let settings = make_settings(&base);
        let engine = IngestEngine::new(db.clone(), settings, paths);

        let client_id = db
            .upsert_client("Client Nine", &base, None)
            .expect("upsert client");
        let event_root = base.join("Client Nine").join("Barat");
        let event_id = db
            .prepare_event(client_id, "Barat", &event_root)
            .expect("prepare event");
        let old_dest = event_root.join("Photos").join("A001.ARW");
        if let Some(parent) = old_dest.parent() {
            fs::create_dir_all(parent).expect("create destination parent");
        }
        fs::write(&old_dest, b"rawdata-old").expect("write old destination");

        let mut old_file = file.clone();
        old_file.size_bytes = 11;
        old_file.source_modified = "2026-01-01T00:00:00Z".into();
        db.ensure_file_row(
            event_id,
            &old_file,
            &old_dest,
            crate::app::types::FileStatus::Pending,
        )
        .expect("seed old file row");
        db.update_file_status(
            event_id,
            &old_file.source_path,
            &old_dest,
            crate::app::types::FileStatus::Verified,
            Some("hash-old"),
            None,
        )
        .expect("seed verified old file");

        let request = SessionRequest {
            source_drive: source.clone(),
            client_name: "Client Nine".into(),
            event_name: "Barat".into(),
            camera_label_override: None,
            skip_already_copied: false,
            base_path: base.clone(),
            scan: summary,
        };

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let report = runtime
            .block_on(engine.run_session(request, Arc::new(AtomicBool::new(false)), |_| {}))
            .expect("run session");

        let new_dest = event_root.join("Photos").join("A001_1.ARW");
        let fallback_dest = event_root.join("Photos").join("A001_001.ARW");
        assert_eq!(report.verified_files, 1);
        assert!(
            new_dest.exists() || fallback_dest.exists(),
            "expected changed file to be copied as a new variant"
        );
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("changed since the previous ingest record")),
            "expected warning about changed tracked file"
        );
    }

    #[test]
    fn camera_override_routes_new_media_into_cam2_subfolders() {
        let temp = tempdir().expect("temp dir");
        let source = temp.path().join("card");
        let old_source = temp.path().join("old_card");
        let base = temp.path().join("dest");
        fs::create_dir_all(&source).expect("create source");
        fs::create_dir_all(&old_source).expect("create old source");
        fs::create_dir_all(&base).expect("create base");

        let photo_src = source.join("A001.JPG");
        let video_src = source.join("C001.MP4");
        let old_src = old_source.join("OLD001.ARW");
        fs::write(&photo_src, b"jpgdata").expect("write photo source");
        fs::write(&video_src, b"videodata").expect("write video source");
        fs::write(&old_src, b"oldraw").expect("write old source");

        let photo = make_scanned_file(&photo_src, MediaType::PhotoJpg);
        let video = make_scanned_file(&video_src, MediaType::Video);
        let old_file = make_scanned_file(&old_src, MediaType::PhotoRaw);
        let summary = ScanSummary {
            manufacturer: "Sony".into(),
            drive_root: source.clone(),
            card_label: Some(source.display().to_string()),
            total_size_bytes: photo.size_bytes + video.size_bytes,
            total_files: 2,
            raw_count: 0,
            jpg_count: 1,
            video_count: 1,
            audio_count: 0,
            skipped_count: 0,
            files: vec![photo.clone(), video.clone()],
        };

        let paths = test_paths(temp.path());
        paths.ensure().expect("ensure paths");
        let db = Database::open(&paths).expect("db open");
        let settings = make_settings(&base);
        let engine = IngestEngine::new(db.clone(), settings, paths);

        let client_id = db
            .upsert_client("Client Ten", &base, None)
            .expect("upsert client");
        let existing_event_root = base.join("Client Ten").join("Barat");
        let existing_event_id = db
            .prepare_event(client_id, "Barat", &existing_event_root)
            .expect("prepare event");
        let old_dest = existing_event_root.join("Photos").join("OLD001.ARW");
        if let Some(parent) = old_dest.parent() {
            fs::create_dir_all(parent).expect("create old destination parent");
        }
        fs::write(&old_dest, b"oldraw").expect("write old destination");
        db.ensure_file_row(
            existing_event_id,
            &old_file,
            &old_dest,
            crate::app::types::FileStatus::Pending,
        )
        .expect("seed old file row");
        db.update_file_status(
            existing_event_id,
            &old_file.source_path,
            &old_dest,
            crate::app::types::FileStatus::Verified,
            Some("hash-old"),
            None,
        )
        .expect("seed verified old file");
        db.mark_event(
            existing_event_id,
            crate::app::types::EventStatus::Completed,
            &existing_event_root,
            1,
            0,
            1,
            old_file.size_bytes,
            false,
        )
        .expect("complete previous event");

        let request = SessionRequest {
            source_drive: source.clone(),
            client_name: "Client Ten".into(),
            event_name: "Barat".into(),
            camera_label_override: Some("Cam 2".into()),
            skip_already_copied: false,
            base_path: base.clone(),
            scan: summary,
        };

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime
            .block_on(engine.run_session(request, Arc::new(AtomicBool::new(false)), |_| {}))
            .expect("run session");

        assert!(
            existing_event_root
                .join("Photos")
                .join("Cam 2")
                .join("A001.JPG")
                .exists(),
            "expected photo in Photos/Cam 2"
        );
        assert!(
            existing_event_root
                .join("Videos")
                .join("Cam 2")
                .join("C001.MP4")
                .exists(),
            "expected video in Videos/Cam 2"
        );
    }

    #[test]
    fn resume_path_skips_already_verified_files() {
        let temp = tempdir().expect("temp dir");
        let source = temp.path().join("card");
        let base = temp.path().join("dest");
        fs::create_dir_all(&source).expect("create source");
        fs::create_dir_all(&base).expect("create base");

        let src_one = source.join("A001.ARW");
        let src_two = source.join("A002.ARW");
        fs::write(&src_one, b"raw-1").expect("write source one");
        fs::write(&src_two, b"raw-2").expect("write source two");

        let file_one = make_scanned_file(&src_one, MediaType::PhotoRaw);
        let file_two = make_scanned_file(&src_two, MediaType::PhotoRaw);
        let summary = ScanSummary {
            manufacturer: "Sony".into(),
            drive_root: source.clone(),
            card_label: Some(source.display().to_string()),
            total_size_bytes: file_one.size_bytes + file_two.size_bytes,
            total_files: 2,
            raw_count: 2,
            jpg_count: 0,
            video_count: 0,
            audio_count: 0,
            skipped_count: 0,
            files: vec![file_one.clone(), file_two.clone()],
        };

        let paths = test_paths(temp.path());
        paths.ensure().expect("ensure paths");
        let db = Database::open(&paths).expect("db open");
        let settings = make_settings(&base);
        let engine = IngestEngine::new(db.clone(), settings, paths);

        let client_id = db
            .upsert_client("Client Two", &base, None)
            .expect("upsert client");
        let event_root = base.join("Client Two").join("Mehndi");
        let event_id = db
            .prepare_event(client_id, "Mehndi", &event_root)
            .expect("prepare event");
        let existing_dest = event_root.join("Photos").join("Cam A").join("A001.ARW");
        if let Some(parent) = existing_dest.parent() {
            fs::create_dir_all(parent).expect("create existing parent");
        }
        fs::write(&existing_dest, b"raw-1").expect("existing destination");
        db.ensure_file_row(
            event_id,
            &file_one,
            &existing_dest,
            crate::app::types::FileStatus::Pending,
        )
        .expect("seed file row");
        db.update_file_status(
            event_id,
            &file_one.source_path,
            &existing_dest,
            crate::app::types::FileStatus::Verified,
            Some("hash-existing"),
            None,
        )
        .expect("set verified");
        db.mark_event(
            event_id,
            crate::app::types::EventStatus::Partial,
            &event_root,
            1,
            0,
            summary.total_files,
            summary.total_size_bytes,
            false,
        )
        .expect("mark partial");

        let request = SessionRequest {
            source_drive: source.clone(),
            client_name: "Client Two".into(),
            event_name: "Mehndi".into(),
            camera_label_override: Some("Cam A".into()),
            skip_already_copied: false,
            base_path: base.clone(),
            scan: summary,
        };

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let report = runtime
            .block_on(engine.run_session(request, Arc::new(AtomicBool::new(false)), |_| {}))
            .expect("resume run session");

        assert_eq!(report.failed_files, 0);
        assert_eq!(report.verified_files, 2);
        assert!(report
            .warnings
            .iter()
            .any(|warning| warning.contains("Resuming existing ingest")));
    }

    #[test]
    fn duplicate_record_with_missing_destination_is_not_skipped() {
        let temp = tempdir().expect("temp dir");
        let source = temp.path().join("card");
        let base = temp.path().join("dest");
        fs::create_dir_all(&source).expect("create source");
        fs::create_dir_all(&base).expect("create base");
        let src = source.join("A001.ARW");
        fs::write(&src, b"rawdata").expect("write source file");

        let file = make_scanned_file(&src, MediaType::PhotoRaw);
        let summary = ScanSummary {
            manufacturer: "Sony".into(),
            drive_root: source.clone(),
            card_label: Some(source.display().to_string()),
            total_size_bytes: file.size_bytes,
            total_files: 1,
            raw_count: 1,
            jpg_count: 0,
            video_count: 0,
            audio_count: 0,
            skipped_count: 0,
            files: vec![file.clone()],
        };

        let paths = test_paths(temp.path());
        paths.ensure().expect("ensure paths");
        let db = Database::open(&paths).expect("db open");
        let settings = make_settings(&base);
        let engine = IngestEngine::new(db.clone(), settings, paths);

        let client_id = db
            .upsert_client("Client Five", &base, None)
            .expect("upsert client");
        let existing_event_root = base.join("Client Five").join("Barat");
        let existing_event_id = db
            .prepare_event(client_id, "Barat", &existing_event_root)
            .expect("prepare event");
        let missing_dest = existing_event_root.join("Photos").join("A001.ARW");
        db.ensure_file_row(
            existing_event_id,
            &file,
            &missing_dest,
            crate::app::types::FileStatus::Pending,
        )
        .expect("seed duplicate file row");
        db.update_file_status(
            existing_event_id,
            &file.source_path,
            &missing_dest,
            crate::app::types::FileStatus::Verified,
            Some("hash-existing"),
            None,
        )
        .expect("seed verified duplicate");
        db.mark_event(
            existing_event_id,
            crate::app::types::EventStatus::Completed,
            &existing_event_root,
            1,
            0,
            1,
            summary.total_size_bytes,
            false,
        )
        .expect("complete duplicate source event");

        let request = SessionRequest {
            source_drive: source.clone(),
            client_name: "Client Five".into(),
            event_name: "Barat".into(),
            camera_label_override: None,
            skip_already_copied: false,
            base_path: base.clone(),
            scan: summary,
        };

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let report = runtime
            .block_on(engine.run_session(request, Arc::new(AtomicBool::new(false)), |_| {}))
            .expect("run session");

        let expected_dest = base
            .join("Client Five")
            .join("Barat")
            .join("Photos")
            .join("A001.ARW");
        assert!(
            expected_dest.exists(),
            "expected copied file at {}",
            expected_dest.display()
        );
        assert_eq!(report.verified_files, 1);
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| { warning.contains("ignored because destination file is missing") }),
            "expected warning about stale duplicate destination"
        );
    }

    #[test]
    fn unreadable_duplicate_candidate_does_not_abort_ingest() {
        let temp = tempdir().expect("temp dir");
        let source = temp.path().join("new_card");
        let old_source = temp.path().join("old_card");
        let base = temp.path().join("dest");
        fs::create_dir_all(&source).expect("create source");
        fs::create_dir_all(&old_source).expect("create old source");
        fs::create_dir_all(&base).expect("create base");
        let src = source.join("A001.ARW");
        let old_src = old_source.join("OLD001.ARW");
        fs::write(&src, b"rawdata").expect("write source file");
        fs::write(&old_src, b"rawdata").expect("write old source file");

        let file = make_scanned_file(&src, MediaType::PhotoRaw);
        let old_file = make_scanned_file(&old_src, MediaType::PhotoRaw);
        let summary = ScanSummary {
            manufacturer: "Sony".into(),
            drive_root: source.clone(),
            card_label: Some(source.display().to_string()),
            total_size_bytes: file.size_bytes,
            total_files: 1,
            raw_count: 1,
            jpg_count: 0,
            video_count: 0,
            audio_count: 0,
            skipped_count: 0,
            files: vec![file.clone()],
        };

        let paths = test_paths(temp.path());
        paths.ensure().expect("ensure paths");
        let db = Database::open(&paths).expect("db open");
        let settings = make_settings(&base);
        let engine = IngestEngine::new(db.clone(), settings, paths);

        let client_id = db
            .upsert_client("Client Eight", &base, None)
            .expect("upsert client");
        let existing_event_root = base.join("Client Eight").join("Barat");
        let existing_event_id = db
            .prepare_event(client_id, "Barat", &existing_event_root)
            .expect("prepare event");
        let unreadable_dest = existing_event_root.join("Photos").join("A001.ARW");
        fs::create_dir_all(&unreadable_dest).expect("create unreadable duplicate directory");
        db.ensure_file_row(
            existing_event_id,
            &old_file,
            &unreadable_dest,
            crate::app::types::FileStatus::Pending,
        )
        .expect("seed duplicate file row");
        db.update_file_status(
            existing_event_id,
            &old_file.source_path,
            &unreadable_dest,
            crate::app::types::FileStatus::Verified,
            Some("hash-existing"),
            None,
        )
        .expect("seed verified duplicate");
        db.mark_event(
            existing_event_id,
            crate::app::types::EventStatus::Completed,
            &existing_event_root,
            1,
            0,
            1,
            summary.total_size_bytes,
            false,
        )
        .expect("complete duplicate source event");

        let request = SessionRequest {
            source_drive: source.clone(),
            client_name: "Client Eight".into(),
            event_name: "Walima".into(),
            camera_label_override: None,
            skip_already_copied: true,
            base_path: base.clone(),
            scan: summary,
        };

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let report = runtime
            .block_on(engine.run_session(request, Arc::new(AtomicBool::new(false)), |_| {}))
            .expect("run session");

        let expected_dest = base
            .join("Client Eight")
            .join("Walima")
            .join("Photos")
            .join("A001.ARW");
        assert!(
            expected_dest.exists(),
            "expected copied file at {}",
            expected_dest.display()
        );
        assert_eq!(report.verified_files, 1);
        assert!(
            report.warnings.iter().any(|warning| {
                warning.contains("previous copy could not be read")
            }),
            "expected warning about unreadable duplicate candidate"
        );
    }

    #[test]
    fn skipped_duplicate_still_records_total_file_count() {
        let temp = tempdir().expect("temp dir");
        let source = temp.path().join("card");
        let base = temp.path().join("dest");
        fs::create_dir_all(&source).expect("create source");
        fs::create_dir_all(&base).expect("create base");
        let src = source.join("A001.ARW");
        fs::write(&src, b"rawdata").expect("write source file");

        let file = make_scanned_file(&src, MediaType::PhotoRaw);
        let summary = ScanSummary {
            manufacturer: "Sony".into(),
            drive_root: source.clone(),
            card_label: Some(source.display().to_string()),
            total_size_bytes: file.size_bytes,
            total_files: 1,
            raw_count: 1,
            jpg_count: 0,
            video_count: 0,
            audio_count: 0,
            skipped_count: 0,
            files: vec![file.clone()],
        };

        let paths = test_paths(temp.path());
        paths.ensure().expect("ensure paths");
        let db = Database::open(&paths).expect("db open");
        let settings = make_settings(&base);
        let engine = IngestEngine::new(db.clone(), settings, paths);

        let client_id = db
            .upsert_client("Client Six", &base, None)
            .expect("upsert client");
        let existing_event_root = base.join("Client Six").join("Barat");
        let existing_event_id = db
            .prepare_event(client_id, "Barat", &existing_event_root)
            .expect("prepare event");
        let duplicate_dest = existing_event_root.join("Photos").join("A001.ARW");
        if let Some(parent) = duplicate_dest.parent() {
            fs::create_dir_all(parent).expect("create duplicate destination parent");
        }
        fs::write(&duplicate_dest, b"rawdata").expect("write duplicate destination");
        db.ensure_file_row(
            existing_event_id,
            &file,
            &duplicate_dest,
            crate::app::types::FileStatus::Pending,
        )
        .expect("seed duplicate file row");
        db.update_file_status(
            existing_event_id,
            &file.source_path,
            &duplicate_dest,
            crate::app::types::FileStatus::Verified,
            Some("hash-existing"),
            None,
        )
        .expect("seed verified duplicate");
        db.mark_event(
            existing_event_id,
            crate::app::types::EventStatus::Completed,
            &existing_event_root,
            1,
            0,
            1,
            summary.total_size_bytes,
            false,
        )
        .expect("complete duplicate source event");

        let request = SessionRequest {
            source_drive: source.clone(),
            client_name: "Client Six".into(),
            event_name: "Barat".into(),
            camera_label_override: None,
            skip_already_copied: false,
            base_path: base.clone(),
            scan: summary,
        };

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let report = runtime
            .block_on(engine.run_session(request, Arc::new(AtomicBool::new(false)), |_| {}))
            .expect("run session");

        assert!(report
            .warnings
            .iter()
            .any(|warning| warning.contains("Skipped duplicate")));
        let events = db.recent_events(1).expect("read recent events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].total_files, 1);
        assert_eq!(events[0].verified_files, 0);
    }

    #[test]
    fn cross_event_duplicate_is_skipped_when_option_is_enabled() {
        let temp = tempdir().expect("temp dir");
        let source = temp.path().join("new_card");
        let old_source = temp.path().join("old_card");
        let base = temp.path().join("dest");
        fs::create_dir_all(&source).expect("create source");
        fs::create_dir_all(&old_source).expect("create old source");
        fs::create_dir_all(&base).expect("create base");
        let src = source.join("B777.ARW");
        let old_src = old_source.join("A001.ARW");
        fs::write(&src, b"rawdata").expect("write source file");
        fs::write(&old_src, b"rawdata").expect("write old source file");

        let file = make_scanned_file(&src, MediaType::PhotoRaw);
        let old_file = make_scanned_file(&old_src, MediaType::PhotoRaw);
        let summary = ScanSummary {
            manufacturer: "Sony".into(),
            drive_root: source.clone(),
            card_label: Some(source.display().to_string()),
            total_size_bytes: file.size_bytes,
            total_files: 1,
            raw_count: 1,
            jpg_count: 0,
            video_count: 0,
            audio_count: 0,
            skipped_count: 0,
            files: vec![file.clone()],
        };

        let paths = test_paths(temp.path());
        paths.ensure().expect("ensure paths");
        let db = Database::open(&paths).expect("db open");
        let settings = make_settings(&base);
        let engine = IngestEngine::new(db.clone(), settings, paths);

        let client_id = db
            .upsert_client("Client Seven", &base, None)
            .expect("upsert client");
        let existing_event_root = base.join("Client Seven").join("Barat");
        let existing_event_id = db
            .prepare_event(client_id, "Barat", &existing_event_root)
            .expect("prepare event");
        let duplicate_dest = existing_event_root.join("Photos").join("A001.ARW");
        if let Some(parent) = duplicate_dest.parent() {
            fs::create_dir_all(parent).expect("create duplicate destination parent");
        }
        fs::write(&duplicate_dest, b"rawdata").expect("write duplicate destination");
        db.ensure_file_row(
            existing_event_id,
            &old_file,
            &duplicate_dest,
            crate::app::types::FileStatus::Pending,
        )
        .expect("seed duplicate file row");
        db.update_file_status(
            existing_event_id,
            &old_file.source_path,
            &duplicate_dest,
            crate::app::types::FileStatus::Verified,
            Some("hash-existing"),
            None,
        )
        .expect("seed verified duplicate");
        db.mark_event(
            existing_event_id,
            crate::app::types::EventStatus::Completed,
            &existing_event_root,
            1,
            0,
            1,
            summary.total_size_bytes,
            false,
        )
        .expect("complete duplicate source event");

        let request = SessionRequest {
            source_drive: source.clone(),
            client_name: "Client Seven".into(),
            event_name: "Walima".into(),
            camera_label_override: None,
            skip_already_copied: true,
            base_path: base.clone(),
            scan: summary,
        };

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let report = runtime
            .block_on(engine.run_session(request, Arc::new(AtomicBool::new(false)), |_| {}))
            .expect("run session");

        let new_event_dest = base
            .join("Client Seven")
            .join("Walima")
            .join("Photos")
            .join("A001.ARW");
        assert!(
            !new_event_dest.exists(),
            "duplicate should not be recopied into {}",
            new_event_dest.display()
        );
        assert_eq!(report.verified_files, 0);
        assert!(report
            .warnings
            .iter()
            .any(|warning| warning.contains("Skipped duplicate")));
    }

    #[test]
    fn preflight_blocks_when_space_requirement_is_unmet() {
        let temp = tempdir().expect("temp dir");
        let source = temp.path().join("card");
        let base = temp.path().join("dest");
        fs::create_dir_all(&source).expect("create source");
        fs::create_dir_all(&base).expect("create base");
        let src = source.join("C0001.MP4");
        fs::write(&src, b"video").expect("write source file");

        let file = make_scanned_file(&src, MediaType::Video);
        let summary = ScanSummary {
            manufacturer: "Generic".into(),
            drive_root: source.clone(),
            card_label: Some(source.display().to_string()),
            total_size_bytes: u64::MAX / 4,
            total_files: 1,
            raw_count: 0,
            jpg_count: 0,
            video_count: 1,
            audio_count: 0,
            skipped_count: 0,
            files: vec![file],
        };

        let paths = test_paths(temp.path());
        paths.ensure().expect("ensure paths");
        let db = Database::open(&paths).expect("db open");
        let settings = make_settings(&base);
        let engine = IngestEngine::new(db, settings, paths);

        let request = SessionRequest {
            source_drive: source,
            client_name: "Client Three".into(),
            event_name: "Walima".into(),
            camera_label_override: Some("Cam A".into()),
            skip_already_copied: false,
            base_path: base,
            scan: summary,
        };

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let result =
            runtime.block_on(engine.run_session(request, Arc::new(AtomicBool::new(false)), |_| {}));
        let error_text = format!("{:#}", result.expect_err("expected preflight to fail"));
        assert!(error_text.contains("Destination drive full"));
    }

    #[test]
    fn single_camera_without_override_does_not_create_camera_folder() {
        let temp = tempdir().expect("temp dir");
        let source = temp.path().join("card");
        let base = temp.path().join("dest");
        fs::create_dir_all(source.join("DCIM").join("100MSDCF")).expect("create source");
        fs::create_dir_all(&base).expect("create base");
        fs::write(
            source.join("DCIM").join("100MSDCF").join("A001.ARW"),
            b"rawdata",
        )
        .expect("write raw");

        let summary = scan_drive(&source, false, false).expect("scan");
        let paths = test_paths(temp.path());
        paths.ensure().expect("ensure paths");
        let db = Database::open(&paths).expect("db open");
        let settings = make_settings(&base);
        let engine = IngestEngine::new(db, settings, paths);

        let request = SessionRequest {
            source_drive: source.clone(),
            client_name: "Client Four".into(),
            event_name: "Ubtan".into(),
            camera_label_override: None,
            skip_already_copied: false,
            base_path: base.clone(),
            scan: summary,
        };

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime
            .block_on(engine.run_session(request, Arc::new(AtomicBool::new(false)), |_| {}))
            .expect("run session");

        let expected = base
            .join("Client Four")
            .join("Ubtan")
            .join("Photos")
            .join("A001.ARW");
        assert!(
            expected.exists(),
            "expected copied file at {}",
            expected.display()
        );
    }
}
