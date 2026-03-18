#![cfg_attr(windows, windows_subsystem = "windows")]

mod app;

use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex, OnceLock,
};
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use app::db::Database;
use app::drive_monitor::detect_drives;
use app::ingest::IngestEngine;
use app::scanner::scan_drive_with_cancel;
use app::settings::{AppPaths, Settings};
use app::types::{
    next_event_suggestion, validate_folder_name, ClientSummary, EventOption, ScanSummary,
    SessionRequest,
};
use chrono::Local;
use rfd::FileDialog;
use slint::ComponentHandle;
use tracing::{error, info};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

slint::include_modules!();

#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
#[cfg(windows)]
use windows::core::PCWSTR;
#[cfg(windows)]
use windows::Win32::Foundation::{CloseHandle, GetLastError, ERROR_ALREADY_EXISTS, HANDLE};
#[cfg(windows)]
use windows::Win32::System::Threading::CreateMutexW;

const SUGGESTION_DEBOUNCE_MS: u64 = 120;
const PROGRESS_UI_MIN_INTERVAL_MS: u128 = 160;
const PROGRESS_UI_MIN_BYTES_DELTA: u64 = 8 * 1024 * 1024;

static LOG_GUARD: OnceLock<WorkerGuard> = OnceLock::new();

#[derive(Clone)]
struct AppState {
    db: Database,
    settings: Settings,
    current_scan: Arc<Mutex<Option<ScanSummary>>>,
    cancel_flag: Arc<AtomicBool>,
    operation_active: Arc<AtomicBool>,
    last_auto_source: Arc<Mutex<Option<String>>>,
    client_query_seq: Arc<AtomicU64>,
    event_query_seq: Arc<AtomicU64>,
    mock_mode: bool,
}

fn main() -> Result<()> {
    let mock_mode = std::env::args().any(|arg| arg == "--mock");
    let paths = AppPaths::discover()?;
    paths.ensure()?;
    let _instance_guard = acquire_single_instance()?;
    init_logging(&paths)?;
    init_panic_hook();

    let settings = Settings::load_or_create(&paths)?;
    let db = Database::open(&paths)?;
    let state = AppState {
        db: db.clone(),
        settings: settings.clone(),
        current_scan: Arc::new(Mutex::new(None)),
        cancel_flag: Arc::new(AtomicBool::new(false)),
        operation_active: Arc::new(AtomicBool::new(false)),
        last_auto_source: Arc::new(Mutex::new(None)),
        client_query_seq: Arc::new(AtomicU64::new(0)),
        event_query_seq: Arc::new(AtomicU64::new(0)),
        mock_mode,
    };

    let window = AppWindow::new()?;
    window.set_base_path(
        settings
            .general
            .default_base_path
            .display()
            .to_string()
            .into(),
    );
    let initial_event_options = db.event_options("").unwrap_or_default();
    apply_event_menu(
        &window,
        &initial_event_options,
        &window.get_event_name().to_string(),
    );
    if mock_mode {
        apply_mock_bootstrap(&window);
    }
    let resume_text = startup_resume_text(&db);
    if let Some(text) = &resume_text {
        window.set_status_text(format!("Ready. {text}").into());
        window.set_report_text(text.clone().into());
    } else {
        window.set_status_text("Ready. Refresh cards to detect media.".into());
    }

    wire_refresh(&window, state.clone());
    wire_scan(&window, state.clone());
    wire_start(&window, state.clone());
    wire_cancel(&window, state.clone());
    wire_suggest_client(&window, state.clone());
    wire_client_typing_suggestions(&window, state.clone());
    wire_event_typing_suggestions(&window, state.clone());
    wire_source_drive_edit(&window, state.clone());
    wire_select_source_path(&window, state.clone());
    wire_select_base_path(&window);
    wire_history(&window, state.clone());
    start_drive_polling(&window, state.clone(), mock_mode);

    window.run()?;
    Ok(())
}

fn wire_refresh(window: &AppWindow, state: AppState) {
    let weak = window.as_weak();
    window.on_refresh_cards(move || {
        if state.mock_mode {
            if let Some(ui) = weak.upgrade() {
                ui.set_drives_text(mock_drive_text().into());
                ui.set_source_drive("E:\\".into());
                ui.set_status_text("Mock drive refresh complete.".into());
            }
            return;
        }
        let weak = weak.clone();
        let state = state.clone();
        thread::spawn(move || match detect_drives() {
            Ok(drives) => {
                let source_drive = primary_auto_scan_source(&drives);
                let text = format_detected_drives(&drives);
                let _ = weak.upgrade_in_event_loop(move |ui| {
                    ui.set_drives_text(text.into());
                    if !can_apply_auto_source(&state, &ui.get_source_drive().to_string(), &source_drive)
                    {
                        ui.set_status_text("Drive refresh complete.".into());
                        return;
                    }

                    apply_source_change(
                        &state,
                        &ui,
                        &source_drive,
                        SourceChangeOrigin::AutoDetected,
                        true,
                        "Removable media detected. Starting scan...",
                    );
                });
                info!("drive refresh complete");
            }
            Err(error) => {
                let message = format!("Drive detection failed: {error:#}");
                let _ = weak.upgrade_in_event_loop(move |ui| ui.set_status_text(message.into()));
            }
        });
    });
}

fn start_drive_polling(window: &AppWindow, state: AppState, mock_mode: bool) {
    if mock_mode {
        return;
    }
    let weak = window.as_weak();
    thread::spawn(move || {
        let mut last_snapshot = String::new();
        let mut last_primary_signature = String::new();
        loop {
            let Some(_) = weak.upgrade() else {
                break;
            };
            match detect_drives() {
                Ok(drives) => {
                    let source_drive = primary_auto_scan_source(&drives);
                    let snapshot = format_detected_drives(&drives);
                    let primary_signature = primary_detected_signature(&drives);
                    let snapshot_changed = snapshot != last_snapshot;
                    let primary_changed = primary_signature != last_primary_signature;

                    if snapshot_changed || primary_changed {
                        last_snapshot = snapshot.clone();
                        last_primary_signature = primary_signature.clone();
                        let state = state.clone();
                        let _ = weak.upgrade_in_event_loop(move |ui| {
                            ui.set_drives_text(snapshot.into());
                            if !can_apply_auto_source(
                                &state,
                                &ui.get_source_drive().to_string(),
                                &source_drive,
                            ) {
                                return;
                            }

                            apply_source_change(
                                &state,
                                &ui,
                                &source_drive,
                                SourceChangeOrigin::AutoDetected,
                                true,
                                "Removable media detected. Starting scan...",
                            );
                        });
                    }
                }
                Err(_) => {}
            }
            thread::sleep(Duration::from_secs(2));
        }
    });
}

fn wire_scan(window: &AppWindow, state: AppState) {
    let weak = window.as_weak();
    window.on_scan_card(move || {
        if state.operation_active.load(Ordering::Relaxed) {
            if let Some(ui) = weak.upgrade() {
                ui.set_status_text("Another operation is already running.".into());
            }
            return;
        }

        if state.mock_mode {
            if let Err(error) = set_current_scan(&state, Some(mock_scan_summary())) {
                if let Some(ui) = weak.upgrade() {
                    ui.set_status_text(format!("Scan cache error: {error}").into());
                }
                return;
            }
            if let Some(ui) = weak.upgrade() {
                ui.set_source_drive("E:\\".into());
                ui.set_camera_label("".into());
                ui.set_client_name("Mock Client".into());
                ui.set_event_name("Barat".into());
                ui.set_base_path(r"D:\Projects".into());
                ui.set_status_text(
                    "Mock scan complete: Sony 12 files · RAW 6 · JPG/PNG 3 · Video 2 · Audio 1 · 4.8 GB"
                        .into(),
                );
                ui.set_progress_label("Ready to ingest.".into());
                ui.set_report_text(mock_report_text().into());
            }
            return;
        }
        let source_drive = weak
            .upgrade()
            .map(|ui| ui.get_source_drive().to_string())
            .unwrap_or_default();
        if source_drive.trim().is_empty() {
            if let Some(ui) = weak.upgrade() {
                ui.set_status_text("Enter or refresh a source drive first.".into());
            }
            return;
        }

        if let Some(ui) = weak.upgrade() {
            state.cancel_flag.store(false, Ordering::Relaxed);
            state.operation_active.store(true, Ordering::Relaxed);
            ui.set_status_text("Scanning source...".into());
            ui.set_progress_label("Reading files and building media inventory.".into());
            ui.set_report_text(
                format!(
                    "Scan started for {}\nPlease wait while FastTrack scans supported media files.",
                    source_drive.trim()
                )
                .into(),
            );
            ui.set_progress_percent(0.0);
            ui.set_verify_percent(0.0);
        }

        let weak = weak.clone();
        let state = state.clone();
        thread::spawn(move || {
            let source_drive_trimmed = source_drive.trim().to_string();
            let root = std::path::PathBuf::from(&source_drive_trimmed);
            match scan_drive_with_cancel(
                &root,
                state.settings.ingest.include_proxy_files,
                state.settings.ingest.skip_hidden_files,
                Some(state.cancel_flag.clone()),
            ) {
                Ok(scan) => {
                    let current_client = weak
                        .upgrade()
                        .map(|ui| ui.get_client_name().to_string())
                        .unwrap_or_default();
                    let suggestion = if current_client.trim().is_empty() {
                        next_event_suggestion(&[])
                    } else {
                        state
                            .db
                            .suggest_next_event(current_client.trim())
                            .unwrap_or_else(|_| next_event_suggestion(&[]))
                    };
                    if let Err(error) = set_current_scan(&state, Some(scan.clone())) {
                        state.operation_active.store(false, Ordering::Relaxed);
                        let _ = weak.upgrade_in_event_loop(move |ui| {
                            ui.set_status_text(format!("Scan cache error: {error}").into())
                        });
                        return;
                    }

                    let summary = scan.summary_text();
                    let existing_paths = state
                        .db
                        .matching_event_roots_for_scan(&scan, 4)
                        .unwrap_or_default();
                    let event_options = state
                        .db
                        .event_options(current_client.trim())
                        .unwrap_or_default();
                    let resume_hint = state
                        .db
                        .resumable_event(current_client.trim(), &suggestion)
                        .ok()
                        .flatten()
                        .map(|resume| format_resume(&resume));
                    let base_path = state
                        .db
                        .find_client_base_path(current_client.trim())
                        .ok()
                        .flatten()
                        .unwrap_or_else(|| state.settings.general.default_base_path.clone())
                        .display()
                        .to_string();
                    let summary_clone = summary.clone();
                    let duplicate_prompt = if existing_paths.is_empty() {
                        None
                    } else {
                        Some(format_existing_copy_warning(&existing_paths))
                    };
                    let _ = weak.upgrade_in_event_loop(move |ui| {
                        ui.set_event_name(suggestion.into());
                        apply_event_menu(&ui, &event_options, &ui.get_event_name().to_string());
                        ui.set_base_path(base_path.into());
                        if duplicate_prompt.is_some() {
                            ui.set_status_text(
                                "Scan complete. Some files already exist in previous destination paths."
                                    .into(),
                            );
                        } else {
                            ui.set_status_text(format!("Scan complete: {summary}").into());
                        }
                        ui.set_progress_label("Ready to ingest.".into());
                        ui.set_report_text(
                            duplicate_prompt
                                .or(resume_hint)
                                .unwrap_or(summary_clone)
                                .into(),
                        );
                    });
                    state.operation_active.store(false, Ordering::Relaxed);
                }
                Err(error) => {
                    let scan_cancelled = error
                        .downcast_ref::<app::errors::MoonError>()
                        .is_some_and(|value| matches!(value, app::errors::MoonError::ScanCancelled));
                    let error_text = format!("{error:#}");
                    let _ = weak.upgrade_in_event_loop(move |ui| {
                        if scan_cancelled {
                            ui.set_status_text("Scan cancelled.".into());
                            ui.set_progress_label("Scan cancelled.".into());
                            ui.set_report_text(
                                format!("Scan cancelled for {}.", source_drive_trimmed).into(),
                            );
                        } else {
                            ui.set_status_text(format!("Scan failed: {error_text}").into());
                            ui.set_progress_label("Scan failed.".into());
                            ui.set_report_text(
                                format!(
                                    "Scan failed for {}\n{}",
                                    source_drive_trimmed, error_text
                                )
                                .into(),
                            );
                        }
                        ui.set_progress_percent(0.0);
                        ui.set_verify_percent(0.0);
                    });
                    state.operation_active.store(false, Ordering::Relaxed);
                }
            }
        });
    });
}

fn wire_start(window: &AppWindow, state: AppState) {
    let weak = window.as_weak();
    window.on_start_ingest(move || {
        if state.mock_mode {
            simulate_mock_ingest(weak.clone());
            return;
        }
        let Some(ui) = weak.upgrade() else {
            return;
        };

        let client_name = match validate_folder_name(&ui.get_client_name().to_string(), 100) {
            Ok(value) => value,
            Err(error) => {
                ui.set_status_text(error.to_string().into());
                return;
            }
        };
        let event_name = match validate_folder_name(&ui.get_event_name().to_string(), 100) {
            Ok(value) => value,
            Err(error) => {
                ui.set_status_text(error.to_string().into());
                return;
            }
        };
        let camera_label = ui.get_camera_label().to_string();
        let base_path = std::path::PathBuf::from(ui.get_base_path().to_string());
        let source_drive = std::path::PathBuf::from(ui.get_source_drive().to_string());
        let scan = match get_current_scan(&state) {
            Ok(Some(scan)) => scan,
            Ok(None) => {
                ui.set_status_text("Scan a source card before starting ingest.".into());
                return;
            }
            Err(error) => {
                ui.set_status_text(format!("Cannot start ingest: {error}").into());
                return;
            }
        };
        if !same_source_path(
            &source_drive.display().to_string(),
            &scan.drive_root.display().to_string(),
        ) {
            let _ = set_current_scan(&state, None);
            ui.set_status_text("Source changed since last scan. Run scan again.".into());
            return;
        }

        state.cancel_flag.store(false, Ordering::Relaxed);
        state.operation_active.store(true, Ordering::Relaxed);
        ui.set_status_text("Starting ingest...".into());

        let request = SessionRequest {
            source_drive,
            client_name,
            event_name,
            camera_label_override: (!camera_label.trim().is_empty()).then_some(camera_label),
            skip_already_copied: ui.get_skip_already_copied(),
            base_path,
            scan,
        };

        let weak = weak.clone();
        let state = state.clone();
        if let Ok(Some(resume)) = state
            .db
            .resumable_event(&request.client_name, &request.event_name)
        {
            ui.set_status_text(format!("Resuming: {}", format_resume(&resume)).into());
        }
        thread::spawn(move || {
            let paths = match AppPaths::discover() {
                Ok(paths) => paths,
                Err(error) => {
                    state.operation_active.store(false, Ordering::Relaxed);
                    let _ = weak.upgrade_in_event_loop(move |ui| {
                        ui.set_status_text(format!("Cannot prepare app paths: {error:#}").into());
                        ui.set_progress_label("Failed".into());
                    });
                    return;
                }
            };
            let engine = IngestEngine::new(state.db.clone(), state.settings.clone(), paths);
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    state.operation_active.store(false, Ordering::Relaxed);
                    let _ = weak.upgrade_in_event_loop(move |ui| {
                        ui.set_status_text(format!("Runtime startup failed: {error:#}").into());
                        ui.set_progress_label("Failed".into());
                    });
                    return;
                }
            };
            let started_at = std::time::Instant::now();
            let mut last_ui_tick = started_at;
            let mut last_ui_bytes = 0u64;

            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                runtime.block_on(engine.run_session(
                    request,
                    state.cancel_flag.clone(),
                    |progress| {
                        let now = std::time::Instant::now();
                        let bytes_delta = progress.bytes_done.saturating_sub(last_ui_bytes);
                        let should_emit = progress.bytes_done >= progress.total_bytes
                            || progress.files_done >= progress.total_files
                            || bytes_delta >= PROGRESS_UI_MIN_BYTES_DELTA
                            || now.duration_since(last_ui_tick).as_millis()
                                >= PROGRESS_UI_MIN_INTERVAL_MS;
                        if !should_emit {
                            return;
                        }

                        last_ui_tick = now;
                        last_ui_bytes = progress.bytes_done;

                        let elapsed = started_at.elapsed().as_secs_f64().max(0.001);
                        let speed_mbps = (progress.bytes_done as f64 / 1024.0 / 1024.0) / elapsed;
                        let remaining_bytes =
                            progress.total_bytes.saturating_sub(progress.bytes_done) as f64;
                        let eta_text = if speed_mbps > 0.01 {
                            let eta_secs = remaining_bytes / (speed_mbps * 1024.0 * 1024.0);
                            if eta_secs < 60.0 {
                                format!("ETA {:.0}s", eta_secs)
                            } else {
                                format!("ETA {:.1}m", eta_secs / 60.0)
                            }
                        } else {
                            "ETA --".to_string()
                        };
                        let text = format!("{} | {:.1} MB/s | {}", progress, speed_mbps, eta_text);
                        let pct = if progress.total_bytes > 0 {
                            progress.bytes_done as f32 / progress.total_bytes as f32
                        } else {
                            0.0
                        };
                        let vpct = if progress.total_files > 0 {
                            progress.files_done as f32 / progress.total_files as f32
                        } else {
                            0.0
                        };
                        let plabel = format!(
                            "Progress: {} of {} files ({} / {})",
                            progress.files_done,
                            progress.total_files,
                            app::types::format_bytes(progress.bytes_done),
                            app::types::format_bytes(progress.total_bytes)
                        );
                        let _ = weak.upgrade_in_event_loop(move |ui| {
                            ui.set_status_text(text.clone().into());
                            ui.set_progress_percent(pct);
                            ui.set_verify_percent(vpct);
                            ui.set_progress_label(plabel.into());
                        });
                    },
                ))
            }));

            match result {
                Ok(Ok(report)) => {
                    let text = report.to_text();
                    state.operation_active.store(false, Ordering::Relaxed);
                    let _ = weak.upgrade_in_event_loop(move |ui| {
                        ui.set_status_text("Ingest complete.".into());
                        ui.set_report_text(text.into());
                        ui.set_progress_percent(1.0);
                        ui.set_verify_percent(1.0);
                        ui.set_progress_label("Complete".into());
                    });
                }
                Ok(Err(error)) => {
                    state.operation_active.store(false, Ordering::Relaxed);
                    let error_text = format!("{error:#}");
                    let _ = weak.upgrade_in_event_loop(move |ui| {
                        ui.set_status_text(format!("Ingest failed: {error_text}").into());
                        ui.set_progress_percent(0.0);
                        ui.set_verify_percent(0.0);
                        ui.set_progress_label("Failed".into());
                        ui.set_report_text(format!("Ingest failed.\n{error_text}").into());
                    });
                }
                Err(_) => {
                    state.operation_active.store(false, Ordering::Relaxed);
                    let _ = weak.upgrade_in_event_loop(move |ui| {
                        ui.set_status_text(
                            "Ingest crashed unexpectedly. Check logs and try again.".into(),
                        );
                        ui.set_progress_percent(0.0);
                        ui.set_verify_percent(0.0);
                        ui.set_progress_label("Crashed".into());
                    });
                }
            }
        });
    });
}

fn wire_cancel(window: &AppWindow, state: AppState) {
    let weak = window.as_weak();
    window.on_cancel_ingest(move || {
        state.cancel_flag.store(true, Ordering::Relaxed);
        if let Some(ui) = weak.upgrade() {
            ui.set_status_text("Cancelling current operation...".into());
            ui.set_progress_label("Cancelling...".into());
        }
    });
}

fn wire_suggest_client(window: &AppWindow, state: AppState) {
    let weak = window.as_weak();
    window.on_suggest_client(move || {
        let Some(ui) = weak.upgrade() else {
            return;
        };
        let query = ui.get_client_name().to_string();
        let weak = weak.clone();
        let state = state.clone();
        thread::spawn(move || {
            let clients = match state.db.search_clients(&query, 5) {
                Ok(clients) => clients,
                Err(error) => {
                    let text = format!("Could not load client suggestions: {error:#}");
                    let _ = weak.upgrade_in_event_loop(move |ui| {
                        ui.set_status_text(text.clone().into());
                        ui.set_report_text(text.into());
                    });
                    return;
                }
            };

            if clients.is_empty() {
                let text = if query.trim().is_empty() {
                    "No saved clients yet. Start an ingest to create one.".to_string()
                } else {
                    format!("No saved clients matched '{}'.", query.trim())
                };
                let _ = weak.upgrade_in_event_loop(move |ui| {
                    ui.set_status_text(text.clone().into());
                    ui.set_report_text(text.into());
                });
                return;
            }

            let selected = select_client(&clients, &query);
            let selected_name = selected.name.clone();
            let selected_base = selected.base_path.display().to_string();
            let text = format_client_suggestions(&clients, &selected_name);
            let event_options = state
                .db
                .event_options(selected_name.trim())
                .unwrap_or_default();
            let suggested_event = state
                .db
                .suggest_next_event(selected_name.trim())
                .unwrap_or_else(|_| next_event_suggestion(&[]));
            let _ = weak.upgrade_in_event_loop(move |ui| {
                ui.set_client_name(selected_name.into());
                ui.set_base_path(selected_base.into());
                apply_event_menu(&ui, &event_options, &suggested_event);
                if ui.get_event_name().to_string().trim().is_empty() {
                    ui.set_event_name(suggested_event.into());
                }
                ui.set_status_text("Client suggestions loaded.".into());
                ui.set_report_text(text.into());
            });
        });
    });
}

fn wire_client_typing_suggestions(window: &AppWindow, state: AppState) {
    let weak = window.as_weak();
    window.on_client_name_edited(move |value| {
        let query = value.to_string();
        let request_id = state.client_query_seq.fetch_add(1, Ordering::Relaxed) + 1;

        let weak = weak.clone();
        let state = state.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(SUGGESTION_DEBOUNCE_MS));
            if state.client_query_seq.load(Ordering::Relaxed) != request_id {
                return;
            }

            let clients = match state.db.search_clients(&query, 6) {
                Ok(clients) => clients,
                Err(_) => return,
            };
            if clients.is_empty() {
                return;
            }
            if state.client_query_seq.load(Ordering::Relaxed) != request_id {
                return;
            }

            let selected = select_client(&clients, &query);
            let selected_name = selected.name.clone();
            let selected_base = selected.base_path.display().to_string();
            let report = format_client_suggestions(&clients, &selected_name);
            let event_options = state
                .db
                .event_options(selected_name.trim())
                .unwrap_or_default();
            let suggested_event = state
                .db
                .suggest_next_event(selected_name.trim())
                .unwrap_or_else(|_| next_event_suggestion(&[]));
            let typed_query = query.clone();
            let state_for_ui = state.clone();

            let _ = weak.upgrade_in_event_loop(move |ui| {
                if state_for_ui.client_query_seq.load(Ordering::Relaxed) != request_id {
                    return;
                }
                if ui.get_client_name().to_string() != typed_query {
                    return;
                }
                if selected_name.eq_ignore_ascii_case(typed_query.trim()) {
                    ui.set_base_path(selected_base.into());
                }
                apply_event_menu(&ui, &event_options, &suggested_event);
                ui.set_report_text(report.into());
            });
        });
    });
}

fn wire_event_typing_suggestions(window: &AppWindow, state: AppState) {
    let weak = window.as_weak();
    window.on_event_name_edited(move |value| {
        let event_query = value.to_string();
        let request_id = state.event_query_seq.fetch_add(1, Ordering::Relaxed) + 1;
        let client_name = weak
            .upgrade()
            .map(|ui| ui.get_client_name().to_string())
            .unwrap_or_default();
        if client_name.trim().is_empty() {
            return;
        }

        let weak = weak.clone();
        let state = state.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(SUGGESTION_DEBOUNCE_MS));
            if state.event_query_seq.load(Ordering::Relaxed) != request_id {
                return;
            }

            let full_options = match state.db.event_options(client_name.trim()) {
                Ok(options) => options,
                Err(_) => return,
            };
            let options =
                match state
                    .db
                    .search_event_options(client_name.trim(), event_query.trim(), 8)
                {
                    Ok(options) => options,
                    Err(_) => return,
                };
            if state.event_query_seq.load(Ordering::Relaxed) != request_id {
                return;
            }

            let menu_options = if event_query.trim().is_empty() || options.is_empty() {
                full_options.clone()
            } else {
                options.clone()
            };

            let suggested = state
                .db
                .suggest_next_event(client_name.trim())
                .unwrap_or_else(|_| next_event_suggestion(&[]));
            let report = format_event_options(&menu_options, &suggested);
            let typed_query = event_query.clone();
            let state_for_ui = state.clone();

            let _ = weak.upgrade_in_event_loop(move |ui| {
                if state_for_ui.event_query_seq.load(Ordering::Relaxed) != request_id {
                    return;
                }
                if ui.get_event_name().to_string() != typed_query {
                    return;
                }
                apply_event_menu(&ui, &menu_options, &typed_query);
                ui.set_report_text(report.into());
            });
        });
    });
}

fn wire_source_drive_edit(window: &AppWindow, state: AppState) {
    let weak = window.as_weak();
    window.on_source_drive_edited(move |value| {
        let Some(ui) = weak.upgrade() else {
            return;
        };
        apply_source_change(
            &state,
            &ui,
            &value.to_string(),
            SourceChangeOrigin::Manual,
            false,
            "Source changed. Scan again before ingest.",
        );
    });
}

fn wire_select_source_path(window: &AppWindow, state: AppState) {
    let weak = window.as_weak();
    window.on_select_source_path(move || {
        let mut dialog = FileDialog::new().set_title("Select source drive or folder");
        if let Some(ui) = weak.upgrade() {
            let current = std::path::PathBuf::from(ui.get_source_drive().to_string());
            if current.exists() {
                dialog = dialog.set_directory(current);
            }
        }

        let picked = dialog.pick_folder();

        let Some(path) = picked else {
            return;
        };

        if let Some(ui) = weak.upgrade() {
            apply_source_change(
                &state,
                &ui,
                &path.display().to_string(),
                SourceChangeOrigin::Manual,
                true,
                "Source path selected. Starting scan...",
            );
        }
    });
}

fn wire_select_base_path(window: &AppWindow) {
    let weak = window.as_weak();
    window.on_select_base_path(move || {
        let mut dialog = FileDialog::new().set_title("Select destination root");
        if let Some(ui) = weak.upgrade() {
            let current = std::path::PathBuf::from(ui.get_base_path().to_string());
            if current.exists() {
                dialog = dialog.set_directory(current);
            }
        }

        let picked = dialog.pick_folder();

        let Some(path) = picked else {
            return;
        };

        if let Some(ui) = weak.upgrade() {
            ui.set_base_path(path.display().to_string().into());
            ui.set_status_text("Destination path selected.".into());
        }
    });
}

fn wire_history(window: &AppWindow, state: AppState) {
    let weak = window.as_weak();
    window.on_show_history(move || {
        let weak = weak.clone();
        let state = state.clone();
        thread::spawn(move || {
            let text = match state.db.recent_events(8) {
                Ok(events) if !events.is_empty() => format_history(&events),
                Ok(_) => "No ingest history recorded yet.".to_string(),
                Err(error) => format!("Could not load history: {error:#}"),
            };
            let _ = weak.upgrade_in_event_loop(move |ui| {
                ui.set_status_text("History loaded.".into());
                ui.set_report_text(text.into());
            });
        });
    });
}

fn init_logging(paths: &AppPaths) -> Result<()> {
    let file_name = format!("fasttrack_{}.log", Local::now().format("%Y%m%d"));
    let file_appender = tracing_appender::rolling::never(&paths.logs_dir, file_name);
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
    let _ = LOG_GUARD.set(guard);
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .with_writer(non_blocking)
        .with_ansi(false)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
    Ok(())
}

fn init_panic_hook() {
    std::panic::set_hook(Box::new(|panic_info| {
        error!("unhandled panic: {panic_info}");
    }));
}

#[cfg(windows)]
struct InstanceGuard(HANDLE);

#[cfg(windows)]
impl Drop for InstanceGuard {
    fn drop(&mut self) {
        unsafe {
            let _ = CloseHandle(self.0);
        }
    }
}

#[cfg(windows)]
fn acquire_single_instance() -> Result<InstanceGuard> {
    let name: Vec<u16> = std::ffi::OsStr::new("Global\\FastTrackV1")
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let handle = unsafe { CreateMutexW(None, false, PCWSTR(name.as_ptr()))? };
    let last_error = unsafe { GetLastError() };
    if last_error == ERROR_ALREADY_EXISTS {
        error!("FastTrack is already running");
        unsafe {
            let _ = CloseHandle(handle);
        }
        bail!("FastTrack is already running.");
    }
    Ok(InstanceGuard(handle))
}

#[cfg(not(windows))]
fn acquire_single_instance() -> Result<()> {
    Ok(())
}

fn startup_resume_text(db: &Database) -> Option<String> {
    db.pending_resumes()
        .ok()
        .and_then(|mut events| events.drain(..).next())
        .map(|resume| format!("Pending resume detected: {}", format_resume(&resume)))
}

fn set_current_scan(state: &AppState, scan: Option<ScanSummary>) -> Result<()> {
    let mut guard = state
        .current_scan
        .lock()
        .map_err(|_| anyhow!("internal state error: scan cache unavailable"))?;
    *guard = scan;
    Ok(())
}

#[derive(Clone, Copy)]
enum SourceChangeOrigin {
    Manual,
    AutoDetected,
}

fn apply_source_change(
    state: &AppState,
    ui: &AppWindow,
    new_source: &str,
    origin: SourceChangeOrigin,
    start_scan: bool,
    status_text: &str,
) {
    let new_source = new_source.trim();
    let current_source = ui.get_source_drive().to_string();

    if new_source.is_empty() {
        let _ = set_current_scan(state, None);
        let _ = set_last_auto_source(state, None);
        ui.set_source_drive("".into());
        ui.set_status_text(status_text.into());
        return;
    }

    let scan_changed = invalidate_scan_for_source(state, new_source)
        .map(|changed| changed || !same_source_path(&current_source, new_source))
        .unwrap_or(false);

    ui.set_source_drive(new_source.into());
    match origin {
        SourceChangeOrigin::Manual => {
            let _ = set_last_auto_source(state, None);
        }
        SourceChangeOrigin::AutoDetected => {
            let _ = set_last_auto_source(state, Some(new_source.to_string()));
        }
    }

    if start_scan {
        ui.set_status_text(status_text.into());
        ui.invoke_scan_card();
    } else if scan_changed {
        ui.set_status_text(status_text.into());
    }
}

fn can_apply_auto_source(state: &AppState, current_source: &str, candidate_source: &str) -> bool {
    if candidate_source.trim().is_empty() || state.operation_active.load(Ordering::Relaxed) {
        return false;
    }

    let current_source = current_source.trim();
    if current_source.is_empty() || same_source_path(current_source, candidate_source) {
        return true;
    }

    get_last_auto_source(state)
        .is_some_and(|last_auto| same_source_path(current_source, &last_auto))
}

fn invalidate_scan_for_source(state: &AppState, source: &str) -> Result<bool> {
    let Some(scan) = get_current_scan(state)? else {
        return Ok(false);
    };

    if same_source_path(&scan.drive_root.display().to_string(), source) {
        return Ok(false);
    }

    set_current_scan(state, None)?;
    Ok(true)
}

fn set_last_auto_source(state: &AppState, source: Option<String>) -> Result<()> {
    let mut guard = state
        .last_auto_source
        .lock()
        .map_err(|_| anyhow!("internal state error: auto source state unavailable"))?;
    *guard = source;
    Ok(())
}

fn get_last_auto_source(state: &AppState) -> Option<String> {
    state
        .last_auto_source
        .lock()
        .ok()
        .and_then(|guard| guard.clone())
}

fn primary_auto_scan_source(drives: &[app::drive_monitor::DetectedDrive]) -> String {
    drives
        .iter()
        .find(|drive| drive.is_removable)
        .map(|drive| drive.root.display().to_string())
        .unwrap_or_default()
}

fn primary_detected_signature(drives: &[app::drive_monitor::DetectedDrive]) -> String {
    drives
        .first()
        .map(|drive| {
            format!(
                "{}|{}|{}",
                drive.root.display(),
                drive.label,
                drive.free_bytes
            )
        })
        .unwrap_or_default()
}

fn format_detected_drives(drives: &[app::drive_monitor::DetectedDrive]) -> String {
    if drives.is_empty() {
        return "No candidate media cards detected.".to_string();
    }

    drives
        .iter()
        .map(|drive| {
            format!(
                "{} (free {})",
                drive.label,
                app::types::format_bytes(drive.free_bytes)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn same_source_path(left: &str, right: &str) -> bool {
    let left = left.trim();
    let right = right.trim();
    if left.is_empty() || right.is_empty() {
        return false;
    }

    #[cfg(windows)]
    {
        left.eq_ignore_ascii_case(right)
    }

    #[cfg(not(windows))]
    {
        left == right
    }
}

fn get_current_scan(state: &AppState) -> Result<Option<ScanSummary>> {
    state
        .current_scan
        .lock()
        .map(|guard| guard.clone())
        .map_err(|_| anyhow!("internal state error: scan cache unavailable"))
}

fn format_resume(resume: &app::types::ResumableEvent) -> String {
    format!(
        "{} -> {} ({:?}, {} of {} verified) at {}",
        resume.client_name,
        resume.event_name,
        resume.status,
        resume.verified_files,
        resume.total_files,
        resume.destination_path.display()
    )
}

fn format_history(events: &[app::types::HistoryEvent]) -> String {
    let mut text = String::from("Recent ingests:\n");
    for event in events {
        let completed_at = event.completed_at.as_deref().unwrap_or("in progress");
        text.push_str(&format!(
            "- {} / {} [{:?}] {} of {} verified, locked: {}, completed: {}, path: {}\n",
            event.client_name,
            event.event_name,
            event.status,
            event.verified_files,
            event.total_files,
            if event.locked { "yes" } else { "no" },
            completed_at,
            event.destination_path.display()
        ));
    }
    text
}

fn select_client<'a>(clients: &'a [ClientSummary], query: &str) -> &'a ClientSummary {
    clients
        .iter()
        .find(|client| client.name.eq_ignore_ascii_case(query.trim()))
        .unwrap_or(&clients[0])
}

fn format_client_suggestions(clients: &[ClientSummary], selected_name: &str) -> String {
    let mut text = format!("Client suggestions (selected: {}):\n", selected_name);
    for client in clients {
        text.push_str(&format!(
            "- {} -> {}\n",
            client.name,
            client.base_path.display()
        ));
    }
    text
}

fn apply_event_menu(ui: &AppWindow, options: &[EventOption], selected: &str) {
    let values = options
        .iter()
        .map(|option| slint::SharedString::from(option.name.clone()))
        .collect::<Vec<_>>();
    ui.set_event_menu(slint::ModelRc::new(slint::VecModel::from(values)));

    let selected_index = options
        .iter()
        .position(|option| option.name.eq_ignore_ascii_case(selected))
        .map(|index| index as i32)
        .unwrap_or(-1);
    ui.set_event_menu_index(selected_index);
}

fn format_event_options(options: &[EventOption], suggested: &str) -> String {
    let mut text = format!("Event options (suggested: {}):\n", suggested);
    for option in options {
        text.push_str(&format!(
            "- {}{}\n",
            option.name,
            if option.completed { " (Done)" } else { "" }
        ));
    }
    text
}

fn format_existing_copy_warning(paths: &[std::path::PathBuf]) -> String {
    let mut text = String::from(
        "Some scanned files already appear to be copied.\nDo you want to make another copy?\nExisting paths:\n",
    );
    for path in paths {
        text.push_str(&format!("- files are in {}\n", path.display()));
    }
    text
}

fn apply_mock_bootstrap(window: &AppWindow) {
    window.set_drives_text(mock_drive_text().into());
    window.set_source_drive("E:\\".into());
    window.set_client_name("Mock Client".into());
    window.set_event_name("Barat".into());
    window.set_camera_label("".into());
    window.set_report_text(mock_report_text().into());
    window.set_status_text("Mock mode enabled. No real files are scanned or copied.".into());
}

fn mock_drive_text() -> String {
    [
        "Sony A7 IV - E:\\ (128 GB, 64.8 GB free) · RAW 6 · JPG 3 · MP4 2",
        "Canon R6 - F:\\ (64 GB, 22.1 GB free) · CR3 4 · JPG 2",
        "Nikon Z6 II - G:\\ (128 GB, 79.0 GB free) · NEF 5 · MOV 1",
    ]
    .join("\n")
}

fn mock_report_text() -> String {
    [
        "Mock session preview:",
        "- Client: Mock Client",
        "- Event: Barat",
        "- Camera: Sony A7IV (SN-1234)",
        "- Planned destination: D:\\Projects\\Mock Client\\Barat\\Photos\\Cam A",
        "- Duplicate estimate: 1 file would be skipped",
        "- Lock mode: Attribute",
    ]
    .join("\n")
}

fn mock_scan_summary() -> ScanSummary {
    ScanSummary {
        manufacturer: "Sony".into(),
        drive_root: std::path::PathBuf::from(r"E:\"),
        card_label: Some("SONY_128GB".into()),
        total_size_bytes: 4_800_000_000,
        total_files: 12,
        raw_count: 6,
        jpg_count: 3,
        video_count: 2,
        audio_count: 1,
        skipped_count: 2,
        files: vec![
            app::types::ScannedFile {
                source_path: std::path::PathBuf::from(r"E:\DCIM\100MSDCF\A001.ARW"),
                relative_source_path: std::path::PathBuf::from(r"DCIM\100MSDCF\A001.ARW"),
                filename: "A001.ARW".into(),
                size_bytes: 45_000_000,
                source_modified: "2026-03-18T10:00:00Z".into(),
                source_created: "2026-03-18T10:00:00Z".into(),
                media_type: app::types::MediaType::PhotoRaw,
                camera_model: Some("Sony A7IV".into()),
                camera_serial: Some("1234".into()),
            },
            app::types::ScannedFile {
                source_path: std::path::PathBuf::from(r"E:\PRIVATE\M4ROOT\CLIP\C0001.MP4"),
                relative_source_path: std::path::PathBuf::from(r"PRIVATE\M4ROOT\CLIP\C0001.MP4"),
                filename: "C0001.MP4".into(),
                size_bytes: 1_200_000_000,
                source_modified: "2026-03-18T10:03:00Z".into(),
                source_created: "2026-03-18T10:03:00Z".into(),
                media_type: app::types::MediaType::Video,
                camera_model: Some("Sony A7IV".into()),
                camera_serial: Some("1234".into()),
            },
        ],
    }
}

fn simulate_mock_ingest(weak: slint::Weak<AppWindow>) {
    thread::spawn(move || {
        let steps: Vec<(&str, f32, f32, &str)> = vec![
            ("Mock preflight passed.", 0.0, 0.0, "Preflight..."),
            (
                "Mock copy: 3/12 files (1.2 GB / 4.8 GB).",
                0.25,
                0.0,
                "3 of 12 files (1.2 GB / 4.8 GB)",
            ),
            (
                "Mock copy: 8/12 files (3.7 GB / 4.8 GB).",
                0.77,
                0.0,
                "8 of 12 files (3.7 GB / 4.8 GB)",
            ),
            (
                "Mock copy: 12/12 files (4.8 GB / 4.8 GB).",
                1.0,
                0.5,
                "12 of 12 files · Verifying...",
            ),
            (
                "Mock verify: all copied files verified.",
                1.0,
                1.0,
                "12 of 12 verified",
            ),
            (
                "Mock ingest complete. 12/12 verified. No failures.",
                1.0,
                1.0,
                "Complete",
            ),
        ];
        for (text, pct, vpct, plabel) in steps {
            let t = text.to_string();
            let pl = plabel.to_string();
            let _ = weak.upgrade_in_event_loop(move |ui| {
                ui.set_status_text(t.into());
                ui.set_progress_percent(pct);
                ui.set_verify_percent(vpct);
                ui.set_progress_label(pl.into());
            });
            thread::sleep(Duration::from_millis(600));
        }
        let _ = weak.upgrade_in_event_loop(move |ui| {
            ui.set_report_text(
                [
                    "Mock Final Report",
                    "- Total files: 12",
                    "- Verified: 12",
                    "- Failed: 0",
                    "- Destination: D:\\Projects\\Mock Client\\Barat",
                    "- Safety status: Locked (simulated)",
                ]
                .join("\n")
                .into(),
            );
        });
    });
}
