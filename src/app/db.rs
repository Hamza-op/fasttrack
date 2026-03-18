use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};

use crate::app::settings::AppPaths;
use crate::app::types::{
    next_event_suggestion, ClientSummary, DuplicateRecord, EventOption, EventStatus,
    FileRecordSummary, FileStatus, HistoryEvent, MediaType, ResumableEvent, ScannedFile,
    StoredFileRecord,
};

const SCHEMA_VERSION: &str = "1";

#[derive(Clone)]
pub struct Database {
    connection: Arc<Mutex<Connection>>,
    paths: AppPaths,
}

impl Database {
    pub fn open(paths: &AppPaths) -> Result<Self> {
        paths.ensure()?;
        let connection = Connection::open(&paths.db_file)?;
        connection.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;
            PRAGMA busy_timeout = 5000;
            "#,
        )?;

        let database = Self {
            connection: Arc::new(Mutex::new(connection)),
            paths: paths.clone(),
        };
        database.migrate_legacy_schema()?;
        database.create_schema()?;
        database.ensure_integrity()?;
        Ok(database)
    }

    fn with_connection<F, T>(&self, func: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T>,
    {
        let guard = self
            .connection
            .lock()
            .map_err(|_| anyhow!("database mutex poisoned"))?;
        func(&guard)
    }

    fn create_schema(&self) -> Result<()> {
        self.with_connection(|connection| {
            connection.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS clients (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE COLLATE NOCASE,
                    base_path TEXT NOT NULL,
                    notes TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                    event_name TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'in_progress', 'completed', 'failed', 'partial')),
                    destination_path TEXT NOT NULL,
                    locked INTEGER NOT NULL DEFAULT 0,
                    started_at TEXT,
                    completed_at TEXT,
                    total_files INTEGER DEFAULT 0,
                    verified_files INTEGER DEFAULT 0,
                    failed_files INTEGER DEFAULT 0,
                    total_bytes INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
                    source_path TEXT NOT NULL,
                    dest_path TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    xxh3_hash TEXT,
                    source_modified TEXT NOT NULL,
                    source_created TEXT,
                    media_type TEXT NOT NULL CHECK(media_type IN ('photo_raw', 'photo_jpg', 'video', 'audio')),
                    camera_model TEXT,
                    camera_serial TEXT,
                    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'copying', 'verifying', 'verified', 'failed', 'skipped_duplicate')),
                    error_message TEXT,
                    copied_at TEXT
                );

                CREATE TABLE IF NOT EXISTS cameras (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    serial_number TEXT UNIQUE,
                    model_name TEXT NOT NULL,
                    user_label TEXT,
                    last_seen TEXT NOT NULL DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL DEFAULT (datetime('now')),
                    action TEXT NOT NULL,
                    details TEXT,
                    username TEXT
                );

                CREATE TABLE IF NOT EXISTS custom_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE COLLATE NOCASE,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS pending_locks (
                    path TEXT PRIMARY KEY,
                    recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
                );

                CREATE INDEX IF NOT EXISTS idx_files_event ON files(event_id);
                CREATE INDEX IF NOT EXISTS idx_files_hash ON files(xxh3_hash);
                CREATE INDEX IF NOT EXISTS idx_files_duplicate_lookup ON files(filename, size_bytes, source_created);
                CREATE INDEX IF NOT EXISTS idx_files_duplicate_identity_lookup ON files(size_bytes, source_created, source_modified, media_type);
                CREATE INDEX IF NOT EXISTS idx_files_event_status ON files(event_id, status);
                CREATE INDEX IF NOT EXISTS idx_events_client ON events(client_id);
                CREATE INDEX IF NOT EXISTS idx_events_completed_totals ON events(status, total_files, total_bytes);
                CREATE INDEX IF NOT EXISTS idx_clients_name ON clients(name);
                CREATE INDEX IF NOT EXISTS idx_custom_events_name ON custom_events(name);

                CREATE VIRTUAL TABLE IF NOT EXISTS clients_fts USING fts5(name, content='clients', content_rowid='id');

                CREATE TRIGGER IF NOT EXISTS clients_ai AFTER INSERT ON clients BEGIN
                    INSERT INTO clients_fts(rowid, name) VALUES (new.id, new.name);
                END;
                CREATE TRIGGER IF NOT EXISTS clients_ad AFTER DELETE ON clients BEGIN
                    INSERT INTO clients_fts(clients_fts, rowid, name) VALUES('delete', old.id, old.name);
                END;
                CREATE TRIGGER IF NOT EXISTS clients_au AFTER UPDATE ON clients BEGIN
                    INSERT INTO clients_fts(clients_fts, rowid, name) VALUES('delete', old.id, old.name);
                    INSERT INTO clients_fts(rowid, name) VALUES (new.id, new.name);
                END;
                "#,
            )?;

            upsert_schema_version(connection)?;
            Ok(())
        })
    }

    fn migrate_legacy_schema(&self) -> Result<()> {
        self.with_connection(|connection| {
            migrate_table_if_incompatible(
                connection,
                "clients",
                &[
                    "id",
                    "name",
                    "base_path",
                    "notes",
                    "created_at",
                    "updated_at",
                ],
            )?;
            migrate_table_if_incompatible(
                connection,
                "events",
                &[
                    "id",
                    "client_id",
                    "event_name",
                    "status",
                    "destination_path",
                    "locked",
                    "started_at",
                    "completed_at",
                    "total_files",
                    "verified_files",
                    "failed_files",
                    "total_bytes",
                ],
            )?;
            migrate_table_if_incompatible(
                connection,
                "files",
                &[
                    "id",
                    "event_id",
                    "source_path",
                    "dest_path",
                    "filename",
                    "size_bytes",
                    "xxh3_hash",
                    "source_modified",
                    "source_created",
                    "media_type",
                    "camera_model",
                    "camera_serial",
                    "status",
                    "error_message",
                    "copied_at",
                ],
            )?;
            migrate_table_if_incompatible(connection, "settings", &["key", "value"])?;
            Ok(())
        })
    }

    fn ensure_integrity(&self) -> Result<()> {
        let integrity = self.integrity_value()?;

        if integrity.eq_ignore_ascii_case("ok") {
            return Ok(());
        }

        if let Some(backup) = self.latest_backup()? {
            fs::copy(&backup, &self.paths.db_file)?;
        }

        Ok(())
    }

    pub fn check_integrity(&self) -> Result<()> {
        let integrity = self.quick_integrity_value()?;
        if integrity.eq_ignore_ascii_case("ok") {
            return Ok(());
        }
        let deep_integrity = self.integrity_value()?;
        if deep_integrity.eq_ignore_ascii_case("ok") {
            return Ok(());
        }
        Err(anyhow!("database integrity check failed: {deep_integrity}"))
    }

    fn integrity_value(&self) -> Result<String> {
        self.with_connection(|connection| {
            let value: String =
                connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
            Ok(value)
        })
    }

    fn quick_integrity_value(&self) -> Result<String> {
        self.with_connection(|connection| {
            let value: String =
                connection.query_row("PRAGMA quick_check(1)", [], |row| row.get(0))?;
            Ok(value)
        })
    }

    fn latest_backup(&self) -> Result<Option<PathBuf>> {
        let mut entries: Vec<PathBuf> = fs::read_dir(&self.paths.backups_dir)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("db"))
            .collect();
        entries.sort();
        Ok(entries.pop())
    }

    pub fn backup_database(&self) -> Result<()> {
        if !self.paths.db_file.exists() {
            return Ok(());
        }

        let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
        let backup = self
            .paths
            .backups_dir
            .join(format!("moon_ingest_backup_{timestamp}.db"));
        fs::copy(&self.paths.db_file, backup)?;

        let mut backups: Vec<PathBuf> = fs::read_dir(&self.paths.backups_dir)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .collect();
        backups.sort();
        while backups.len() > 5 {
            if let Some(path) = backups.first().cloned() {
                let _ = fs::remove_file(&path);
                backups.remove(0);
            }
        }
        Ok(())
    }

    pub fn find_client_base_path(&self, client_name: &str) -> Result<Option<PathBuf>> {
        self.with_connection(|connection| {
            let value = connection
                .query_row(
                    "SELECT base_path FROM clients WHERE name = ?1 COLLATE NOCASE",
                    params![client_name],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            Ok(value.map(PathBuf::from))
        })
    }

    pub fn search_clients(&self, query: &str, limit: usize) -> Result<Vec<ClientSummary>> {
        self.with_connection(|connection| {
            let normalized = query.trim();
            let mut clients = Vec::new();
            if normalized.is_empty() {
                let mut statement = connection.prepare(
                    r#"
                    SELECT name, base_path
                    FROM clients
                    ORDER BY updated_at DESC, name ASC
                    LIMIT ?1
                    "#,
                )?;
                let rows = statement.query_map(params![limit.max(1) as i64], |row| {
                    Ok(ClientSummary {
                        name: row.get(0)?,
                        base_path: PathBuf::from(row.get::<_, String>(1)?),
                    })
                })?;
                for row in rows {
                    clients.push(row?);
                }
            } else {
                let contains = format!("%{normalized}%");
                let mut statement = connection.prepare(
                    r#"
                    SELECT name, base_path
                    FROM clients
                    WHERE name LIKE ?1 COLLATE NOCASE
                    ORDER BY updated_at DESC, name ASC
                    LIMIT ?2
                    "#,
                )?;
                let rows = statement.query_map(params![contains, limit.max(1) as i64], |row| {
                    Ok(ClientSummary {
                        name: row.get(0)?,
                        base_path: PathBuf::from(row.get::<_, String>(1)?),
                    })
                })?;
                for row in rows {
                    clients.push(row?);
                }

                clients.sort_by(|left, right| {
                    let left_name = left.name.to_ascii_lowercase();
                    let right_name = right.name.to_ascii_lowercase();
                    let query = normalized.to_ascii_lowercase();

                    let left_rank = if left_name == query {
                        0
                    } else if left_name.starts_with(&query) {
                        1
                    } else {
                        2
                    };
                    let right_rank = if right_name == query {
                        0
                    } else if right_name.starts_with(&query) {
                        1
                    } else {
                        2
                    };
                    left_rank
                        .cmp(&right_rank)
                        .then_with(|| left_name.cmp(&right_name))
                });
                clients.truncate(limit.max(1));
            }
            Ok(clients)
        })
    }

    pub fn upsert_client(
        &self,
        client_name: &str,
        base_path: &Path,
        notes: Option<&str>,
    ) -> Result<i64> {
        self.with_connection(|connection| {
            connection.execute(
                r#"
                INSERT INTO clients(name, base_path, notes, updated_at)
                VALUES(?1, ?2, ?3, ?4)
                ON CONFLICT(name) DO UPDATE SET base_path = excluded.base_path, notes = excluded.notes, updated_at = excluded.updated_at
                "#,
                params![
                    client_name,
                    base_path.display().to_string(),
                    notes,
                    Utc::now().to_rfc3339()
                ],
            )?;

            let id = connection.query_row(
                "SELECT id FROM clients WHERE name = ?1 COLLATE NOCASE",
                params![client_name],
                |row| row.get(0),
            )?;
            Ok(id)
        })
    }

    pub fn remember_custom_event(&self, event_name: &str) -> Result<()> {
        if crate::app::types::EVENT_SEQUENCE
            .iter()
            .any(|value| value.eq_ignore_ascii_case(event_name))
        {
            return Ok(());
        }

        self.with_connection(|connection| {
            connection.execute(
                "INSERT OR IGNORE INTO custom_events(name) VALUES(?1)",
                params![event_name],
            )?;
            Ok(())
        })
    }

    pub fn suggest_next_event(&self, client_name: &str) -> Result<String> {
        let completed = self.with_connection(|connection| {
            let mut statement = connection.prepare(
                r#"
                SELECT e.event_name
                FROM events e
                JOIN clients c ON c.id = e.client_id
                WHERE c.name = ?1 COLLATE NOCASE
                  AND e.status = 'completed'
                "#,
            )?;
            let rows = statement.query_map(params![client_name], |row| row.get::<_, String>(0))?;
            let mut events = Vec::new();
            for row in rows {
                events.push(row?);
            }
            Ok(events)
        })?;

        Ok(next_event_suggestion(&completed))
    }

    pub fn event_options(&self, client_name: &str) -> Result<Vec<EventOption>> {
        self.with_connection(|connection| {
            let mut completed_statement = connection.prepare(
                r#"
                SELECT DISTINCT e.event_name
                FROM events e
                JOIN clients c ON c.id = e.client_id
                WHERE c.name = ?1 COLLATE NOCASE
                  AND e.status = 'completed'
                "#,
            )?;
            let completed_rows = completed_statement
                .query_map(params![client_name], |row| row.get::<_, String>(0))?;
            let mut completed = Vec::new();
            for row in completed_rows {
                completed.push(row?);
            }

            let mut options = crate::app::types::EVENT_SEQUENCE
                .iter()
                .map(|name| EventOption {
                    name: (*name).to_string(),
                    completed: completed.iter().any(|item| item.eq_ignore_ascii_case(name)),
                })
                .collect::<Vec<_>>();

            let mut custom_statement = connection
                .prepare("SELECT name FROM custom_events ORDER BY created_at ASC, name ASC")?;
            let custom_rows = custom_statement.query_map([], |row| row.get::<_, String>(0))?;
            for row in custom_rows {
                let name = row?;
                if options
                    .iter()
                    .all(|option| !option.name.eq_ignore_ascii_case(&name))
                {
                    options.push(EventOption {
                        completed: completed
                            .iter()
                            .any(|item| item.eq_ignore_ascii_case(&name)),
                        name,
                    });
                }
            }

            Ok(options)
        })
    }

    pub fn search_event_options(
        &self,
        client_name: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<EventOption>> {
        let normalized = query.trim().to_ascii_lowercase();
        let mut options = self.event_options(client_name)?;
        if normalized.is_empty() {
            options.truncate(limit.max(1));
            return Ok(options);
        }

        options.retain(|option| option.name.to_ascii_lowercase().contains(&normalized));
        options.sort_by(|left, right| {
            let left_name = left.name.to_ascii_lowercase();
            let right_name = right.name.to_ascii_lowercase();
            let left_rank = if left_name == normalized {
                0
            } else if left_name.starts_with(&normalized) {
                1
            } else {
                2
            };
            let right_rank = if right_name == normalized {
                0
            } else if right_name.starts_with(&normalized) {
                1
            } else {
                2
            };
            left_rank
                .cmp(&right_rank)
                .then_with(|| left_name.cmp(&right_name))
        });
        options.truncate(limit.max(1));
        Ok(options)
    }

    pub fn resumable_event(
        &self,
        client_name: &str,
        event_name: &str,
    ) -> Result<Option<ResumableEvent>> {
        self.with_connection(|connection| {
            let resumable = connection
                .query_row(
                    r#"
                    SELECT c.name, e.event_name, e.status, e.verified_files, e.total_files, e.destination_path
                    FROM events e
                    JOIN clients c ON c.id = e.client_id
                    WHERE c.name = ?1 COLLATE NOCASE
                      AND e.event_name = ?2
                      AND e.status IN ('in_progress', 'partial')
                    ORDER BY e.id DESC
                    LIMIT 1
                    "#,
                    params![client_name, event_name],
                    |row| {
                        Ok(ResumableEvent {
                            client_name: row.get(0)?,
                            event_name: row.get(1)?,
                            status: EventStatus::from_db(&row.get::<_, String>(2)?),
                            verified_files: row.get::<_, i64>(3)? as usize,
                            total_files: row.get::<_, i64>(4)? as usize,
                            destination_path: PathBuf::from(row.get::<_, String>(5)?),
                        })
                    },
                )
                .optional()?;
            Ok(resumable)
        })
    }

    pub fn pending_resumes(&self) -> Result<Vec<ResumableEvent>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                r#"
                SELECT c.name, e.event_name, e.status, e.verified_files, e.total_files, e.destination_path
                FROM events e
                JOIN clients c ON c.id = e.client_id
                WHERE e.status IN ('in_progress', 'partial')
                ORDER BY e.id DESC
                "#,
            )?;
            let rows = statement.query_map([], |row| {
                Ok(ResumableEvent {
                    client_name: row.get(0)?,
                    event_name: row.get(1)?,
                    status: EventStatus::from_db(&row.get::<_, String>(2)?),
                    verified_files: row.get::<_, i64>(3)? as usize,
                    total_files: row.get::<_, i64>(4)? as usize,
                    destination_path: PathBuf::from(row.get::<_, String>(5)?),
                })
            })?;

            let mut events = Vec::new();
            for row in rows {
                events.push(row?);
            }
            Ok(events)
        })
    }

    pub fn recent_events(&self, limit: usize) -> Result<Vec<HistoryEvent>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                r#"
                SELECT c.name, e.event_name, e.status, e.verified_files, e.total_files, e.locked, e.completed_at, e.destination_path
                FROM events e
                JOIN clients c ON c.id = e.client_id
                ORDER BY COALESCE(e.completed_at, e.started_at) DESC, e.id DESC
                LIMIT ?1
                "#,
            )?;
            let rows = statement.query_map(params![limit as i64], |row| {
                Ok(HistoryEvent {
                    client_name: row.get(0)?,
                    event_name: row.get(1)?,
                    status: EventStatus::from_db(&row.get::<_, String>(2)?),
                    verified_files: row.get::<_, i64>(3)? as usize,
                    total_files: row.get::<_, i64>(4)? as usize,
                    locked: row.get::<_, i64>(5)? != 0,
                    completed_at: row.get(6)?,
                    destination_path: PathBuf::from(row.get::<_, String>(7)?),
                })
            })?;

            let mut events = Vec::new();
            for row in rows {
                events.push(row?);
            }
            Ok(events)
        })
    }

    pub fn latest_event_for_client_event(
        &self,
        client_name: &str,
        event_name: &str,
    ) -> Result<Option<HistoryEvent>> {
        self.with_connection(|connection| {
            let event = connection
                .query_row(
                    r#"
                    SELECT c.name, e.event_name, e.status, e.verified_files, e.total_files, e.locked, e.completed_at, e.destination_path
                    FROM events e
                    JOIN clients c ON c.id = e.client_id
                    WHERE c.name = ?1 COLLATE NOCASE
                      AND e.event_name = ?2
                    ORDER BY COALESCE(e.completed_at, e.started_at) DESC, e.id DESC
                    LIMIT 1
                    "#,
                    params![client_name, event_name],
                    |row| {
                        Ok(HistoryEvent {
                            client_name: row.get(0)?,
                            event_name: row.get(1)?,
                            status: EventStatus::from_db(&row.get::<_, String>(2)?),
                            verified_files: row.get::<_, i64>(3)? as usize,
                            total_files: row.get::<_, i64>(4)? as usize,
                            locked: row.get::<_, i64>(5)? != 0,
                            completed_at: row.get(6)?,
                            destination_path: PathBuf::from(row.get::<_, String>(7)?),
                        })
                    },
                )
                .optional()?;
            Ok(event)
        })
    }

    pub fn prepare_event(
        &self,
        client_id: i64,
        event_name: &str,
        destination_path: &Path,
    ) -> Result<i64> {
        self.with_connection(|connection| {
            let existing = connection
                .query_row(
                    r#"
                    SELECT id
                    FROM events
                    WHERE client_id = ?1
                      AND event_name = ?2
                      AND status IN ('in_progress', 'partial')
                    ORDER BY id DESC
                    LIMIT 1
                    "#,
                    params![client_id, event_name],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?;

            if let Some(id) = existing {
                connection.execute(
                    "UPDATE events SET destination_path = ?2, status = 'in_progress' WHERE id = ?1",
                    params![id, destination_path.display().to_string()],
                )?;
                return Ok(id);
            }

            connection.execute(
                r#"
                INSERT INTO events(client_id, event_name, status, destination_path, started_at)
                VALUES(?1, ?2, 'in_progress', ?3, ?4)
                "#,
                params![
                    client_id,
                    event_name,
                    destination_path.display().to_string(),
                    Utc::now().to_rfc3339()
                ],
            )?;
            Ok(connection.last_insert_rowid())
        })
    }

    pub fn load_event_files(&self, event_id: i64) -> Result<HashMap<PathBuf, StoredFileRecord>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT source_path, dest_path, status, xxh3_hash, size_bytes, source_modified, source_created, media_type FROM files WHERE event_id = ?1",
            )?;
            let rows = statement.query_map(params![event_id], |row| {
                Ok((
                    PathBuf::from(row.get::<_, String>(0)?),
                    StoredFileRecord {
                        dest_path: PathBuf::from(row.get::<_, String>(1)?),
                        status: FileStatus::from_db(&row.get::<_, String>(2)?),
                        hash: row.get(3)?,
                        size_bytes: row.get::<_, i64>(4)? as u64,
                        source_modified: row.get(5)?,
                        source_created: row.get(6)?,
                        media_type: MediaType::from_db(&row.get::<_, String>(7)?),
                    },
                ))
            })?;

            let mut files = HashMap::new();
            for row in rows {
                let (path, record) = row?;
                files.insert(path, record);
            }
            Ok(files)
        })
    }

    pub fn find_duplicate_candidates_for_client(
        &self,
        client_name: &str,
        file: &ScannedFile,
    ) -> Result<Vec<DuplicateRecord>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                    r#"
                    SELECT e.event_name, f.xxh3_hash, f.dest_path
                    FROM files f
                    JOIN events e ON e.id = f.event_id
                    JOIN clients c ON c.id = e.client_id
                    WHERE c.name = ?1 COLLATE NOCASE
                      AND e.status = 'completed'
                      AND f.status = 'verified'
                      AND f.size_bytes = ?2
                      AND f.source_created = ?3
                      AND f.source_modified = ?4
                      AND f.media_type = ?5
                    ORDER BY f.id DESC
                    LIMIT 8
                    "#,
                )?;
            let rows = statement.query_map(
                params![
                    client_name,
                    file.size_bytes as i64,
                    file.source_created,
                    file.source_modified,
                    file.media_type.db_value(),
                ],
                |row| {
                    Ok(DuplicateRecord {
                        event_name: row.get(0)?,
                        status: FileStatus::Verified,
                        xxh3_hash: row.get(1)?,
                        dest_path: PathBuf::from(row.get::<_, String>(2)?),
                    })
                },
            )?;

            let mut duplicates = Vec::new();
            for row in rows {
                duplicates.push(row?);
            }
            Ok(duplicates)
        })
    }

    pub fn matching_event_roots_for_scan(
        &self,
        scan: &crate::app::types::ScanSummary,
        limit: usize,
    ) -> Result<Vec<PathBuf>> {
        self.with_connection(|connection| {
            let expected = scan
                .files
                .iter()
                .map(scan_signature)
                .collect::<HashSet<_>>();
            if expected.is_empty() {
                return Ok(Vec::new());
            }

            let mut candidate_statement = connection.prepare(
                r#"
                SELECT id, destination_path
                FROM events
                WHERE status = 'completed'
                  AND total_files = ?1
                  AND total_bytes = ?2
                ORDER BY COALESCE(completed_at, started_at) DESC, id DESC
                LIMIT 50
                "#,
            )?;
            let candidate_rows = candidate_statement.query_map(
                params![scan.total_files as i64, scan.total_size_bytes as i64],
                |row| Ok((row.get::<_, i64>(0)?, PathBuf::from(row.get::<_, String>(1)?))),
            )?;

            let mut files_statement = connection.prepare(
                r#"
                SELECT filename, size_bytes, source_created, source_modified, media_type, camera_model, camera_serial, dest_path, status
                FROM files
                WHERE event_id = ?1
                "#,
            )?;

            let hard_limit = limit.max(1);
            let mut roots = Vec::new();

            for candidate in candidate_rows {
                let (event_id, event_root) = candidate?;
                if !event_root.exists() {
                    continue;
                }

                let rows = files_statement.query_map(params![event_id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)? as u64,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        PathBuf::from(row.get::<_, String>(7)?),
                        row.get::<_, String>(8)?,
                    ))
                })?;

                let mut actual = HashSet::new();
                let mut all_present = true;
                for row in rows {
                    let (
                        filename,
                        size_bytes,
                        source_created,
                        source_modified,
                        media_type,
                        camera_model,
                        camera_serial,
                        dest_path,
                        status,
                    ) = row?;
                    if status != "verified" && status != "skipped_duplicate" {
                        all_present = false;
                        break;
                    }
                    if !dest_path.exists() {
                        all_present = false;
                        break;
                    }

                    actual.insert(db_signature(
                        &filename,
                        size_bytes,
                        &source_created,
                        &source_modified,
                        &media_type,
                        camera_model.as_deref(),
                        camera_serial.as_deref(),
                    ));
                }

                if !all_present {
                    continue;
                }
                if actual.len() == expected.len() && actual == expected {
                    roots.push(event_root);
                    if roots.len() >= hard_limit {
                        break;
                    }
                }
            }

            Ok(roots)
        })
    }

    pub fn ensure_file_row(
        &self,
        event_id: i64,
        file: &ScannedFile,
        dest_path: &Path,
        status: FileStatus,
    ) -> Result<()> {
        self.with_connection(|connection| {
            connection.execute(
                r#"
                INSERT INTO files(
                    event_id, source_path, dest_path, filename, size_bytes, source_modified, source_created,
                    media_type, camera_model, camera_serial, status
                )
                SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11
                WHERE NOT EXISTS (
                    SELECT 1 FROM files WHERE event_id = ?1 AND source_path = ?2
                )
                "#,
                params![
                    event_id,
                    file.source_path.display().to_string(),
                    dest_path.display().to_string(),
                    file.filename,
                    file.size_bytes as i64,
                    file.source_modified,
                    file.source_created,
                    file.media_type.db_value(),
                    file.camera_model,
                    file.camera_serial,
                    status.db_value(),
                ],
            )?;
            Ok(())
        })
    }

    pub fn update_file_status(
        &self,
        event_id: i64,
        source_path: &Path,
        dest_path: &Path,
        status: FileStatus,
        hash: Option<&str>,
        error_message: Option<&str>,
    ) -> Result<()> {
        self.with_connection(|connection| {
            connection.execute(
                r#"
                UPDATE files
                SET dest_path = ?3,
                    status = ?4,
                    xxh3_hash = COALESCE(?5, xxh3_hash),
                    error_message = ?6,
                    copied_at = ?7
                WHERE event_id = ?1 AND source_path = ?2
                "#,
                params![
                    event_id,
                    source_path.display().to_string(),
                    dest_path.display().to_string(),
                    status.db_value(),
                    hash,
                    error_message,
                    Utc::now().to_rfc3339()
                ],
            )?;
            Ok(())
        })
    }

    pub fn mark_event(
        &self,
        event_id: i64,
        status: EventStatus,
        destination_path: &Path,
        verified_files: usize,
        failed_files: usize,
        total_files: usize,
        total_bytes: u64,
        locked: bool,
    ) -> Result<()> {
        self.with_connection(|connection| {
            connection.execute(
                r#"
                UPDATE events
                SET status = ?2,
                    destination_path = ?3,
                    verified_files = ?4,
                    failed_files = ?5,
                    total_files = ?6,
                    total_bytes = ?7,
                    locked = ?8,
                    completed_at = CASE WHEN ?2 IN ('completed', 'failed', 'partial') THEN ?9 ELSE completed_at END
                WHERE id = ?1
                "#,
                params![
                    event_id,
                    status.db_value(),
                    destination_path.display().to_string(),
                    verified_files as i64,
                    failed_files as i64,
                    total_files as i64,
                    total_bytes as i64,
                    if locked { 1 } else { 0 },
                    Utc::now().to_rfc3339()
                ],
            )?;
            Ok(())
        })
    }

    pub fn record_audit(&self, action: &str, details: &str) -> Result<()> {
        self.with_connection(|connection| {
            connection.execute(
                "INSERT INTO audit_log(action, details, username) VALUES(?1, ?2, ?3)",
                params![action, details, whoami::username()],
            )?;
            Ok(())
        })
    }

    pub fn manifest_records(
        &self,
        event_id: i64,
        event_root: &Path,
    ) -> Result<Vec<FileRecordSummary>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                r#"
                SELECT filename, dest_path, size_bytes, xxh3_hash, source_modified, status
                FROM files
                WHERE event_id = ?1
                ORDER BY id ASC
                "#,
            )?;

            let rows = statement.query_map(params![event_id], |row| {
                let dest_path = PathBuf::from(row.get::<_, String>(1)?);
                let relative_path = dest_path
                    .strip_prefix(event_root)
                    .unwrap_or(&dest_path)
                    .display()
                    .to_string()
                    .replace('\\', "/");
                Ok(FileRecordSummary {
                    filename: row.get(0)?,
                    relative_path,
                    size_bytes: row.get::<_, i64>(2)? as u64,
                    xxh3_hash: row.get(3)?,
                    source_modified: row.get(4)?,
                    status: FileStatus::from_db(&row.get::<_, String>(5)?),
                })
            })?;

            let mut records = Vec::new();
            for row in rows {
                records.push(row?);
            }
            Ok(records)
        })
    }
}

fn scan_signature(file: &ScannedFile) -> String {
    db_signature(
        &file.filename,
        file.size_bytes,
        &file.source_created,
        &file.source_modified,
        file.media_type.db_value(),
        file.camera_model.as_deref(),
        file.camera_serial.as_deref(),
    )
}

fn db_signature(
    filename: &str,
    size_bytes: u64,
    source_created: &str,
    source_modified: &str,
    media_type: &str,
    camera_model: Option<&str>,
    camera_serial: Option<&str>,
) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}",
        filename,
        size_bytes,
        source_created,
        source_modified,
        media_type,
        camera_model.unwrap_or("").trim().to_ascii_lowercase(),
        camera_serial.unwrap_or("").trim().to_ascii_lowercase(),
    )
}

fn migrate_table_if_incompatible(
    connection: &Connection,
    table_name: &str,
    required_columns: &[&str],
) -> Result<()> {
    let columns = table_columns(connection, table_name)?;
    if columns.is_empty()
        || required_columns
            .iter()
            .all(|column| columns.iter().any(|value| value == column))
    {
        return Ok(());
    }

    let legacy_name = format!("{table_name}_legacy");
    let legacy_exists: Option<String> = connection
        .query_row(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?1",
            params![legacy_name],
            |row| row.get(0),
        )
        .optional()?;

    if legacy_exists.is_none() {
        connection.execute_batch(&format!(
            "ALTER TABLE {table_name} RENAME TO {legacy_name};"
        ))?;
    } else {
        connection.execute_batch(&format!("DROP TABLE IF EXISTS {table_name};"))?;
    }

    if table_name == "clients" {
        connection.execute_batch(
            r#"
            DROP TABLE IF EXISTS clients_fts;
            DROP TRIGGER IF EXISTS clients_ai;
            DROP TRIGGER IF EXISTS clients_ad;
            DROP TRIGGER IF EXISTS clients_au;
            "#,
        )?;
    }
    if table_name == "files" {
        connection.execute_batch(
            "DROP INDEX IF EXISTS idx_files_hash; DROP INDEX IF EXISTS idx_files_event;",
        )?;
    }
    if table_name == "events" {
        connection.execute_batch("DROP INDEX IF EXISTS idx_events_client;")?;
    }

    Ok(())
}

fn table_columns(connection: &Connection, table_name: &str) -> Result<Vec<String>> {
    let pragma = format!("PRAGMA table_info({table_name})");
    let mut statement = connection.prepare(&pragma)?;
    let rows = statement.query_map([], |row| row.get::<_, String>(1))?;
    let mut columns = Vec::new();
    for row in rows {
        columns.push(row?);
    }
    Ok(columns)
}

fn upsert_schema_version(connection: &Connection) -> Result<()> {
    let columns = table_columns(connection, "settings")?;
    if columns.iter().any(|column| column == "updated_at") {
        connection.execute(
            "INSERT OR REPLACE INTO settings(key, value, updated_at) VALUES('schema_version', ?1, ?2)",
            params![SCHEMA_VERSION, Utc::now().to_rfc3339()],
        )?;
    } else {
        connection.execute(
            "INSERT OR REPLACE INTO settings(key, value) VALUES('schema_version', ?1)",
            params![SCHEMA_VERSION],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use tempfile::tempdir;

    use crate::app::settings::AppPaths;
    use crate::app::types::{FileStatus, MediaType, ScannedFile};

    use super::Database;

    fn test_paths(root: &Path) -> AppPaths {
        let root_dir = root.join("MoonIngest");
        AppPaths {
            root_dir: root_dir.clone(),
            logs_dir: root_dir.join("logs"),
            backups_dir: root_dir.join("backups"),
            settings_file: root_dir.join("settings.toml"),
            db_file: root_dir.join("moon_ingest.db"),
            lock_file: root_dir.join(".lock"),
        }
    }

    fn sample_file(path: &Path, filename: &str) -> ScannedFile {
        ScannedFile {
            source_path: path.to_path_buf(),
            relative_source_path: PathBuf::from(filename),
            filename: filename.to_string(),
            size_bytes: 10,
            source_modified: "2026-01-01T00:00:00Z".into(),
            source_created: "2026-01-01T00:00:00Z".into(),
            media_type: MediaType::Video,
            camera_model: Some("Sony A7IV".into()),
            camera_serial: Some("12345".into()),
        }
    }

    #[test]
    fn finds_duplicate_candidates_by_identity_tuple_for_same_client() {
        let temp = tempdir().unwrap();
        let paths = test_paths(temp.path());
        fs::create_dir_all(&paths.root_dir).unwrap();
        let db = Database::open(&paths).unwrap();

        let client_id = db
            .upsert_client("Ahmed Khan", Path::new(r"D:\Projects"), None)
            .unwrap();
        let event_id = db
            .prepare_event(
                client_id,
                "Barat",
                Path::new(r"D:\Projects\Ahmed Khan\Barat"),
            )
            .unwrap();

        let file = sample_file(Path::new(r"E:\C0001.MP4"), "C0001.MP4");
        let dest = Path::new(r"D:\Projects\Ahmed Khan\Barat\Videos\Sony A7IV\C0001.MP4");
        db.ensure_file_row(event_id, &file, dest, FileStatus::Pending)
            .unwrap();
        db.update_file_status(
            event_id,
            &file.source_path,
            dest,
            FileStatus::Verified,
            Some("hash1"),
            None,
        )
        .unwrap();
        db.mark_event(
            event_id,
            crate::app::types::EventStatus::Completed,
            Path::new(r"D:\Projects\Ahmed Khan\Barat"),
            1,
            0,
            1,
            10,
            false,
        )
        .unwrap();

        let duplicates = db
            .find_duplicate_candidates_for_client("Ahmed Khan", &file)
            .unwrap();
        assert_eq!(duplicates.len(), 1);
        assert_eq!(duplicates[0].event_name, "Barat");
        assert!(matches!(duplicates[0].status, FileStatus::Verified));
    }

    #[test]
    fn event_options_include_custom_event() {
        let temp = tempdir().unwrap();
        let paths = test_paths(temp.path());
        fs::create_dir_all(&paths.root_dir).unwrap();
        let db = Database::open(&paths).unwrap();
        db.remember_custom_event("Nikkah").unwrap();

        let options = db.event_options("No Client").unwrap();
        assert!(options.iter().any(|option| option.name == "Nikkah"));
        assert!(options.iter().any(|option| option.name == "Ubtan"));
    }
}
