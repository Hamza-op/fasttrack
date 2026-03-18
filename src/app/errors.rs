use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum MoonError {
    #[error("Failed to read source file '{path}': {source}")]
    SourceReadError {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Failed to write to destination '{path}': {source}")]
    DestWriteError {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Destination drive full: needed {needed} bytes, only {available} available")]
    DiskFull { needed: u64, available: u64 },

    #[error("Checksum mismatch for '{filename}': source={source_hash}, dest={dest_hash}")]
    ChecksumMismatch {
        filename: String,
        source_hash: String,
        dest_hash: String,
    },

    #[error("No recognized media found on drive {drive}")]
    NoMediaFound { drive: String },

    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("Cannot set attributes on '{path}': {source}")]
    AttributeError {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Invalid name: {reason}")]
    InvalidClientName { reason: String },

    #[error("Path too long ({length} chars, max {max}): {path}")]
    PathTooLong {
        path: String,
        length: usize,
        max: usize,
    },

    #[error("Scan cancelled by user")]
    ScanCancelled,

    #[error("Ingest cancelled by user after {copied} of {total} files")]
    Cancelled { copied: u32, total: u32 },
}
