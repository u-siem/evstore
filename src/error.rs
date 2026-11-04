//! Error type of the store.

use std::fmt;
use std::io;
use std::path::PathBuf;

/// Errors returned by [`Store`](crate::Store) operations.
#[derive(Debug)]
pub enum StoreError {
    /// An I/O operation on `path` failed.
    Io {
        /// File or directory involved.
        path: PathBuf,
        /// Underlying error.
        source: io::Error,
    },
    /// A file failed validation (bad magic, checksum or structure).
    Corrupt {
        /// File that failed validation.
        file: PathBuf,
        /// Byte offset where the problem was detected.
        offset: u64,
        /// What was wrong.
        reason: String,
    },
    /// Another process (or another `Store` in this process) holds the
    /// directory lock.
    Locked(PathBuf),
    /// The caller passed an invalid argument (for example a bad option).
    InvalidArgument(String),
}

impl StoreError {
    pub(crate) fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        StoreError::Io {
            path: path.into(),
            source,
        }
    }

    pub(crate) fn corrupt(
        file: impl Into<PathBuf>,
        offset: u64,
        reason: impl Into<String>,
    ) -> Self {
        StoreError::Corrupt {
            file: file.into(),
            offset,
            reason: reason.into(),
        }
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::Io { path, source } => {
                write!(f, "I/O error on {}: {source}", path.display())
            }
            StoreError::Corrupt {
                file,
                offset,
                reason,
            } => write!(
                f,
                "corrupt file {} at offset {offset}: {reason}",
                file.display()
            ),
            StoreError::Locked(dir) => write!(
                f,
                "store directory {} is locked by another owner",
                dir.display()
            ),
            StoreError::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StoreError::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Result alias for store operations.
pub type Result<T, E = StoreError> = std::result::Result<T, E>;
