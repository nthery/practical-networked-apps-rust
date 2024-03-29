use std::convert::From;
use std::fmt;
use std::io;
use std::net;

// TODO: encapsulate in struct storing what operation failed (set...)?
#[derive(Debug)]
pub enum KvError {
    Io(io::Error),
    Serde(serde_json::Error),
    Sled(sled::Error),
    KeyNotFound(String),
    BadEngine,
    Server(String),
    UnknownEngine,
    Other(String),
}

impl From<serde_json::Error> for KvError {
    fn from(err: serde_json::Error) -> KvError {
        KvError::Serde(err)
    }
}

impl From<sled::Error> for KvError {
    fn from(err: sled::Error) -> KvError {
        KvError::Sled(err)
    }
}

impl From<io::Error> for KvError {
    fn from(err: io::Error) -> KvError {
        KvError::Io(err)
    }
}

impl From<net::AddrParseError> for KvError {
    fn from(err: net::AddrParseError) -> KvError {
        KvError::Other(format!("Error while parsing IP address: {}", err).to_owned())
    }
}

impl<T> From<std::sync::PoisonError<T>> for KvError {
    fn from(err: std::sync::PoisonError<T>) -> KvError {
        KvError::Other(format!("Error while locking mutex: {}", err).to_owned())
    }
}

impl From<rayon::ThreadPoolBuildError> for KvError {
    fn from(err: rayon::ThreadPoolBuildError) -> KvError {
        KvError::Other(format!("Error while creating rayon thread pool: {}", err).to_owned())
    }
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            KvError::Io(_) => write!(f, "I/O error"),
            KvError::Serde(_) => write!(f, "Serialization error"),
            KvError::Sled(_) => write!(f, "Sled error"),
            KvError::KeyNotFound(ref key) => write!(f, "Key not found: {}", key),
            KvError::BadEngine => write!(f, "Selected engine does not support data stored on disk"),
            KvError::Server(ref msg) => write!(f, "Server error: {}", msg),
            KvError::UnknownEngine => write!(f, "Unknown engine"),
            KvError::Other(ref err) => write!(f, "{}", err),
        }
    }
}

impl std::error::Error for KvError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            KvError::Io(ref err) => Some(err),
            KvError::Serde(ref err) => Some(err),
            KvError::Sled(ref err) => Some(err),
            KvError::KeyNotFound(_) => None,
            KvError::BadEngine => None,
            KvError::Server(_) => None,
            KvError::UnknownEngine => None,
            KvError::Other(_) => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, KvError>;
