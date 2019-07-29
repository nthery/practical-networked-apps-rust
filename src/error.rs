use std::convert::From;
use std::fmt;
use std::io;
use std::net;

// TODO: encapsulate in struct storing what operation failed (set...)?
#[derive(Debug)]
pub enum KvError {
    Io(io::Error),
    Serde(serde_json::Error),
    KeyNotFound(String),
    Server(String),
    Other(Box<dyn std::error::Error>),
}

impl From<serde_json::Error> for KvError {
    fn from(err: serde_json::Error) -> KvError {
        KvError::Serde(err)
    }
}

impl From<io::Error> for KvError {
    fn from(err: io::Error) -> KvError {
        KvError::Io(err)
    }
}

impl From<net::AddrParseError> for KvError {
    fn from(err: net::AddrParseError) -> KvError {
        KvError::Other(Box::new(err))
    }
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            KvError::Io(_) => write!(f, "I/O error"),
            KvError::Serde(_) => write!(f, "Serialization error"),
            KvError::KeyNotFound(ref key) => write!(f, "Key not found: {}", key),
            KvError::Server(ref msg) => write!(f, "Server error: {}", msg),
            KvError::Other(ref err) => write!(f, "Other error: {}", err),
        }
    }
}

impl std::error::Error for KvError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            KvError::Io(ref err) => Some(err),
            KvError::Serde(ref err) => Some(err),
            KvError::KeyNotFound(_) => None,
            KvError::Server(_) => None,
            KvError::Other(ref err) => err.source(),
        }
    }
}

pub type Result<T> = std::result::Result<T, KvError>;
