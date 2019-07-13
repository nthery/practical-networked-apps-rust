use std::fmt;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum Error {
    IoError(PathBuf, io::Error),
    SerdeError(serde_json::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Error::IoError(ref pathbuf, _) => write!(f, "kvs i/o error on {}", pathbuf.display()),
            Error::SerdeError(_) => write!(f, "kvs serialization error"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            Error::IoError(_, ref err) => Some(err),
            Error::SerdeError(ref err) => Some(err),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct KvStore {
    filename: PathBuf,
    file: File,
}

// TODO: Most methods take String arguments because tests use str::to_owned().  There
// must be a better way.
impl KvStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<KvStore> {
        let pathbuf = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path.as_ref())
            .map_err(|err| Error::IoError(pathbuf.clone(), err))?;

        Ok(KvStore {
            filename: pathbuf,
            file: file,
        })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        serde_json::to_writer(&self.file, &(key, value)).map_err(Error::SerdeError)
    }

    pub fn get(&self, _key: String) -> Result<Option<String>> {
        unimplemented!();
    }

    pub fn remove(&mut self, _key: String) -> Result<()> {
        unimplemented!();
    }
}
