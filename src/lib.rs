use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, prelude::*, BufReader, Seek, Write};
use std::path::{Path, PathBuf};

// TODO: encapsulate in struct storing what operation failed (set...)?
#[derive(Debug)]
pub enum KvError {
    Io(PathBuf, io::Error),
    Serde(serde_json::Error),
    KeyNotFound(String),
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            KvError::Io(ref pathbuf, _) => write!(f, "I/O error on {}", pathbuf.display()),
            KvError::Serde(_) => write!(f, "Serialization error"),
            KvError::KeyNotFound(ref key) => write!(f, "Key not found: {}", key),
        }
    }
}

impl std::error::Error for KvError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            KvError::Io(_, ref err) => Some(err),
            KvError::Serde(ref err) => Some(err),
            KvError::KeyNotFound(_) => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, KvError>;

pub struct KvStore {
    filename: PathBuf,
    file: File,
}

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set(String, String),
    Rm(String),
}

// TODO: Most methods take String arguments because tests use str::to_owned().  There
// must be a better way.
impl KvStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<KvStore> {
        let pathbuf = path.as_ref().join("kv.db");
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&pathbuf)
            .map_err(|err| KvError::Io(pathbuf.clone(), err))?;

        Ok(KvStore {
            filename: pathbuf,
            file,
        })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.file
            .seek(io::SeekFrom::End(0))
            .map_err(|err| KvError::Io(self.filename.clone(), err))?;
        let ser = serde_json::to_string(&Command::Set(key, value)).map_err(KvError::Serde)?;
        self.file
            .write_fmt(format_args!("{}\n", ser))
            .map_err(|err| self.io_to_kv_err(err))
    }

    pub fn get(&self, _key: String) -> Result<Option<String>> {
        unimplemented!();
    }

    pub fn remove(&mut self, rmkey: String) -> Result<()> {
        let mut kvs = HashMap::new();
        self.file
            .seek(io::SeekFrom::Start(0))
            .map_err(|err| KvError::Io(self.filename.clone(), err))?;
        let mut rd = BufReader::new(&self.file);
        loop {
            let mut ser = String::new();
            match rd.read_line(&mut ser) {
                Ok(0) => break,
                Err(err) => return Err(self.io_to_kv_err(err)),
                _ => (),
            }
            match serde_json::from_str::<Command>(&ser) {
                Ok(Command::Set(key, value)) => {
                    kvs.insert(key, value);
                }
                Ok(Command::Rm(key)) => {
                    kvs.remove(&key);
                }
                Err(err) => return Err(KvError::Serde(err)),
            }
        }

        if kvs.get(&rmkey).is_some() {
            let ser = serde_json::to_string(&Command::Rm(rmkey)).map_err(KvError::Serde)?;
            self.file
                .write_fmt(format_args!("{}\n", ser))
                .map_err(|err| self.io_to_kv_err(err))?;
            Ok(())
        } else {
            Err(KvError::KeyNotFound(rmkey))
        }
    }

    fn io_to_kv_err(&self, err: io::Error) -> KvError {
        KvError::Io(self.filename.clone(), err)
    }
}
