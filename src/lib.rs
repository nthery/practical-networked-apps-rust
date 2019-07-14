use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fs::OpenOptions;
use std::io::{self, prelude::*, BufReader, ErrorKind};
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
        Ok(KvStore {
            filename: path.as_ref().join("kv.db"),
        })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.append_to_log(Command::Set(key, value))
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        let kvs = self.load_map()?;
        Ok(kvs.get(&key).map(|val| val.to_string()))
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let kvs = self.load_map()?;
        match kvs.get(&key) {
            Some(_) => self.append_to_log(Command::Rm(key)),
            None => Err(KvError::KeyNotFound(key)),
        }
    }

    fn append_to_log(&self, cmd: Command) -> Result<()> {
        let ser = serde_json::to_string(&cmd).map_err(KvError::Serde)?;
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.filename)
            .map_err(|err| self.io_to_kv_err(err))?;

        file.write_fmt(format_args!("{}\n", ser))
            .map_err(|err| self.io_to_kv_err(err))
    }

    fn io_to_kv_err(&self, err: io::Error) -> KvError {
        KvError::Io(self.filename.clone(), err)
    }

    fn load_map(&self) -> Result<(HashMap<String, String>)> {
        let mut kvs = HashMap::new();

        let file = match OpenOptions::new().read(true).open(&self.filename) {
            Ok(f) => f,
            Err(ref err) if err.kind() == ErrorKind::NotFound => return Ok(kvs),
            Err(err) => return Err(self.io_to_kv_err(err)),
        };
        let mut rd = BufReader::new(&file);

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

        Ok(kvs)
    }
}
