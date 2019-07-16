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
    map: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug)]
enum Tag {
    Set,
    Rm,
}

#[derive(Serialize, Deserialize, Debug)]
struct Header {
    tag: Tag,
    key: String,
    value_size: usize,
}

// TODO: Most methods take String arguments because tests use str::to_owned().  There
// must be a better way.
impl KvStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<KvStore> {
        let filename = path.as_ref().join("kv.db");
        let map = load_map_from(&filename)?;
        Ok(KvStore { filename, map })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // Update the in-ram map if and only if on-disk log updated.
        // TODO: optimize away clone() calls.
        self.append_to_log(Tag::Set, &key, Some(&value))?;
        self.map.insert(key, value);
        Ok(())
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.map.get(&key).map(|val| val.to_string()))
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.map.get(&key) {
            Some(_) => {
                // Update the in-ram map if and only if on-disk log updated.
                // TODO: optimize away clone() calls.
                self.append_to_log(Tag::Rm, &key, None).and_then(|()| {
                    self.map.remove(&key);
                    Ok(())
                })
            }
            None => Err(KvError::KeyNotFound(key)),
        }
    }

    fn append_to_log(&self, tag: Tag, key: &str, val_opt: Option<&str>) -> Result<()> {
        let ser_val_opt = match val_opt {
            Some(val) => Some(serde_json::to_string(val).map_err(KvError::Serde)?),
            None => None,
        };

        let hdr = Header {
            tag,
            key: key.to_string(),
            value_size: match ser_val_opt {
                Some(ref ser) => ser.len() + "\n".len(),
                None => 0,
            },
        };

        let ser_hdr = serde_json::to_string(&hdr).map_err(KvError::Serde)?;

        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.filename)
            .map_err(|err| self.io_to_kv_err(err))?;

        // TODO: What if the write fails halfway through?
        file.write_fmt(format_args!("{}\n", ser_hdr))
            .map_err(|err| self.io_to_kv_err(err))?;
        if ser_val_opt.is_some() {
            file.write_fmt(format_args!("{}\n", ser_val_opt.unwrap()))
                .map_err(|err| self.io_to_kv_err(err))?;
        }
        Ok(())
    }

    fn io_to_kv_err(&self, err: io::Error) -> KvError {
        KvError::Io(self.filename.clone(), err)
    }
}

fn load_map_from(path: &Path) -> Result<(HashMap<String, String>)> {
    let mut kvs = HashMap::new();

    let file = match OpenOptions::new().read(true).open(&path) {
        Ok(f) => f,
        Err(ref err) if err.kind() == ErrorKind::NotFound => return Ok(kvs),
        Err(err) => return Err(io_to_kv_err(path, err)),
    };
    let mut rd = BufReader::new(&file);

    loop {
        let mut ser_hdr = String::new();
        match rd.read_line(&mut ser_hdr) {
            Ok(0) => break,
            Err(err) => return Err(io_to_kv_err(path, err)),
            _ => (),
        };
        match serde_json::from_str::<Header>(&ser_hdr) {
            Ok(hdr) => match hdr.tag {
                Tag::Set => {
                    let mut ser_val = String::new();
                    match rd.read_line(&mut ser_val) {
                        Ok(0) => break,
                        Err(err) => return Err(io_to_kv_err(path, err)),
                        _ => (),
                    };
                    let val = serde_json::from_str::<String>(&ser_val).map_err(KvError::Serde)?;
                    kvs.insert(hdr.key, val);
                }
                Tag::Rm => {
                    kvs.remove(&hdr.key);
                }
            },
            Err(err) => return Err(KvError::Serde(err)),
        }
    }

    Ok(kvs)
}

fn io_to_kv_err(path: &Path, err: io::Error) -> KvError {
    KvError::Io(path.to_path_buf(), err)
}
