use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fs::OpenOptions;
use std::io::{self, prelude::*, BufReader, ErrorKind, SeekFrom};
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

type Index = HashMap<String, u64>;

pub struct KvStore {
    filename: PathBuf,
    map: Index,
}

#[derive(Serialize, Deserialize, Debug)]
enum Tag {
    Set,
    Rm,
}

#[derive(Serialize, Deserialize, Debug)]
struct Header<'a> {
    tag: Tag,
    key: &'a str,
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
        let off = append_to_log(&self.filename, Tag::Set, &key, Some(&value))?;
        self.map.insert(key, off);
        Ok(())
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(match self.map.get(&key) {
            Some(off) => {
                let file = OpenOptions::new()
                    .read(true)
                    .open(&self.filename)
                    .map_err(|err| self.io_to_kv_err(err))?;
                let mut rd = BufReader::new(&file);
                rd.seek(SeekFrom::Start(*off))
                    .map_err(|err| self.io_to_kv_err(err))?;
                let mut ser_val = String::new();
                rd.read_line(&mut ser_val)
                    .map_err(|err| self.io_to_kv_err(err))?;
                Some(serde_json::from_str(&ser_val).map_err(KvError::Serde)?)
            }
            None => None,
        })
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.map.get(&key) {
            Some(_) => {
                // Update the in-ram map if and only if on-disk log updated.
                append_to_log(&self.filename, Tag::Rm, &key, None).and_then(|_| {
                    self.map.remove(&key);
                    Ok(())
                })
            }
            None => Err(KvError::KeyNotFound(key)),
        }
    }

    fn io_to_kv_err(&self, err: io::Error) -> KvError {
        KvError::Io(self.filename.clone(), err)
    }
}

fn load_map_from(path: &Path) -> Result<Index> {
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
                    let off = rd
                        .seek(SeekFrom::Current(0))
                        .map_err(|err| io_to_kv_err(path, err))?;
                    rd.seek(SeekFrom::Current(hdr.value_size as i64))
                        .map_err(|err| io_to_kv_err(path, err))?;
                    kvs.insert(hdr.key.to_string(), off);
                }
                Tag::Rm => {
                    kvs.remove(hdr.key);
                }
            },
            Err(err) => return Err(KvError::Serde(err)),
        }
    }

    Ok(kvs)
}

fn append_to_log(path: &Path, tag: Tag, key: &str, val_opt: Option<&str>) -> Result<u64> {
    let ser_val_opt = match val_opt {
        Some(val) => Some(serde_json::to_string(val).map_err(KvError::Serde)?),
        None => None,
    };

    let hdr = Header {
        tag,
        key,
        value_size: match ser_val_opt {
            Some(ref ser) => ser.len() + "\n".len(),
            None => 0,
        },
    };

    let ser_hdr = serde_json::to_string(&hdr).map_err(KvError::Serde)?;

    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .map_err(|err| io_to_kv_err(path, err))?;

    // TODO: What if the write fails halfway through?
    file.write_fmt(format_args!("{}\n", ser_hdr))
        .map_err(|err| io_to_kv_err(path, err))?;
    let off = file
        .seek(SeekFrom::Current(0))
        .map_err(|err| io_to_kv_err(path, err))?;
    if ser_val_opt.is_some() {
        file.write_fmt(format_args!("{}\n", ser_val_opt.unwrap()))
            .map_err(|err| io_to_kv_err(path, err))?;
    }

    Ok(off)
}

fn io_to_kv_err(path: &Path, err: io::Error) -> KvError {
    KvError::Io(path.to_path_buf(), err)
}
