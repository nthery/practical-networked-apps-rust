// TODO: break down this file

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::From;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, prelude::*, BufReader, BufWriter, ErrorKind, SeekFrom};
use std::net;
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

pub struct KvsEngine;

pub mod wire {
    use serde::{Serialize, Deserialize};

    // TODO: Use &str instead of String
    #[derive(Debug, Serialize, Deserialize)]
    pub enum Request {
        Get(String)
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct Reply ( pub Result<Option<String>, String> );
}

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

type Index = HashMap<String, u64>;

pub struct KvStore {
    filename: PathBuf,
    map: Index,
    dead_entries: i32,
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

const MAX_DEAD_ENTRIES: i32 = 64;

// TODO: Most methods take String arguments because tests use str::to_owned().  There
// must be a better way.
impl KvStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<KvStore> {
        let filename = path.as_ref().join("kv.db");
        let (map, dead_entries) = load_map_from(&filename)?;
        Ok(KvStore {
            filename,
            map,
            dead_entries,
        })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // Update the in-ram map if and only if on-disk log updated.
        let off = append_to_log(&self.filename, Tag::Set, &key, Some(&value))?;
        if self.map.insert(key, off).is_some() {
            self.add_dead_entry()?;
        }
        Ok(())
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(match self.map.get(&key) {
            Some(off) => Some(self.read_value_from_log(*off)?),
            None => None,
        })
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.map.get(&key) {
            Some(_) => {
                // Update the in-ram map if and only if on-disk log updated.
                append_to_log(&self.filename, Tag::Rm, &key, None).and_then(|_| {
                    if self.map.remove(&key).is_some() {
                        self.add_dead_entry()?;
                    }
                    Ok(())
                })
            }
            None => Err(KvError::KeyNotFound(key)),
        }
    }

    fn read_value_from_log(&self, off: u64) -> Result<String> {
        let file = OpenOptions::new().read(true).open(&self.filename)?;
        let mut rd = BufReader::new(&file);
        self.read_value_from_open_log(&mut rd, off)
    }

    fn read_value_from_open_log(&self, rd: &mut BufReader<&File>, off: u64) -> Result<String> {
        rd.seek(SeekFrom::Start(off))?;
        let mut ser_val = String::new();
        rd.read_line(&mut ser_val)?;
        // TODO: For some reason the conversion from serde o KvError does not kick in here hence
        // the map_err() call.
        serde_json::from_str(&ser_val).map_err(KvError::Serde)
    }

    fn add_dead_entry(&mut self) -> Result<()> {
        self.dead_entries += 1;
        if self.dead_entries > MAX_DEAD_ENTRIES {
            self.compact_log()?;
        }
        Ok(())
    }

    fn compact_log(&mut self) -> Result<()> {
        let tmp_file = NamedTempFile::new_in(".")?;
        let mut tmp_wr = BufWriter::new(tmp_file.as_file());

        let old_file = File::open(&self.filename)?;
        let mut old_rd = BufReader::new(&old_file);

        let mut new_map = Index::new();
        for (key, off) in &self.map {
            let val = self.read_value_from_open_log(&mut old_rd, *off)?;
            let new_off = append_to_open_log(&mut tmp_wr, Tag::Set, &key, Some(&val))?;
            // TODO: move keys from old map rather than clone them.
            new_map.insert(key.to_string(), new_off);
        }

        fs::rename(tmp_file.path(), &self.filename)?;
        self.map = new_map;
        self.dead_entries = 0;

        Ok(())
    }
}

fn load_map_from(path: &Path) -> Result<(Index, i32)> {
    let mut kvs = HashMap::new();
    let mut dead_entries = 0;

    let file = match OpenOptions::new().read(true).open(&path) {
        Ok(f) => f,
        Err(ref err) if err.kind() == ErrorKind::NotFound => return Ok((kvs, dead_entries)),
        Err(err) => return Err(KvError::Io(err)),
    };
    let mut rd = BufReader::new(&file);

    loop {
        let mut ser_hdr = String::new();
        match rd.read_line(&mut ser_hdr) {
            Ok(0) => break,
            Err(err) => return Err(KvError::Io(err)),
            _ => (),
        };
        match serde_json::from_str::<Header>(&ser_hdr) {
            Ok(hdr) => match hdr.tag {
                Tag::Set => {
                    let off = rd.seek(SeekFrom::Current(0))?;
                    rd.seek(SeekFrom::Current(hdr.value_size as i64))?;
                    if kvs.insert(hdr.key.to_string(), off).is_some() {
                        dead_entries += 1;
                    }
                }
                Tag::Rm => {
                    if kvs.remove(hdr.key).is_some() {
                        dead_entries += 1;
                    }
                }
            },
            Err(err) => return Err(KvError::Serde(err)),
        }
    }

    Ok((kvs, dead_entries))
}

fn append_to_log(path: &Path, tag: Tag, key: &str, val_opt: Option<&str>) -> Result<u64> {
    let file = OpenOptions::new().append(true).create(true).open(path)?;
    let mut wr = BufWriter::new(&file);
    append_to_open_log(&mut wr, tag, key, val_opt)
}

fn append_to_open_log(
    wr: &mut BufWriter<&File>,
    tag: Tag,
    key: &str,
    val_opt: Option<&str>,
) -> Result<u64> {
    let ser_val_opt = match val_opt {
        Some(val) => Some(serde_json::to_string(val)?),
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

    let ser_hdr = serde_json::to_string(&hdr)?;

    // TODO: What if the write fails halfway through?
    wr.write_fmt(format_args!("{}\n", ser_hdr))?;
    let off = wr.seek(SeekFrom::Current(0))?;
    if ser_val_opt.is_some() {
        wr.write_fmt(format_args!("{}\n", ser_val_opt.unwrap()))?;
    }

    Ok(off)
}
