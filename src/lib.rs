use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, prelude::*, BufReader, BufWriter, ErrorKind, SeekFrom};
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

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

const MAX_DEAD_ENTRIES: i32 = 16;

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
        let file = OpenOptions::new()
            .read(true)
            .open(&self.filename)
            .map_err(|err| self.io_to_kv_err(err))?;
        let mut rd = BufReader::new(&file);
        rd.seek(SeekFrom::Start(off))
            .map_err(|err| self.io_to_kv_err(err))?;
        let mut ser_val = String::new();
        rd.read_line(&mut ser_val)
            .map_err(|err| self.io_to_kv_err(err))?;
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
        let tmp_file = NamedTempFile::new_in(".").map_err(|err| self.io_to_kv_err(err))?;
        let mut tmp_wr = BufWriter::new(tmp_file.as_file());

        let mut new_map = Index::new();
        for (key, off) in &self.map {
            // TODO: read from open log
            let val = self.read_value_from_log(*off)?;
            let new_off =
                append_to_open_log(&mut tmp_wr, tmp_file.path(), Tag::Set, &key, Some(&val))?;
            // TODO: move keys from old map rather than clone them.
            new_map.insert(key.to_string(), new_off);
        }

        fs::rename(tmp_file.path(), &self.filename).map_err(|err| self.io_to_kv_err(err))?;
        self.map = new_map;
        self.dead_entries = 0;

        Ok(())
    }

    fn io_to_kv_err(&self, err: io::Error) -> KvError {
        KvError::Io(self.filename.clone(), err)
    }
}

fn load_map_from(path: &Path) -> Result<(Index, i32)> {
    let mut kvs = HashMap::new();
    let mut dead_entries = 0;

    let file = match OpenOptions::new().read(true).open(&path) {
        Ok(f) => f,
        Err(ref err) if err.kind() == ErrorKind::NotFound => return Ok((kvs, dead_entries)),
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
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .map_err(|err| io_to_kv_err(path, err))?;
    let mut wr = BufWriter::new(&file);
    append_to_open_log(&mut wr, path, tag, key, val_opt)
}

fn append_to_open_log(
    wr: &mut BufWriter<&File>,
    path: &Path,
    tag: Tag,
    key: &str,
    val_opt: Option<&str>,
) -> Result<u64> {
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

    // TODO: What if the write fails halfway through?
    wr.write_fmt(format_args!("{}\n", ser_hdr))
        .map_err(|err| io_to_kv_err(path, err))?;
    let off = wr
        .seek(SeekFrom::Current(0))
        .map_err(|err| io_to_kv_err(path, err))?;
    if ser_val_opt.is_some() {
        wr.write_fmt(format_args!("{}\n", ser_val_opt.unwrap()))
            .map_err(|err| io_to_kv_err(path, err))?;
    }

    Ok(off)
}

fn io_to_kv_err(path: &Path, err: io::Error) -> KvError {
    KvError::Io(path.to_path_buf(), err)
}
