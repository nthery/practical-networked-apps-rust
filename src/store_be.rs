use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{prelude::*, BufReader, ErrorKind, SeekFrom};
use std::path::{Path, PathBuf};

use crate::engine::KvsEngine;
use crate::error::*;

type Index = HashMap<String, u64>;

#[derive(Clone)]
pub struct KvStore {
    filename: PathBuf,
    map: Index,
    _dead_entries: i32,
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

// TODO: resurect
// const MAX_DEAD_ENTRIES: i32 = 64;

impl KvsEngine for KvStore {
    fn open<P: AsRef<Path>>(path: P) -> Result<KvStore> {
        let filename = path.as_ref().join("kv.db");
        let (map, dead_entries) = load_map_from(&filename)?;
        Ok(KvStore {
            filename,
            map,
            _dead_entries: dead_entries,
        })
    }

    fn boxed_clone(&self) -> Box<dyn KvsEngine> {
        Box::new(self.clone())
    }

    fn set(&self, _key: String, _value: String) -> Result<()> {
        /*
        // Update the in-ram map if and only if on-disk log updated.
        let off = append_to_log(&self.filename, Tag::Set, &key, Some(&value))?;
        if self.map.insert(key, off).is_some() {
            self.add_dead_entry()?;
        }
        Ok(())
        */
        unimplemented!()
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        Ok(match self.map.get(&key) {
            Some(off) => Some(self.read_value_from_log(*off)?),
            None => None,
        })
    }

    fn remove(&self, _key: String) -> Result<()> {
        /*
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
        */
        unimplemented!()
    }
}

impl KvStore {
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

    /*
     TODO: resurect

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
    */
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

/*
TODO: resurect
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
*/

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reopen() -> Result<()> {
        let tmpdir = tempfile::tempdir()?;
        {
            let kvs = KvStore::open(&tmpdir)?;
            kvs.set("k".to_string(), "v".to_string())?;
        }
        let kvs2 = KvStore::open(&tmpdir)?;
        assert_eq!(kvs2.get("k".to_string())?, Some("v".to_string()));
        Ok(())
    }
}
