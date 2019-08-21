use crate::engine::KvsEngine;
use crate::error::*;
use sled::Db;
use std::path::Path;

/// sled key-value store wrapper.
///
/// Note that sled::Db is a Sync type that is already reference-counted and thread-safe.
#[derive(Clone)]
pub struct SledKvsEngine(Db);

impl KvsEngine for SledKvsEngine {
    fn open<P: AsRef<Path>>(path: P) -> Result<SledKvsEngine> {
        Ok(SledKvsEngine(Db::start_default(path.as_ref())?))
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        self.0.set(key.as_bytes(), value.as_bytes())?;
        self.0.flush()?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        // TODO: is there a better way to convert the value to string?
        Ok(self
            .0
            .get(key.as_bytes())?
            .map(|val| String::from_utf8_lossy(val.as_ref()).to_string()))
    }

    fn remove(&self, key: String) -> Result<()> {
        if self.0.del(key.as_bytes())?.is_none() {
            return Err(KvError::KeyNotFound(key));
        }
        self.0.flush()?;
        Ok(())
    }
}
