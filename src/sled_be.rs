use crate::engine::KvsEngine;
use crate::error::*;
use sled::Db;
use std::path::Path;

pub struct SledKvsEngine(Db);

impl SledKvsEngine {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<SledKvsEngine> {
        Ok(SledKvsEngine(Db::start_default(path.as_ref())?))
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.0
            .set(key.as_bytes(), value.as_bytes())
            .map(|_| ())
            .map_err(KvError::Sled)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        // TODO: is there a better way to convert the value to string?
        Ok(self
            .0
            .get(key.as_bytes())?
            .map(|val| String::from_utf8_lossy(val.as_ref()).to_string()))
    }

    fn remove(&mut self, key: String) -> Result<()> {
        match self.0.del(key.as_bytes())? {
            Some(_) => Ok(()),
            None => Err(KvError::KeyNotFound(key))
        }
    }
}
