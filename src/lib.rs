use std::collections::HashMap;
use std::path::Path;

#[derive(Debug)]
pub enum Error { Dummy }

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Default)]
pub struct KvStore {
    items: HashMap<String, String>,
}

// TODO: Most methods take String arguments because tests use str::to_owned().  There
// must be a better way.
impl KvStore {
    pub fn new() -> KvStore {
        KvStore::default()
    }

    pub fn open(_path: &Path) -> Result<KvStore> {
        unimplemented!();
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.items.insert(key, value);
        Ok(())
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.items.get(&key).map(|val| val.to_owned()))
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.items.remove(&key);
        Ok(())
    }
}
