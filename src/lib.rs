use std::collections::HashMap;

pub struct KvStore {
    items: HashMap<String, String>
}

// TODO: Most methods take String arguments because tests use str::to_owned().  There
// must be a better way.
impl KvStore {
    pub fn new() -> KvStore {
        KvStore{ items: HashMap::new() }
    }

    pub fn set(&mut self, key: String, value: String) {
        self.items.insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<String> {
        self.items.get(&key).map(|val| val.to_owned())
    }

    pub fn remove(&mut self, key: String) {
        self.items.remove(&key);
    }
}
