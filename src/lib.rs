pub struct KvStore;

impl KvStore {
    pub fn new() -> KvStore {
        KvStore{}
    }

    pub fn set(&mut self, _key: String, _value: String) {
    }

    pub fn get(&self, _key: String) -> Option<String> {
        Some("".to_owned())
    }

    pub fn remove(&mut self, _key: String) {
    }
}
