use std::fs;

pub mod wire;

mod error;
pub use error::KvError;
pub use error::Result;

mod store;
pub use store::KvStore;

mod sled_be;
pub use sled_be::SledKvsEngine;

mod engine;
pub use engine::KvsEngine;

pub fn open_engine(name: &str) -> Result<Box<dyn KvsEngine>> {
    // TODO: detect existing engine data
    match name {
        "kvs" => {
            let dirname = "pna-kvs";
            fs::create_dir_all(&dirname)?;
            Ok(Box::new(KvStore::open(&dirname)?))
        }
        "sled" => {
            let dirname = "pna-sled";
            fs::create_dir_all(&dirname)?;
            Ok(Box::new(SledKvsEngine::open(&dirname)?))
        }
        _ => Err(KvError::UnknownEngine),
    }
}
