use std::fs;

pub mod wire;

mod error;
pub use error::KvError;
pub use error::Result;

mod store;
pub use store::KvStore;

mod engine;
pub use engine::KvsEngine;

pub fn open_engine(name: &str) -> Result<Box<dyn KvsEngine>> {
    match name {
        "kvs" => {
            let dirname = "pna-kvs";
            fs::create_dir_all(&dirname)?;
            Ok(Box::new(KvStore::open(&dirname)?))
        }
        _ => Err(KvError::UnknownEngine),
    }
}
