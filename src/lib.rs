use std::fs;
use std::sync::Arc;

mod wire;

mod error;
pub use error::KvError;
pub use error::Result;

mod store_be;
pub use store_be::KvStore;

mod sled_be;
pub use sled_be::SledKvsEngine;

mod engine;
pub use engine::KvsEngine;

mod client;
pub use client::KvsClient;
mod server;
pub use server::KvsServer;

pub mod thread_pool;

/// Creates a new store or opens an existing one in the current directory.
///
/// TODO: Returning an Arc value is convenient for the KvStore engine but suboptimal for
/// SledKvsEngine because the latter already uses Arc values internally.
pub fn open_engine(name_opt: Option<&str>) -> Result<Arc<dyn KvsEngine>> {
    let mut data_found: Option<&str> = None;
    for entry in fs::read_dir(".")? {
        let dir_found = match entry?.file_name().to_str() {
            Some("pna-kvs") => Some("kvs"),
            Some("pna-sled") => Some("sled"),
            _ => None,
        };
        if dir_found.is_some() {
            if data_found.is_some() {
                return Err(KvError::BadEngine);
            }
            data_found = dir_found
        }
    }

    let name = match name_opt {
        Some(n) => {
            if let Some(d) = data_found {
                if n != d {
                    return Err(KvError::BadEngine);
                }
            }
            n
        }
        None => {
            if let Some(n) = data_found {
                n
            } else {
                "kvs"
            }
        }
    };

    match name {
        "kvs" => {
            let dirname = "pna-kvs";
            fs::create_dir_all(&dirname)?;
            Ok(Arc::new(KvStore::open(&dirname)?))
        }
        "sled" => {
            let dirname = "pna-sled";
            fs::create_dir_all(&dirname)?;
            Ok(Arc::new(SledKvsEngine::open(&dirname)?))
        }
        _ => Err(KvError::UnknownEngine),
    }
}
