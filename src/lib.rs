use std::fs;

mod wire;

mod error;
pub use error::KvError;
pub use error::Result;

mod store_be;
pub use store_be::KvStore;

mod engine;
pub use engine::KvsEngine;

mod client;
pub use client::KvsClient;
mod server;
pub use server::KvsServer;

pub mod thread_pool;

pub fn open_engine(name_opt: Option<&str>) -> Result<Box<dyn KvsEngine>> {
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
            Ok(Box::new(KvStore::open(&dirname)?))
        }
        "sled" => {
            /*
            let dirname = "pna-sled";
            fs::create_dir_all(&dirname)?;
            Ok(Box::new(SledKvsEngine::open(&dirname)?))
            */
            unimplemented!()
        }
        _ => Err(KvError::UnknownEngine),
    }
}
