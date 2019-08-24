use std::fs;
use std::path::{Path, PathBuf};

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
pub use thread_pool::{NaiveThreadPool, RayonThreadPool, SharedQueueThreadPool, ThreadPool};

/// All supported backends.
#[derive(PartialEq)]
pub enum EngineKind {
    Kvs,
    Sled,
}

/// Computes backend to use and directory where data should be stored.
///
/// This function implements the convoluted logic described in the spec.
///
/// TODO: This function is ugly because it pushes responsibility of engine creation to its callers.
/// A better interface would be a function that creates the engine and passes it to a closure
/// argument.  I tried doing that and miserably failed because the closure must be generic (take a
/// impl KvsEngine).
pub fn prepare_engine_creation(name_opt: Option<&str>) -> Result<(EngineKind, PathBuf)> {
    // Look for existing database if any in current directory.
    let mut on_disk_data: Option<EngineKind> = None;
    for entry in fs::read_dir(".")? {
        let found = match entry?.file_name().to_str() {
            Some("pna-kvs") => Some(EngineKind::Kvs),
            Some("pna-sled") => Some(EngineKind::Sled),
            _ => None,
        };
        if found.is_some() {
            on_disk_data = found;
            break;
        }
    }

    // Compute engine to use.
    let selected_kind = match name_opt {
        Some(name) => {
            let desired_kind = match name {
                "kvs" => EngineKind::Kvs,
                "sled" => EngineKind::Sled,
                _ => return Err(KvError::BadEngine),
            };
            if let Some(kind) = on_disk_data {
                if desired_kind != kind {
                    return Err(KvError::BadEngine);
                }
            };
            desired_kind
        }
        None => {
            if let Some(kind) = on_disk_data {
                kind
            } else {
                EngineKind::Kvs
            }
        }
    };

    // Make sub-directory that will hold data in case it does not exist already.
    let dir = Path::new(match selected_kind {
        EngineKind::Kvs => "pna-kvs",
        EngineKind::Sled => "pna-sled",
    });
    fs::create_dir_all(&dir)?;

    Ok((selected_kind, dir.to_path_buf()))
}
