pub mod wire;

mod error;
pub use error::KvError;
pub use error::Result;

mod store;
pub use store::KvStore;

pub struct KvsEngine;


