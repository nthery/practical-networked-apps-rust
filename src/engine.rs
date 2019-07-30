use crate::error::Result;

// TODO: Most methods take String arguments because tests use str::to_owned().  There
// must be a better way.
pub trait KvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn get(&self, key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}
