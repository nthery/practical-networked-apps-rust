use crate::error::Result;

// TODO: Most methods take String arguments because tests use str::to_owned().  There
// must be a better way.
pub trait KvsEngine {
    /// Creates a new engine or loads content from an existing one.
    ///
    /// This function is not in the spec but it allows to factor out benchmarks.
    fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self>
    where
        Self: Sized;

    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn get(&self, key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}
