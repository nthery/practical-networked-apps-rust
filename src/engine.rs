use crate::error::Result;

// TODO: Most methods take String arguments because tests use str::to_owned().  There
// must be a better way.
pub trait KvsEngine: Send + Sync + 'static {
    /// Creates a new engine or loads content from an existing one.
    ///
    /// This function is not in the spec but it allows to factor out benchmarks.
    fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self>
    where
        Self: Sized;

    /// Returns a clone of this engine wrapped into a box.
    ///
    /// The spec calls for deriving this trait from Clone but this prevents from using trait
    /// objects (e.g. open_engine(...) -> Box<dyn KvsEngine).  So we add a layer of indirection: we
    /// implement Clone for Box<dyn KvsEngine> which calls this function.
    fn boxed_clone(&self) -> Box<dyn KvsEngine>;

    fn set(&self, key: String, value: String) -> Result<()>;
    fn get(&self, key: String) -> Result<Option<String>>;
    fn remove(&self, key: String) -> Result<()>;
}
