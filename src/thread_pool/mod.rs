use crate::error::Result;

pub trait ThreadPool {
    fn new(nthreads: u32) -> Result<Self>
    where
        Self: Sized;
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

mod naive;
pub use naive::NaiveThreadPool;

mod shared;
pub use shared::SharedQueueThreadPool;

mod rayon;
pub use rayon::RayonThreadPool;
