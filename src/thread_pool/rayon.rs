//! ThreadPool implementation wrapping rayon crate's thread pool.

use super::ThreadPool;
use crate::error::*;
use rayon::ThreadPoolBuilder;

pub struct RayonThreadPool(rayon::ThreadPool);

impl ThreadPool for RayonThreadPool {
    fn new(nthreads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(RayonThreadPool(
            ThreadPoolBuilder::new()
                .num_threads(nthreads as usize)
                .build()?,
        ))
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.0.spawn(job);
    }
}
