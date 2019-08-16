use super::ThreadPool;
use crate::error::*;

pub struct RayonThreadPool;

impl ThreadPool for RayonThreadPool {
    fn new(_nthreads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        unimplemented!()
    }

    fn spawn<F>(&self, _job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        unimplemented!()
    }
}
