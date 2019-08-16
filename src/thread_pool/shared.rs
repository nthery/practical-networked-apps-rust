//! ThreadPool implementation using shared queue.

// We consider throughout this module that mutex poisoning are fatal errors and so unwrap()
// LockResult.

use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use log::error;

use super::ThreadPool;
use crate::error::Result;

/// Messages sent to worker threads.
enum Msg {
    /// Execute this closure.
    Job(Box<dyn FnOnce() + Send + 'static>),

    /// Cleanup and exit.
    Stop,
}

/// Thread-safe message FIFO allowing consumers to block.
struct MsgQueue {
    /// Accumulates messages not yet processed by worker threads.
    msgs: Mutex<Vec<Msg>>,

    /// Signaled when a message is pushed in `msgs`.
    cv: Condvar,
}

impl MsgQueue {
    /// Offloads execution of `msg` to an arbitrary worker thread.
    pub fn push(&self, msg: Msg) {
        self.msgs.lock().unwrap().push(msg);
        self.cv.notify_one();
    }

    /// Returns oldest message in queue.
    pub fn pop(&self) -> Msg {
        let mut msgs = self.msgs.lock().unwrap();
        while msgs.is_empty() {
            msgs = self.cv.wait(msgs).unwrap();
        }
        msgs.pop().expect("message queue unexpectedly empty")
    }
}

/// An implementation of `ThreadPool` that uses a shared queue to communicate with worker threads.
pub struct SharedQueueThreadPool {
    /// Handles to all worker threads.
    ///
    /// We wrap the handles in an `Option` because `JoinHandle::join()` moves its handle thus
    /// leading to "cannot move out of borrowed content" errors when calling it from a function
    /// that borrows this structure.  `Option::take()` works around this.
    workers: Option<Vec<JoinHandle<()>>>,

    /// Number of worker threads.
    nworkers: usize,

    /// Queue to communicate with worker threads.
    queue: Arc<MsgQueue>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(nthreads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        assert!(nthreads > 0);

        let local_queue = Arc::new(MsgQueue {
            msgs: Mutex::new(Vec::new()),
            cv: Condvar::new(),
        });

        let mut workers = Vec::new();
        for _ in 0..nthreads {
            let remote_queue = local_queue.clone();
            workers.push(thread::spawn(move || worker_thread(remote_queue)));
        }

        Ok(SharedQueueThreadPool {
            workers: Some(workers),
            nworkers: nthreads as usize,
            queue: local_queue,
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() -> () + Send + 'static,
    {
        self.queue.push(Msg::Job(Box::new(job)));
    }
}

// Message pumping loop executed by each worker thread.
fn worker_thread(queue: Arc<MsgQueue>) {
    while let Msg::Job(job) = queue.pop() {
        // We catch panics in situ because this is simpler that having another thread monitoring
        // the state of all worker threads.  We guard closure execution only because we assume that
        // the pool implementation is correct.
        // TODO: Is is correct to AssertUnwindSafe()?
        {
            if panic::catch_unwind(AssertUnwindSafe(|| job())).is_err() {
                error!("closure executed by worker thread panicked");
            }
        }
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        // Ask all worker threads to stop.
        for _ in 0..self.nworkers {
            self.queue.push(Msg::Stop);
        }

        // Wait for all workers to complete.
        let workers = self.workers.take().unwrap();
        for w in workers {
            // A thread panic here denotes an internal error as we catch panics in
            // client-provided closures.
            w.join().expect("worker thread panic");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn one_worker_thread() {
        let (tx, rx) = mpsc::channel();
        let pool = SharedQueueThreadPool::new(1).unwrap();
        pool.spawn(move || tx.send(42).unwrap());
        assert_eq!(rx.recv().unwrap(), 42);
    }

    #[test]
    fn many_worker_thread() {
        let (tx, rx) = mpsc::channel();
        const NWORKERS: u32 = 64;
        const NJOBS: usize = NWORKERS as usize * 4;
        let pool = SharedQueueThreadPool::new(NWORKERS).unwrap();
        for _ in 0..NJOBS {
            let tx = tx.clone();
            pool.spawn(move || {
                tx.send(thread::current().id()).unwrap();
                thread::sleep(Duration::from_millis(50))
            });
        }

        let mut received = HashSet::new();
        for _ in 0..NJOBS {
            received.insert(dbg!(rx.recv().unwrap()));
        }

        // TODO: This assertion is flaky because theoretically a single worker thread could
        // process all closures.
        assert!(dbg!(received.len()) > 1);
    }
}
