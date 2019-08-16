use crate::{thread_pool::*, wire, KvsEngine, Result};
use log::{debug, error};
use std::io::{prelude::*, BufReader};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;

// TODO: Make it f(# core)?
const NTHREADS: u32 = 8;

/// TCP/IP server handling requests from KvsClient instances.
pub struct KvsServer {
    listener: TcpListener,
    engine: Arc<dyn KvsEngine>,
}

impl KvsServer {
    /// Creates a new server listening for requests on `addr` and delegating requests to `engine`.
    pub fn new(engine: Arc<dyn KvsEngine>, addr: SocketAddr) -> Result<KvsServer> {
        Ok(KvsServer {
            listener: TcpListener::bind(addr)?,
            engine,
        })
    }

    /// Serves requests forever or until a fatal error occurs.
    pub fn run(&mut self) -> Result<()> {
        let pool = SharedQueueThreadPool::new(NTHREADS)?;

        for stream in self.listener.incoming() {
            let engine = self.engine.clone();
            let stream = stream?;
            pool.spawn(move || {
                match Self::handle_request(engine.as_ref(), stream) {
                    Ok(_) => debug!("handled request successfully"),
                    Err(err) => {
                        // Errors that can not be forwarded back to clients are logged instead.
                        error!("error while handling request: {}", err)
                    }
                }
            })
        }
        Ok(())
    }

    fn handle_request(engine: &dyn KvsEngine, mut stream: TcpStream) -> Result<()> {
        let mut rd = BufReader::new(&stream);
        let mut line = String::new();
        rd.read_line(&mut line)?;
        let cmd: wire::Request = serde_json::from_str(&line)?;
        debug!("handling request {:?}", cmd);
        match cmd {
            wire::Request::Get(key) => {
                let reply = wire::Reply(engine.get(key).map_err(|err| err.to_string()));
                send_reply(&mut stream, reply)?;
            }
            wire::Request::Set(key, val) => {
                let reply = wire::Reply(
                    engine
                        .set(key, val)
                        .map(|_| None)
                        .map_err(|err| err.to_string()),
                );
                send_reply(&mut stream, reply)?;
            }
            wire::Request::Rm(key) => {
                let reply = wire::Reply(
                    engine
                        .remove(key)
                        .map(|_| None)
                        .map_err(|err| err.to_string()),
                );
                send_reply(&mut stream, reply)?;
            }
        };
        Ok(())
    }
}

fn send_reply(wr: &mut impl Write, r: wire::Reply) -> Result<()> {
    debug!("replying {:?}", r);
    let ser = serde_json::to_string(&r)?;
    writeln!(wr, "{}", ser)?;
    Ok(())
}
