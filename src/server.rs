use crate::{thread_pool::*, wire, KvsEngine, Result};
use log::{debug, error};
use std::io::{prelude::*, BufReader};
use std::net::{SocketAddr, TcpListener, TcpStream};

/// TCP/IP server handling requests from KvsClient instances.
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    listener: TcpListener,
    engine: E,
    thread_pool: P,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    /// Creates a new server listening for requests on `addr` and delegating requests to `engine`.
    pub fn new(engine: E, pool: P, addr: SocketAddr) -> Result<KvsServer<E, P>> {
        Ok(KvsServer {
            listener: TcpListener::bind(addr)?,
            engine,
            thread_pool: pool,
        })
    }

    /// Serves requests forever or until a fatal error occurs.
    pub fn run(&mut self) -> Result<()> {
        for stream in self.listener.incoming() {
            let engine = self.engine.clone();
            let stream = stream?;
            self.thread_pool.spawn(move || {
                match Self::handle_request(engine, stream) {
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

    fn handle_request(engine: E, mut stream: TcpStream) -> Result<()> {
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
