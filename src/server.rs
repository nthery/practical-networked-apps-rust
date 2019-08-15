use crate::{wire, KvsEngine, Result};
use log::{debug, error};
use std::io::{prelude::*, BufReader};
use std::net::{SocketAddr, TcpListener, TcpStream};

/// TCP/IP server handling requests from KvsClient instances.
pub struct KvsServer<'a> {
    listener: TcpListener,
    engine: &'a mut dyn KvsEngine,
}

impl KvsServer<'_> {
    /// Creates a new server listening for requests on `addr` and delegating requests to `engine`.
    pub fn new(engine: &mut dyn KvsEngine, addr: SocketAddr) -> Result<KvsServer> {
        Ok(KvsServer {
            listener: TcpListener::bind(addr)?,
            engine,
        })
    }

    /// Serves requests forever or until a fatal error occurs.
    pub fn run(&mut self) -> Result<()> {
        for sr in self.listener.incoming() {
            match self.handle_request(sr?) {
                Ok(_) => debug!("handled request successfully"),
                Err(err) => {
                    // Errors that can not be forwarded back to clients are logged instead.
                    error!("error while handling request: {}", err)
                }
            }
        }
        Ok(())
    }

    fn handle_request(&self, mut stream: TcpStream) -> Result<()> {
        let mut rd = BufReader::new(&stream);
        let mut line = String::new();
        rd.read_line(&mut line)?;
        let cmd: wire::Request = serde_json::from_str(&line)?;
        debug!("handling request {:?}", cmd);
        match cmd {
            wire::Request::Get(key) => {
                let reply = wire::Reply(self.engine.get(key).map_err(|err| err.to_string()));
                send_reply(&mut stream, reply)?;
            }
            wire::Request::Set(key, val) => {
                let reply = wire::Reply(
                    self.engine
                        .set(key, val)
                        .map(|_| None)
                        .map_err(|err| err.to_string()),
                );
                send_reply(&mut stream, reply)?;
            }
            wire::Request::Rm(key) => {
                let reply = wire::Reply(
                    self.engine
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
