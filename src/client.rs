use crate::{wire, KvError, Result};
use log::debug;

use std::io::prelude::*;
use std::net::SocketAddr;
use std::net::TcpStream;

/// TCP/IP client connecting to key-value store server.
pub struct KvsClient {
    stream: TcpStream,
}

impl KvsClient {
    /// Creates a new client connected to server at `addr`.
    pub fn new(addr: SocketAddr) -> Result<KvsClient> {
        Ok(KvsClient {
            stream: TcpStream::connect(addr)?,
        })
    }

    pub fn get(&mut self, key: &str) -> Result<Option<String>> {
        let req = serde_json::to_string(&wire::Request::Get(key.to_string()))?;
        writeln!(self.stream, "{}", req)?;
        let reply = serde_json::from_reader::<_, wire::Reply>(&mut self.stream)?;
        debug!("received {:?}", reply);
        reply.0.map_err(KvError::Server)
    }

    pub fn set(&mut self, key: &str, val: &str) -> Result<()> {
        let req = serde_json::to_string(&wire::Request::Set(key.to_string(), val.to_string()))?;
        writeln!(self.stream, "{}", req)?;
        let reply = serde_json::from_reader::<_, wire::Reply>(&mut self.stream)?;
        debug!("received {:?}", reply);
        reply.0.map(|_| ()).map_err(KvError::Server)
    }

    pub fn rm(&mut self, key: &str) -> Result<()> {
        let req = serde_json::to_string(&wire::Request::Rm(key.to_string()))?;
        writeln!(self.stream, "{}", req)?;
        let reply = serde_json::from_reader::<_, wire::Reply>(&mut self.stream)?;
        debug!("received {:?}", reply);
        reply.0.map(|_| ()).map_err(KvError::Server)
    }

    /// Requests server to stop.
    ///
    /// When this function returns, the server has stopped all processing.
    pub fn shutdown(&mut self) -> Result<()> {
        let req = serde_json::to_string(&wire::Request::Shutdown)?;
        writeln!(self.stream, "{}", req)?;
        let reply = serde_json::from_reader::<_, wire::Reply>(&mut self.stream)?;
        debug!("received {:?}", reply);
        reply.0.map(|_| ()).map_err(KvError::Server)
    }
}
