use crate::{wire, KvError, Result};
use log::debug;

use std::io::prelude::*;
use std::net::SocketAddr;
use std::net::TcpStream;

/// TCP/IP client connecting to key-value store server.
pub struct KvsClient {
    addr: SocketAddr,
}

impl KvsClient {
    /// Creates a new client connected to server at `addr`.
    pub fn new(addr: SocketAddr) -> Result<KvsClient> {
        Ok(KvsClient { addr })
    }

    pub fn get(&mut self, key: &str) -> Result<Option<String>> {
        self.send_recv(wire::Request::Get(key.to_string()))
    }

    pub fn set(&mut self, key: &str, val: &str) -> Result<()> {
        self.send_recv(wire::Request::Set(key.to_string(), val.to_string()))
            .map(|_| ())
    }

    pub fn rm(&mut self, key: &str) -> Result<()> {
        self.send_recv(wire::Request::Rm(key.to_string()))
            .map(|_| ())
    }

    /// Requests server to stop.
    ///
    /// When this function returns, the server has stopped all processing.
    pub fn shutdown(&mut self) -> Result<()> {
        self.send_recv(wire::Request::Shutdown).map(|_| ())
    }

    /// Sends request `req` to server and waits for reply.
    fn send_recv(&self, req: wire::Request) -> Result<Option<String>> {
        debug!("C: sending {:?}", req);
        // A socket is a vehicle for a single request and so must be created per-request.
        let mut stream = TcpStream::connect(self.addr)?;
        let ser_req = serde_json::to_string(&req)?;
        writeln!(stream, "{}", ser_req)?;
        let reply = serde_json::from_reader::<_, wire::Reply>(&mut stream)?;
        debug!("C: received: {:?}", reply);
        reply.0.map_err(KvError::Server)
    }
}
