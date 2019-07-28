use clap::{App, Arg};
use kvs::Result;
use log::{error, debug, info};
use std::error::Error;
use std::net::SocketAddr;
use std::net::{TcpListener, TcpStream};
use serde_json;
use kvs::{self, wire, KvStore};
use std::io::prelude::*;
use std::io::{self, BufReader};

fn try_main() -> Result<()> {
    let matches = App::new("kvs-server")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::with_name("addr_port")
                .long("addr")
                .value_name("IP:PORT")
                .help("Sets IP address and port to connect to")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("engine")
                .long("engine")
                .value_name("ENGINE-NAME")
                .help("Sets key-value store backend")
                .takes_value(true),
        )
        .get_matches();

    let addr: SocketAddr = matches
        .value_of("addr_port")
        .unwrap_or("127.0.0.1:4000")
        .parse()?;

    let engine = matches.value_of("engine").unwrap_or("kvs");

    info!("version: {}", env!("CARGO_PKG_VERSION"));
    info!("engine: {}", engine);
    info!("address: {}", addr);

    let mut store = KvStore::open(".")?;

    let l = TcpListener::bind(addr)?;
    for sr in l.incoming() {
        match handle_request(&mut store, sr) {
            Ok(_) => debug!("handled request successfully"),
            Err(err) => {
                // Errors that can not be forwarded back to clients are logged instead.
                error!("error while handling request: {}", err)
            },
        }
    }

    Ok(())
}

fn handle_request(store: &mut KvStore, maybe_stream: io::Result<TcpStream>) -> kvs::Result<()> {
    let mut stream = maybe_stream?;
    let mut rd = BufReader::new(&stream);
    let mut line = String::new();
    rd.read_line(&mut line)?;
    let cmd: wire::Request = serde_json::from_str(&line)?;
    debug!("handling request {:?}", cmd);
    match cmd {
        wire::Request::Get(key) => { 
            let reply = wire::Reply(store.get(key).map_err(|err| err.to_string()));
            debug!("replying {:?}", reply);
            let ser = serde_json::to_string(&reply)?;
            writeln!(stream, "{}", ser)?;
        },
    };
    Ok(())
}

fn main() {
    // TODO: verbose level hardcoded
    stderrlog::new()
        .module(module_path!())
        .verbosity(10)
        .init()
        .unwrap();
    match try_main() {
        Err(err) => {
            eprintln!("{}", err);
            let mut src_opt = err.source();
            while let Some(src) = src_opt {
                eprintln!("caused by: {}", src);
                src_opt = src.source();
            }
            std::process::exit(1);
        }
        Ok(_) => (),
    }
}
