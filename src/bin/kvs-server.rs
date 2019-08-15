use clap::{App, Arg};
use kvs::{self, KvsServer, Result};
use log::info;

use std::error::Error;

use std::net::SocketAddr;

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

    let engine = matches.value_of("engine");

    info!("version: {}", env!("CARGO_PKG_VERSION"));
    info!("engine: {}", engine.unwrap_or("default"));
    info!("address: {}", addr);

    let store = kvs::open_engine(engine)?;
    KvsServer::new(store, addr)?.run()
}

fn main() {
    // TODO: verbose level hardcoded
    stderrlog::new()
        .module(module_path!())
        .verbosity(10)
        .init()
        .unwrap();
    if let Err(err) = try_main() {
        eprintln!("{}", err);
        let mut src_opt = err.source();
        while let Some(src) = src_opt {
            eprintln!("caused by: {}", src);
            src_opt = src.source();
        }
        std::process::exit(1);
    }
}
