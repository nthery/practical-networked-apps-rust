use clap::{App, AppSettings, Arg, ArgSettings, SubCommand};
use kvs::wire;
use kvs::{KvError, Result};
use log::debug;
use std::error::Error;
use std::io::prelude::*;
use std::net::SocketAddr;
use std::net::TcpStream;

fn do_get(stream: &mut TcpStream, key: &str) -> Result<Option<String>> {
    let req = serde_json::to_string(&wire::Request::Get(key.to_string()))?;
    writeln!(stream, "{}", req)?;
    let reply = serde_json::from_reader::<_, wire::Reply>(stream)?;
    debug!("received {:?}", reply);
    reply.0.map_err(KvError::Server)
}

fn do_set(stream: &mut TcpStream, key: &str, val: &str) -> Result<()> {
    let req = serde_json::to_string(&wire::Request::Set(key.to_string(), val.to_string()))?;
    writeln!(stream, "{}", req)?;
    let reply = serde_json::from_reader::<_, wire::Reply>(stream)?;
    debug!("received {:?}", reply);
    reply.0.map(|_| ()).map_err(KvError::Server)
}

fn do_rm(stream: &mut TcpStream, key: &str) -> Result<()> {
    let req = serde_json::to_string(&wire::Request::Rm(key.to_string()))?;
    writeln!(stream, "{}", req)?;
    let reply = serde_json::from_reader::<_, wire::Reply>(stream)?;
    debug!("received {:?}", reply);
    reply.0.map(|_| ()).map_err(KvError::Server)
}

fn try_main() -> Result<()> {
    let matches = App::new("kvs-client")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::SubcommandRequired)
        .arg(
            Arg::with_name("addr_port")
                .long("addr")
                .value_name("IP:PORT")
                .set(ArgSettings::Global)
                .help("Sets IP address and port to connect to")
                .takes_value(true),
        )
        .subcommand(SubCommand::with_name("get").arg(Arg::with_name("key").required(true).index(1)))
        .subcommand(
            SubCommand::with_name("set")
                .arg(Arg::with_name("key").required(true).index(1))
                .arg(Arg::with_name("value").required(true).index(2)),
        )
        .subcommand(SubCommand::with_name("rm").arg(Arg::with_name("key").required(true).index(1)))
        .get_matches();

    let addr: SocketAddr = matches
        .value_of("addr_port")
        .unwrap_or("127.0.0.1:4000")
        .parse()?;

    let mut stream = TcpStream::connect(addr)?;

    match matches.subcommand() {
        ("get", Some(smatches)) => match do_get(&mut stream, smatches.value_of("key").unwrap()) {
            Ok(Some(val)) => {
                println!("{}", val);
                Ok(())
            }
            Ok(None) => {
                println!("Key not found");
                Ok(())
            }
            Err(err) => Err(err),
        },
        ("set", Some(smatches)) => do_set(
            &mut stream,
            smatches.value_of("key").unwrap(),
            smatches.value_of("value").unwrap(),
        ),
        ("rm", Some(smatches)) => do_rm(&mut stream, smatches.value_of("key").unwrap()),
        _ => panic!("clap should have detected missing subcommand"),
    }
}

fn main() {
    // TODO: verbose level hardcoded
    stderrlog::new()
        .module(module_path!())
        .verbosity(10)
        .init()
        .unwrap();

    match try_main() {
        Err(KvError::KeyNotFound(_)) => {
            // The spec states that these errors go to stdout.
            println!("Key not found");
            std::process::exit(1);
        }
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
