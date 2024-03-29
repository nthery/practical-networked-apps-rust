use clap::{App, AppSettings, Arg, ArgSettings, SubCommand};
use kvs::{KvError, KvsClient, Result};

use std::error::Error;

use std::net::SocketAddr;

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

    let mut client = KvsClient::new(addr)?;

    match matches.subcommand() {
        ("get", Some(smatches)) => match client.get(smatches.value_of("key").unwrap()) {
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
        ("set", Some(smatches)) => client.set(
            smatches.value_of("key").unwrap(),
            smatches.value_of("value").unwrap(),
        ),
        ("rm", Some(smatches)) => client.rm(smatches.value_of("key").unwrap()),
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
