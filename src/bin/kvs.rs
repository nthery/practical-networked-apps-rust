use clap::{App, AppSettings, Arg, SubCommand};
use kvs::{self, EngineKind, KvError, KvStore, KvsEngine, Result, SledKvsEngine};
use std::error::Error;

fn try_main() -> Result<()> {
    let matches = App::new("kvs")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::SubcommandRequired)
        .arg(
            Arg::with_name("engine")
                .long("engine")
                .value_name("ENGINE-NAME")
                .help("Sets key-value store backend")
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

    let (engine_kind, dir) = kvs::prepare_engine_creation(matches.value_of("engine"))?;
    match engine_kind {
        EngineKind::Kvs => handle_subcommand(matches, KvStore::open(dir)?),
        EngineKind::Sled => handle_subcommand(matches, SledKvsEngine::open(dir)?),
    }
}

fn handle_subcommand(matches: clap::ArgMatches, engine: impl KvsEngine) -> Result<()> {
    match matches.subcommand() {
        ("get", Some(smatches)) => match engine.get(smatches.value_of("key").unwrap().to_owned()) {
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
        ("set", Some(smatches)) => engine.set(
            smatches.value_of("key").unwrap().to_owned(),
            smatches.value_of("value").unwrap().to_owned(),
        ),
        ("rm", Some(smatches)) => engine.remove(smatches.value_of("key").unwrap().to_owned()),
        _ => panic!("clap should have detected missing subcommand"),
    }
}

fn main() {
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
