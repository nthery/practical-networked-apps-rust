use clap::{App, AppSettings, Arg, SubCommand};
use kvs::{KvStore, Result};
use std::error::Error;

fn try_main() -> Result<()> {
    let matches = App::new("kvs")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::SubcommandRequired)
        .subcommand(SubCommand::with_name("get").arg(Arg::with_name("key").required(true).index(1)))
        .subcommand(
            SubCommand::with_name("set")
                .arg(Arg::with_name("key").required(true).index(1))
                .arg(Arg::with_name("value").required(true).index(2)),
        )
        .subcommand(SubCommand::with_name("rm").arg(Arg::with_name("key").required(true).index(1)))
        .get_matches();

    let mut store = KvStore::open("kv.db")?;

    match matches.subcommand() {
        ("get", Some(_smatches)) => {
            /*
            if let Some(val) = store.get(smatches.value_of("key").unwrap().to_owned()) {
                println!("{}", val);
            } else {
                eprintln!("unknown key");
                // TODO: exit with error
            }
            */
            unimplemented!()
        }
        ("set", Some(smatches)) => store.set(
            smatches.value_of("key").unwrap().to_owned(),
            smatches.value_of("value").unwrap().to_owned(),
        ),
        ("rm", Some(_smatches)) => {
            /*
            store.remove(
                smatches.value_of("key").unwrap().to_owned()
                );
            */
            unimplemented!()
        }
        _ => panic!("clap should have detected missing subcommand"),
    }
}

fn main() {
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
