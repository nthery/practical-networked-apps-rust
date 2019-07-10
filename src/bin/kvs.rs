use clap::{App,Arg,SubCommand,AppSettings};

use kvs::KvStore;

fn main() {
    let matches = App::new("kvs")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::SubcommandRequired)
        .subcommand(SubCommand::with_name("get")
                    .arg(Arg::with_name("key")
                         .required(true)
                         .index(1)))
        .subcommand(SubCommand::with_name("set")
                    .arg(Arg::with_name("key")
                         .required(true)
                         .index(1))
                    .arg(Arg::with_name("value")
                         .required(true)
                         .index(2)))
        .subcommand(SubCommand::with_name("rm")
                    .arg(Arg::with_name("key")
                         .required(true)
                         .index(1)))
        .get_matches();

    let mut store = KvStore::new();

    match matches.subcommand() {
        ("get", Some(smatches)) => {
            if let Some(val) = store.get(smatches.value_of("key").unwrap().to_owned()) {
                println!("{}", val);
            } else {
                eprintln!("unknown key");
                // TODO: exit with error
            }
        }
        ("set", Some(smatches)) => {
            store.set(
                smatches.value_of("key").unwrap().to_owned(),
                smatches.value_of("value").unwrap().to_owned()
            );
        }
        ("rm", Some(smatches)) => {
            store.remove(
                smatches.value_of("key").unwrap().to_owned()
            );
        }
        _ => unimplemented!()
    }
}
