use clap::{App, AppSettings, Arg, SubCommand};

fn main() {
    let _matches = App::new("kvs")
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

    panic!("unimplemented");
}
