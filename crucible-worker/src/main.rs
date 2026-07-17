mod cli;

use clap::Parser;

use cli::Cli;

fn main() {
    let args = Cli::parse();
    eprintln!("worker {} would connect to {}", args.worker_id, args.socket);
}
