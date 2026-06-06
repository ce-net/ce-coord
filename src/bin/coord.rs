//! `coord` — a demo CLI for `ce-coord`. Drive a replicated map or a typed stream across two (or
//! more) CE nodes from the terminal, to see the SDK work end-to-end.
//!
//! Two-node replicated map:
//!   # node A (writer) — prints its NodeId; type `set k v`, `del k`, `dump`:
//!   coord map-writer balances
//!   # node B (reader) — pass node A's NodeId; prints the map whenever it converges:
//!   coord map-reader balances <writer-node-id>
//!
//! Typed stream:
//!   coord stream-sub events        # on one node
//!   coord stream-pub events        # on another; type lines, each is published

use anyhow::Result;
use ce_coord::Coord;
use clap::{Parser, Subcommand};
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Parser)]
#[command(name = "coord", about = "Replicated state + typed streams on the CE mesh", version)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Own a replicated map: read `set <k> <v>` / `del <k>` / `dump` from stdin.
    MapWriter { name: String },
    /// Follow a writer's replicated map; print it on every convergence.
    MapReader { name: String, writer: String },
    /// Publish each stdin line to a typed string stream.
    StreamPub { name: String },
    /// Print values arriving on a typed string stream.
    StreamSub { name: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let coord = Coord::connect().await?;

    match cli.cmd {
        Cmd::MapWriter { name } => {
            let map = coord.map_writer::<String, String>(&name).await?;
            println!("writer up. follow me with:\n  coord map-reader {name} {}", coord.node_id());
            println!("commands: `set <k> <v>`, `del <k>`, `dump`");
            let mut lines = BufReader::new(tokio::io::stdin()).lines();
            while let Some(line) = lines.next_line().await? {
                let parts: Vec<&str> = line.trim().splitn(3, ' ').collect();
                match parts.as_slice() {
                    ["set", k, v] => {
                        let ver = map.insert(k.to_string(), v.to_string()).await?;
                        println!("ok @ v{ver}");
                    }
                    ["del", k] => {
                        let ver = map.remove(k.to_string()).await?;
                        println!("ok @ v{ver}");
                    }
                    ["dump"] => {
                        for (k, v) in map.entries() {
                            println!("  {k} = {v}");
                        }
                    }
                    [""] => {}
                    _ => println!("?  use: set <k> <v> | del <k> | dump"),
                }
            }
        }
        Cmd::MapReader { name, writer } => {
            let map = coord.map_reader::<String, String>(&name, &writer).await?;
            println!("following {writer} / {name} — waiting for state...");
            let mut w = map.version_watch();
            loop {
                w.changed().await?;
                let v = map.version();
                println!("--- converged @ v{v} ({} keys) ---", map.len());
                for (k, val) in map.entries() {
                    println!("  {k} = {val}");
                }
            }
        }
        Cmd::StreamPub { name } => {
            let stream = coord.stream::<String>(&name).await?;
            println!("publishing to stream `{name}` — type lines:");
            let mut lines = BufReader::new(tokio::io::stdin()).lines();
            while let Some(line) = lines.next_line().await? {
                stream.publish(&line).await?;
            }
        }
        Cmd::StreamSub { name } => {
            let mut stream = coord.stream::<String>(&name).await?;
            println!("subscribed to stream `{name}` — waiting for values:");
            while let Some(item) = stream.next().await {
                println!("  {item}");
            }
        }
    }
    Ok(())
}
