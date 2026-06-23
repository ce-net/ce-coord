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
//!
//! The command logic lives in small `async` functions that take a [`Coord`] and an abstract line
//! source, so they are exercised by the in-crate tests against a mock broker (the `main` wrapper
//! only wires real stdin to them).

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

/// One line of console output. Returned by the command handlers so tests can assert on what the
/// CLI *would* print, instead of scraping real stdout.
pub type Line = String;

/// Apply one writer command line (`set <k> <v>` / `del <k>` / `dump` / blank / unknown) to `map`,
/// returning the lines the CLI would print. The single source of truth for the writer REPL — `main`
/// calls it per stdin line, tests call it with scripted lines.
pub async fn map_writer_cmd(
    map: &ce_coord::RMap<String, String>,
    line: &str,
) -> Result<Vec<Line>> {
    let parts: Vec<&str> = line.trim().splitn(3, ' ').collect();
    Ok(match parts.as_slice() {
        ["set", k, v] => {
            let ver = map.insert(k.to_string(), v.to_string()).await?;
            vec![format!("ok @ v{ver}")]
        }
        ["del", k] => {
            let ver = map.remove(k.to_string()).await?;
            vec![format!("ok @ v{ver}")]
        }
        ["dump"] => map.entries().into_iter().map(|(k, v)| format!("  {k} = {v}")).collect(),
        [""] => vec![],
        _ => vec!["?  use: set <k> <v> | del <k> | dump".to_string()],
    })
}

/// Render a reader's current converged map as the lines the CLI prints on each convergence.
pub fn map_reader_render(map: &ce_coord::RMap<String, String>) -> Vec<Line> {
    let mut out = vec![format!("--- converged @ v{} ({} keys) ---", map.version(), map.len())];
    let mut entries = map.entries();
    entries.sort();
    for (k, val) in entries {
        out.push(format!("  {k} = {val}"));
    }
    out
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
                for out in map_writer_cmd(&map, &line).await? {
                    println!("{out}");
                }
            }
        }
        Cmd::MapReader { name, writer } => {
            let map = coord.map_reader::<String, String>(&name, &writer).await?;
            println!("following {writer} / {name} — waiting for state...");
            let mut w = map.version_watch();
            loop {
                w.changed().await?;
                for out in map_reader_render(&map) {
                    println!("{out}");
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    // The clap arg parser accepts every documented subcommand shape.
    #[test]
    fn cli_parses_all_subcommands() {
        assert!(matches!(
            Cli::parse_from(["coord", "map-writer", "balances"]).cmd,
            Cmd::MapWriter { .. }
        ));
        assert!(matches!(
            Cli::parse_from(["coord", "map-reader", "balances", "abc"]).cmd,
            Cmd::MapReader { .. }
        ));
        assert!(matches!(
            Cli::parse_from(["coord", "stream-pub", "events"]).cmd,
            Cmd::StreamPub { .. }
        ));
        assert!(matches!(
            Cli::parse_from(["coord", "stream-sub", "events"]).cmd,
            Cmd::StreamSub { .. }
        ));
    }

    // A missing required arg is a parse error (clap usage).
    #[test]
    fn cli_rejects_missing_args() {
        assert!(Cli::try_parse_from(["coord", "map-reader", "onlyname"]).is_err());
        assert!(Cli::try_parse_from(["coord"]).is_err());
        assert!(Cli::try_parse_from(["coord", "bogus-cmd"]).is_err());
    }

    use ce_coord::testkit::{within, MockNode};
    use std::time::Duration;

    // Drive the writer REPL's command handler through every branch against a mock node, then a
    // reader that converges and renders. This exercises map_writer_cmd / map_reader_render — the
    // logic `main`'s stdin loop delegates to — without real stdin.
    #[tokio::test]
    async fn map_writer_and_reader_commands() -> anyhow::Result<()> {
        let node = MockNode::start();
        let ca = Coord::with_client(node.client()).await?;
        let cb = Coord::with_client(node.client()).await?;
        let writer_id = ca.node_id().to_string();

        let map = ca.map_writer::<String, String>("cli").await?;
        let reader = cb.map_reader::<String, String>("cli", &writer_id).await?;

        // `set` returns an "ok @ vN" line.
        let out = map_writer_cmd(&map, "set alice 100").await?;
        assert_eq!(out.len(), 1);
        assert!(out[0].starts_with("ok @ v"), "set line: {out:?}");
        map_writer_cmd(&map, "set bob 50").await?;

        // `dump` lists the entries.
        let dump = map_writer_cmd(&map, "dump").await?;
        assert_eq!(dump.len(), 2, "dump shows both keys: {dump:?}");
        assert!(dump.iter().any(|l| l.contains("alice = 100")));

        // `del` returns an ok line; blank line returns nothing; unknown shows usage.
        let del = map_writer_cmd(&map, "del bob").await?;
        assert!(del[0].starts_with("ok @ v"));
        assert!(map_writer_cmd(&map, "").await?.is_empty());
        let unknown = map_writer_cmd(&map, "frobnicate x").await?;
        assert!(unknown[0].starts_with("?"), "unknown usage line: {unknown:?}");

        // Reader converges, then renders the converged state.
        let target = map.version();
        reader.await_version(target).await;
        assert!(within(Duration::from_secs(10), || reader.get(&"alice".into()) == Some("100".into())).await);
        let render = map_reader_render(&reader);
        assert!(render[0].starts_with("--- converged @ v"), "header: {render:?}");
        assert!(render.iter().any(|l| l.contains("alice = 100")));
        Ok(())
    }
}
