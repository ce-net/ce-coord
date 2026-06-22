//! Ephemeral CE node harness for ce-coord live integration tests.
//!
//! Spawns one or two real `ce` nodes in `--ephemeral --no-mine --no-mdns` mode on loopback,
//! polls `/health` until ready, reads the node-written `api.token`, and hands back wired
//! [`CeClient`]s. Nodes and their temp data dirs are killed/removed on `Drop`.
//!
//! Tests call [`live_available`] first: if the release `ce` binary isn't present (e.g. CI without a
//! built node), the test logs WHY and returns early rather than failing — so the suite stays green
//! on machines that genuinely can't run a node, while running for real where one exists.

#![allow(dead_code)]

use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

use ce_rs::CeClient;

/// The release binary the orchestrator builds into the shared target dir.
const CE_BIN: &str = "/Users/07lead01/ce-net/.cargo-shared/release/ce";

/// Port allocators inside the ranges the task reserved (api 18900-18999, p2p 14900-14999).
static API_PORT: AtomicU16 = AtomicU16::new(18900);
static P2P_PORT: AtomicU16 = AtomicU16::new(14900);

fn next_api_port() -> u16 {
    free_port(&API_PORT, 18900, 18999)
}
fn next_p2p_port() -> u16 {
    free_port(&P2P_PORT, 14900, 14999)
}

// Claim the next port in [lo,hi] that actually binds, so parallel tests don't collide.
fn free_port(ctr: &AtomicU16, lo: u16, hi: u16) -> u16 {
    for _ in 0..(hi - lo + 1) {
        let mut p = ctr.fetch_add(1, Ordering::SeqCst);
        if p > hi {
            ctr.store(lo, Ordering::SeqCst);
            p = lo;
        }
        if TcpListener::bind(("127.0.0.1", p)).is_ok() {
            return p;
        }
    }
    lo
}

/// Is a live node runnable here? `false` (with a logged reason) means tests should skip gracefully.
pub fn live_available() -> bool {
    if std::env::var("CE_COORD_NO_LIVE").is_ok() {
        eprintln!("[harness] CE_COORD_NO_LIVE set — skipping live tests");
        return false;
    }
    if !Path::new(CE_BIN).exists() {
        eprintln!("[harness] release ce binary not found at {CE_BIN} — skipping live tests");
        return false;
    }
    true
}

/// A running ephemeral node: process handle, data dir, and a wired client.
pub struct Node {
    child: Child,
    data_dir: PathBuf,
    pub api_port: u16,
    pub p2p_port: u16,
    pub node_id: String,
    /// The dialable loopback multiaddr from this node's `/bootstrap` (includes its peer id).
    pub dial: String,
    pub client: CeClient,
}

impl Node {
    /// Start an ephemeral node. `bootstrap` is an optional multiaddr to dial (for a 2-node mesh).
    pub async fn start(bootstrap: Option<&str>) -> anyhow::Result<Node> {
        let data_dir = std::env::temp_dir().join(format!(
            "ce-coord-test-{}-{}",
            std::process::id(),
            API_PORT.load(Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&data_dir)?;
        let api_port = next_api_port();
        let p2p_port = next_p2p_port();

        let mut cmd = Command::new(CE_BIN);
        // --data-dir is GLOBAL: before the subcommand.
        cmd.arg("--data-dir")
            .arg(&data_dir)
            .arg("start")
            .arg("--no-mine")
            .arg("--ephemeral")
            .arg("--no-mdns")
            .arg("--api-port")
            .arg(api_port.to_string())
            .arg("--port")
            .arg(p2p_port.to_string());
        if let Some(b) = bootstrap {
            cmd.arg("--bootstrap").arg(b);
        }
        // Make /bootstrap return a dialable loopback multiaddr, and keep the node off the public net.
        cmd.env("CE_EXTERNAL_IP", "127.0.0.1");
        cmd.env("CE_NO_AUTOBOOTSTRAP", "1");
        cmd.stdout(Stdio::null()).stderr(Stdio::null());

        let child = cmd.spawn()?;
        let base = format!("http://127.0.0.1:{api_port}");

        // Poll /health until the node answers (or time out).
        let deadline = Instant::now() + Duration::from_secs(40);
        let probe = CeClient::with_token(&base, None);
        loop {
            if probe.health().await.unwrap_or(false) {
                break;
            }
            if Instant::now() > deadline {
                anyhow::bail!("node on api port {api_port} did not become healthy");
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }

        // Read the api.token the node wrote, build an authenticated client.
        let token = read_token(&data_dir, Duration::from_secs(5)).await;
        let client = CeClient::with_token(&base, token);
        let status = client.status().await?;

        // Fetch the dialable multiaddr from /bootstrap. With CE_EXTERNAL_IP=127.0.0.1 set, the node
        // advertises /ip4/127.0.0.1/tcp/<p2p_port>/p2p/<peer_id> — exactly what a peer dials.
        let dial = fetch_dial_addr(&base, p2p_port)
            .await
            .ok_or_else(|| anyhow::anyhow!("could not learn dial addr from /bootstrap"))?;

        Ok(Node { child, data_dir, api_port, p2p_port, node_id: status.node_id, dial, client })
    }

    /// A loopback multiaddr other nodes can dial to bootstrap from this one.
    pub fn dial_addr(&self) -> String {
        self.dial.clone()
    }
}

#[derive(serde::Deserialize)]
struct BootstrapResp {
    peers: Vec<String>,
}

// Read /bootstrap and pick the loopback TCP multiaddr (it carries the peer id).
async fn fetch_dial_addr(base: &str, p2p_port: u16) -> Option<String> {
    let url = format!("{base}/bootstrap");
    let resp: BootstrapResp = reqwest::get(&url).await.ok()?.json().await.ok()?;
    // Prefer a full /ip4/127.0.0.1/.../tcp/<port>/p2p/<id> addr; fall back to building one from a
    // bare /p2p/<id> entry.
    for p in &resp.peers {
        if p.contains("/ip4/127.0.0.1/") && p.contains("/p2p/") {
            return Some(p.clone());
        }
    }
    for p in &resp.peers {
        if let Some(idx) = p.rfind("/p2p/") {
            let pid = &p[idx + 5..];
            if !pid.is_empty() {
                return Some(format!("/ip4/127.0.0.1/tcp/{p2p_port}/p2p/{pid}"));
            }
        }
    }
    None
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

async fn read_token(data_dir: &Path, timeout: Duration) -> Option<String> {
    let path = data_dir.join("api.token");
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(s) = std::fs::read_to_string(&path) {
            let s = s.trim().to_string();
            if !s.is_empty() {
                return Some(s);
            }
        }
        if Instant::now() > deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Wait until `peer_id` shows up as a connected peer in `client`'s atlas/status, or time out.
/// Best-effort: returns whether the link was observed.
pub async fn wait_mesh_link(a: &Node, b: &Node, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        // The atlas lists peers this node has heard capacity from; a connected peer eventually
        // appears. We also accept any successful directed request as proof of a path (done by
        // callers); here we just give the link time to form.
        if let Ok(atlas) = a.client.atlas().await {
            if atlas.iter().any(|e| e.node_id == b.node_id) {
                return true;
            }
        }
        if let Ok(atlas) = b.client.atlas().await {
            if atlas.iter().any(|e| e.node_id == a.node_id) {
                return true;
            }
        }
        if Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

// Silence unused-import warnings for helpers used only by some test files.
#[allow(unused)]
fn _keep(_: &mut dyn Write) {}
