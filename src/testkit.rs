//! In-process mock CE node — a pub/sub broker + content-addressed blob store over HTTP.
//!
//! ce-coord talks to a local CE node purely through `ce-rs`'s `CeClient` (plain HTTP). The live
//! tests need a real `ce` binary, which is absent on most CI machines, so the entire transport,
//! pump, collection, Merged, and snapshot machinery goes unmeasured. This harness closes that gap:
//! it stands up a tiny HTTP server in-process that implements exactly the handful of endpoints
//! ce-coord uses, lets several `CeClient`s (each a distinct "node", identified by its bearer token)
//! share one broker, and thereby drives the **real** ce-coord code end-to-end — propose/publish,
//! the inbox pump, directed catch-up request/reply, snapshot put_object/get_object — with
//! deterministic, fast, fully local delivery.
//!
//! It is intentionally a *broker*, not a CE node: there is no signing, no chain, no libp2p. The
//! `from` field on a delivered message is the sender node's id (its token-derived hex), which is the
//! one property ce-coord's reader-authentication relies on, so the followed-writer check is honoured.
//!
//! Endpoints implemented (all the surface `CeClient` calls from ce-coord):
//!   GET  /status                          -> { node_id, height, difficulty, balance }
//!   GET  /health                          -> 200
//!   POST /mesh/subscribe  { topic }       -> 200   (this node now receives that topic)
//!   POST /mesh/publish    { topic, payload_hex } -> 200  (fan out to every *other* subscriber)
//!   GET  /mesh/messages                   -> [ AppMessage ]   (drains this node's queue)
//!   POST /mesh/request    { to, topic, payload_hex, timeout_ms } -> { payload_hex }  (blocks for reply)
//!   POST /mesh/reply      { token, payload_hex } -> 200
//!   POST /blobs           <raw body>      -> { hash }   (content-addressed store)
//!   GET  /blobs/:hash                     -> raw bytes  (404 if absent)

#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use ce_rs::CeClient;
use sha2::{Digest, Sha256};

/// A pending directed request waiting for its reply (request/reply rendezvous).
struct Pending {
    reply: Mutex<Option<Vec<u8>>>,
    cv: Condvar,
}

/// One node's inbox + the set of topics it subscribes to.
#[derive(Default)]
struct NodeState {
    /// Topics this node has subscribed to (it only receives publishes on these).
    subs: Vec<String>,
    /// Messages queued for this node, drained by GET /mesh/messages.
    inbox: VecDeque<WireMsg>,
}

/// The on-wire `AppMessage` shape ce-rs deserializes.
#[derive(Clone, serde::Serialize)]
struct WireMsg {
    from: String,
    topic: String,
    payload_hex: String,
    received_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_token: Option<u64>,
}

/// Shared broker state behind the HTTP server.
struct Broker {
    /// token -> node id (hex). The bearer token both authenticates and names a node.
    tokens: Mutex<HashMap<String, String>>,
    /// node id -> its inbox + subscriptions.
    nodes: Mutex<HashMap<String, NodeState>>,
    /// content hash (hex sha256) -> bytes.
    blobs: Mutex<HashMap<String, Vec<u8>>>,
    /// reply_token -> rendezvous for an in-flight directed request.
    pending: Mutex<HashMap<u64, Arc<Pending>>>,
    next_token: AtomicU64,
}

impl Broker {
    fn new() -> Broker {
        Broker {
            tokens: Mutex::new(HashMap::new()),
            nodes: Mutex::new(HashMap::new()),
            blobs: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            next_token: AtomicU64::new(1),
        }
    }

    fn node_for(&self, token: &str) -> String {
        self.tokens.lock().unwrap().get(token).cloned().unwrap_or_default()
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A running mock broker. Holds the listener address; clients connect by token.
pub struct MockNode {
    pub base: String,
    broker: Arc<Broker>,
}

impl MockNode {
    /// Stand up the broker on a free loopback port and start serving on a background thread.
    pub fn start() -> MockNode {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock broker");
        let addr = listener.local_addr().expect("addr");
        let base = format!("http://{addr}");
        let broker = Arc::new(Broker::new());
        let srv = broker.clone();
        thread::spawn(move || serve(listener, srv));
        MockNode { base, broker }
    }

    /// Register a fresh node identity and return a `CeClient` authenticated as it. Each call is a
    /// distinct "device" sharing this one broker.
    pub fn client(&self) -> CeClient {
        let token = format!("tok-{}", self.broker.next_token.fetch_add(1, Ordering::SeqCst));
        // Node id: a 64-hex id derived from the token so it's stable + valid-looking.
        let node_id = hex::encode(Sha256::digest(token.as_bytes()));
        self.broker.tokens.lock().unwrap().insert(token.clone(), node_id.clone());
        self.broker.nodes.lock().unwrap().entry(node_id).or_default();
        CeClient::with_token(&self.base, Some(token))
    }
}

// ---- HTTP plumbing (minimal HTTP/1.1, one connection per request, Connection: close) ----

fn serve(listener: TcpListener, broker: Arc<Broker>) {
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let broker = broker.clone();
        thread::spawn(move || {
            let _ = handle_conn(stream, broker);
        });
    }
}

struct Req {
    method: String,
    path: String,
    token: String,
    body: Vec<u8>,
}

fn handle_conn(mut stream: TcpStream, broker: Arc<Broker>) -> std::io::Result<()> {
    let req = match read_request(&mut stream)? {
        Some(r) => r,
        None => return Ok(()),
    };
    // The inbox push stream (`GET /mesh/messages/stream`) is the real-time counterpart to the
    // `GET /mesh/messages` poll. Unlike every other route it is *not* one fixed-length response:
    // the connection stays open and the mock pushes each inbound message as an SSE `data:` frame
    // the instant it lands, exactly as the node does. This exercises ce-coord's streaming pump
    // path (not just the polling fallback the 404 used to force).
    if req.method == "GET" && req.path == "/mesh/messages/stream" {
        return serve_message_stream(&mut stream, &broker, &req);
    }
    let (status, ctype, body) = route(&broker, &req);
    let resp = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(resp.as_bytes())?;
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

/// Serve `GET /mesh/messages/stream` as Server-Sent-Events: emit the SSE header block, then loop
/// draining this node's inbox and writing one `data: <json AppMessage>\n\n` frame per message as it
/// arrives. The connection is held open (with periodic `:keep-alive` comments) until the client
/// disconnects — a write error (the client dropped the stream) ends the loop. This mirrors the
/// node's push endpoint closely enough to drive ce-coord's streaming pump end-to-end.
fn serve_message_stream(stream: &mut TcpStream, broker: &Arc<Broker>, req: &Req) -> std::io::Result<()> {
    let node = broker.node_for(&req.token);
    stream.write_all(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n",
    )?;
    stream.flush()?;
    // Poll the inbox every 10ms (so delivery latency is ~tens of ms, well under a poll interval),
    // and emit a keep-alive comment roughly once a second so a dropped client eventually surfaces as
    // a write error and we stop looping.
    let mut idle_ticks = 0u32;
    loop {
        // Drain whatever has queued for this node and push each as its own SSE frame.
        let drained: Vec<WireMsg> = {
            let mut nodes = broker.nodes.lock().unwrap();
            match nodes.get_mut(&node) {
                Some(st) => st.inbox.drain(..).collect(),
                None => Vec::new(),
            }
        };
        if drained.is_empty() {
            idle_ticks += 1;
            if idle_ticks >= 100 {
                idle_ticks = 0;
                if stream.write_all(b": keep-alive\n\n").is_err() || stream.flush().is_err() {
                    return Ok(());
                }
            }
            thread::sleep(Duration::from_millis(10));
            continue;
        }
        idle_ticks = 0;
        for msg in drained {
            let json = serde_json::to_string(&msg).unwrap();
            let frame = format!("data: {json}\n\n");
            if stream.write_all(frame.as_bytes()).is_err() || stream.flush().is_err() {
                return Ok(());
            }
        }
    }
}

fn read_request(stream: &mut TcpStream) -> std::io::Result<Option<Req>> {
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    let mut tmp = [0u8; 4096];
    // Read until we have the full header block.
    let header_end = loop {
        if let Some(idx) = find_subslice(&buf, b"\r\n\r\n") {
            break idx + 4;
        }
        let n = stream.read(&mut tmp)?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > 64 * 1024 * 1024 {
            return Ok(None);
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();

    let mut content_length = 0usize;
    let mut token = String::new();
    for line in lines {
        let lower = line.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-length:") {
            content_length = v.trim().parse().unwrap_or(0);
        } else if lower.starts_with("authorization:") {
            // "Authorization: Bearer <tok>"
            if let Some((_, b)) = line.split_once(':') {
                token = b.trim().trim_start_matches("Bearer").trim().to_string();
            }
        }
    }

    let mut body = buf[header_end..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut tmp)?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(content_length);
    Ok(Some(Req { method, path, token, body }))
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn json_ok(v: serde_json::Value) -> (&'static str, &'static str, Vec<u8>) {
    ("200 OK", "application/json", serde_json::to_vec(&v).unwrap())
}

fn empty_ok() -> (&'static str, &'static str, Vec<u8>) {
    ("200 OK", "application/json", b"{}".to_vec())
}

fn route(broker: &Broker, req: &Req) -> (&'static str, &'static str, Vec<u8>) {
    let node = broker.node_for(&req.token);
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/health") => empty_ok(),
        ("GET", "/status") => json_ok(serde_json::json!({
            "node_id": node,
            "height": 0u64,
            "difficulty": 1u8,
            "balance": "0",
        })),
        ("POST", "/mesh/subscribe") => {
            let v: serde_json::Value = serde_json::from_slice(&req.body).unwrap_or_default();
            let topic = v["topic"].as_str().unwrap_or_default().to_string();
            let mut nodes = broker.nodes.lock().unwrap();
            let st = nodes.entry(node).or_default();
            if !st.subs.contains(&topic) {
                st.subs.push(topic);
            }
            empty_ok()
        }
        ("POST", "/mesh/publish") => {
            let v: serde_json::Value = serde_json::from_slice(&req.body).unwrap_or_default();
            let topic = v["topic"].as_str().unwrap_or_default().to_string();
            let payload_hex = v["payload_hex"].as_str().unwrap_or_default().to_string();
            let msg = WireMsg {
                from: node.clone(),
                topic: topic.clone(),
                payload_hex,
                received_at: now_secs(),
                reply_token: None,
            };
            let mut nodes = broker.nodes.lock().unwrap();
            // Fan out to every *other* node subscribed to this topic (gossip doesn't echo).
            let targets: Vec<String> = nodes
                .iter()
                .filter(|(id, st)| **id != node && st.subs.contains(&topic))
                .map(|(id, _)| id.clone())
                .collect();
            for id in targets {
                if let Some(st) = nodes.get_mut(&id) {
                    st.inbox.push_back(msg.clone());
                }
            }
            empty_ok()
        }
        ("GET", "/mesh/messages") => {
            let mut nodes = broker.nodes.lock().unwrap();
            let drained: Vec<WireMsg> = match nodes.get_mut(&node) {
                Some(st) => st.inbox.drain(..).collect(),
                None => Vec::new(),
            };
            ("200 OK", "application/json", serde_json::to_vec(&drained).unwrap())
        }
        ("POST", "/mesh/request") => {
            let v: serde_json::Value = serde_json::from_slice(&req.body).unwrap_or_default();
            let to = v["to"].as_str().unwrap_or_default().to_string();
            let topic = v["topic"].as_str().unwrap_or_default().to_string();
            let payload_hex = v["payload_hex"].as_str().unwrap_or_default().to_string();
            let timeout_ms = v["timeout_ms"].as_u64().unwrap_or(5000);

            let token = broker.next_token.fetch_add(1, Ordering::SeqCst);
            let pend = Arc::new(Pending { reply: Mutex::new(None), cv: Condvar::new() });
            broker.pending.lock().unwrap().insert(token, pend.clone());

            // Deliver the request to the target's inbox with a reply token.
            {
                let mut nodes = broker.nodes.lock().unwrap();
                if let Some(st) = nodes.get_mut(&to) {
                    st.inbox.push_back(WireMsg {
                        from: node.clone(),
                        topic,
                        payload_hex,
                        received_at: now_secs(),
                        reply_token: Some(token),
                    });
                } else {
                    // No such node: drop the rendezvous and 504 so the SDK returns Err.
                    broker.pending.lock().unwrap().remove(&token);
                    return ("504 Gateway Timeout", "text/plain", b"no route".to_vec());
                }
            }

            // Block until reply() fills it, or the timeout elapses.
            let deadline = Instant::now() + Duration::from_millis(timeout_ms.min(30_000));
            let mut guard = pend.reply.lock().unwrap();
            while guard.is_none() {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let (g, _to) = pend.cv.wait_timeout(guard, remaining).unwrap();
                guard = g;
            }
            broker.pending.lock().unwrap().remove(&token);
            match guard.take() {
                Some(bytes) => json_ok(serde_json::json!({ "payload_hex": hex::encode(bytes) })),
                None => ("504 Gateway Timeout", "text/plain", b"request timed out".to_vec()),
            }
        }
        ("POST", "/mesh/reply") => {
            let v: serde_json::Value = serde_json::from_slice(&req.body).unwrap_or_default();
            let token = v["token"].as_u64().unwrap_or(0);
            let payload_hex = v["payload_hex"].as_str().unwrap_or_default();
            let bytes = hex::decode(payload_hex).unwrap_or_default();
            if let Some(pend) = broker.pending.lock().unwrap().get(&token).cloned() {
                *pend.reply.lock().unwrap() = Some(bytes);
                pend.cv.notify_all();
            }
            empty_ok()
        }
        ("POST", "/blobs") => {
            let hash = hex::encode(Sha256::digest(&req.body));
            broker.blobs.lock().unwrap().insert(hash.clone(), req.body.clone());
            json_ok(serde_json::json!({ "hash": hash }))
        }
        ("GET", p) if p.starts_with("/blobs/") => {
            let hash = p.trim_start_matches("/blobs/");
            match broker.blobs.lock().unwrap().get(hash) {
                Some(bytes) => ("200 OK", "application/octet-stream", bytes.clone()),
                None => ("404 Not Found", "text/plain", b"blob not found".to_vec()),
            }
        }
        _ => ("404 Not Found", "text/plain", b"unknown endpoint".to_vec()),
    }
}

/// Poll `cond` every 25ms until it returns true or `timeout` elapses. Returns whether it became
/// true. Used to wait for the (250ms-polling) pump to deliver and apply messages.
pub async fn within<F: Fn() -> bool>(timeout: Duration, cond: F) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
