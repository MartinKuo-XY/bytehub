//! Pre-warmed TCP connection pool for remote peers.
//!
//! For each peer a sub-pool of idle `TcpStream`s is maintained.
//! When a new relay request arrives, `acquire()` pops a ready stream
//! instead of spending an RTT on TCP handshake.  After the relay
//! finishes, the caller may `return_conn()` the stream back to the
//! pool so it can be reused.
//!
//! **When is pooling useful?**
//! - High-churn short-lived connections (HTTP/1.0, lightweight proxies)
//! - Remote peers with noticeable RTT (cross-region, WAN)
//!
//! **When is pooling *not* useful / disabled automatically?**
//! - `obfs` mode: the handshake consumes a custom byte sequence that
//!   cannot be replayed, so pooled connections are discarded.
//! - `transport` (TLS/WS): kaminari takes ownership of the raw stream;
//!   the connection is never returned to us in a reusable state.
//! - Relay ended with error: connection state is unknown.
//!
//! The pool is optional — it is only created when `pool_size > 0` is
//! present in the endpoint config.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use bytehub_lb::Token;

use crate::endpoint::{ConnectOpts, RemoteAddr};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Pool configuration embedded in `ConnectOpts`.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of idle connections kept per peer.
    pub pool_size: usize,
    /// Minimum idle connections the warmup task tries to maintain.
    pub pool_min_idle: usize,
    /// TCP connect timeout used by the warmup / acquire path (milliseconds).
    pub pool_connect_timeout_ms: u64,
    /// Discard idle connections older than this many seconds.
    pub pool_idle_timeout_secs: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            pool_size: 8,
            pool_min_idle: 2,
            pool_connect_timeout_ms: 3000,
            pool_idle_timeout_secs: 30,
        }
    }
}

// ---------------------------------------------------------------------------
// Idle connection wrapper
// ---------------------------------------------------------------------------

struct IdleConn {
    stream: TcpStream,
    created_at: Instant,
}

// ---------------------------------------------------------------------------
// Per-peer sub-pool
// ---------------------------------------------------------------------------

struct PeerPool {
    idle: Vec<IdleConn>,
}

impl PeerPool {
    fn new() -> Self {
        Self { idle: Vec::new() }
    }

    /// Pop the most-recently-added connection that is still alive and
    /// within the idle timeout.
    fn pop_healthy(&mut self, idle_timeout: Duration) -> Option<TcpStream> {
        while let Some(conn) = self.idle.pop() {
            if conn.created_at.elapsed() > idle_timeout {
                log::debug!("[pool] dropping idle conn: idle_timeout exceeded");
                // The TcpStream is dropped here → kernel closes the socket.
                continue;
            }
            if is_conn_alive(&conn.stream) {
                return Some(conn.stream);
            }
            log::debug!("[pool] dropping idle conn: peer closed");
        }
        None
    }

    fn push(&mut self, stream: TcpStream, max_size: usize) {
        if self.idle.len() < max_size {
            self.idle.push(IdleConn { stream, created_at: Instant::now() });
        }
        // else: drop the stream → kernel closes socket
    }

    fn len(&self) -> usize {
        self.idle.len()
    }
}

/// Non-blocking liveness check.
///
/// Uses `try_read` to test whether the peer has sent EOF or RST.
/// - `WouldBlock`  → buffer empty, connection still alive  → **true**
/// - `Ok(0)`       → peer sent FIN / EOF                   → **false**
/// - `Ok(n > 0)`   → unexpected data waiting (banner, etc) → **false**
/// - `Err(_)`      → socket error                          → **false**
fn is_conn_alive(stream: &TcpStream) -> bool {
    let mut buf = [0u8; 1];
    match stream.try_read(&mut buf) {
        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => true,
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// RemotePool — the public interface
// ---------------------------------------------------------------------------

/// Manages a connection pool for every peer in the endpoint.
pub struct RemotePool {
    pools: Mutex<HashMap<u8, PeerPool>>,
    config: Arc<PoolConfig>,
    /// Static token→address map used by the warmup task.
    peer_addrs: Vec<(Token, RemoteAddr)>,
}

impl RemotePool {
    /// Build a pool and register all known peers.
    pub fn new(config: PoolConfig, peer_addrs: Vec<(Token, RemoteAddr)>) -> Arc<Self> {
        let mut pools = HashMap::new();
        for (token, _) in &peer_addrs {
            pools.insert(token.0, PeerPool::new());
        }
        Arc::new(Self {
            pools: Mutex::new(pools),
            config: Arc::new(config),
            peer_addrs,
        })
    }

    /// Acquire a connection for `token`.
    ///
    /// 1. Try to pop a healthy idle connection from the sub-pool.
    /// 2. If none, establish a fresh connection via `connect_new`.
    ///
    /// Works correctly in **obfs mode**: the pool stores plain TCP connections;
    /// the obfs handshake is performed inside `bidi_copy_buf` at relay time,
    /// not during connection establishment.
    pub async fn acquire(
        &self,
        token: Token,
        conn_opts: &ConnectOpts,
    ) -> std::io::Result<TcpStream> {
        let idle_timeout = Duration::from_secs(self.config.pool_idle_timeout_secs);

        // Step 1 — check the pool.
        {
            let mut pools = self.pools.lock().await;
            if let Some(pool) = pools.get_mut(&token.0) {
                if let Some(stream) = pool.pop_healthy(idle_timeout) {
                    log::debug!("[pool] token={:?} — reuse idle conn", token);
                    return Ok(stream);
                }
            }
        }

        // Step 2 — pool empty, connect now.
        log::debug!("[pool] token={:?} — no idle conn, connecting", token);
        self.connect_new(token, conn_opts).await
    }

    /// Establish a **brand-new** TCP connection to the peer for `token`.
    ///
    /// This bypasses the pool entirely — it is used both by `acquire` (on
    /// cache miss) and by the warmup task (which must always create *new*
    /// connections, never pop-and-return existing idle ones).
    async fn connect_new(
        &self,
        token: Token,
        conn_opts: &ConnectOpts,
    ) -> std::io::Result<TcpStream> {
        let raddr = self
            .peer_addrs
            .iter()
            .find(|(t, _)| t.0 == token.0)
            .map(|(_, a)| a)
            .ok_or_else(|| std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("pool: unknown token {:?}", token),
            ))?;

        // Use the pool's own connect_timeout, not the relay-level one.
        let mut patched = conn_opts.clone();
        let secs = ((self.config.pool_connect_timeout_ms + 999) / 1000).max(1) as usize;
        patched.connect_timeout = secs;

        super::socket::connect(raddr, &patched).await
    }

    /// Return a finished connection back to the pool.
    ///
    /// The connection is **discarded** (not pooled) when:
    /// - `relay_ok` is false  — relay error, connection state unknown
    /// - `obfs` is non-empty  — the obfs handshake writes/reads a one-time
    ///   prefix at relay start, leaving the stream in an XOR-encrypted state
    ///   that cannot be reused for a fresh relay
    /// - connection is no longer alive (peer sent FIN/RST)
    ///
    /// NOTE: The current relay functions (`plain::run_relay`, `relay_plain_timed`)
    /// consume the `TcpStream` by move, so this method cannot be called after
    /// relay completes.  It is kept as a public API for future use when relay
    /// is refactored to return the stream back to the caller.
    ///
    /// **obfs and the acquire side**: obfs does NOT prevent pool usage on the
    /// acquire side.  The pool pre-warms plain TCP connections; the obfs
    /// handshake is performed inside `bidi_copy_buf` at relay time, so a
    /// pooled connection is just as usable in obfs mode as in plain mode.
    /// Only the return side is blocked (stream state not reusable after relay).
    #[allow(dead_code)]
    pub async fn return_conn(
        &self,
        token: Token,
        stream: TcpStream,
        relay_ok: bool,
        obfs: &str,
    ) {
        if !relay_ok {
            log::debug!("[pool] token={:?} — discard: relay failed", token);
            return;
        }
        if !obfs.is_empty() {
            log::debug!("[pool] token={:?} — discard: obfs mode ({}), not reusable", token, obfs);
            return;
        }
        if !is_conn_alive(&stream) {
            log::debug!("[pool] token={:?} — discard: conn not alive after relay", token);
            return;
        }

        let mut pools = self.pools.lock().await;
        if let Some(pool) = pools.get_mut(&token.0) {
            pool.push(stream, self.config.pool_size);
            log::debug!("[pool] token={:?} — returned, pool.len={}", token, pool.len());
        }
    }

    /// Spawn the background warmup task.
    ///
    /// The task wakes every `pool_idle_timeout_secs / 2` seconds and
    /// pre-fills each sub-pool up to `pool_min_idle` connections.
    /// It exits when `cancel` fires.
    ///
    /// **Why `connect_new` and not `acquire`?**
    /// `acquire` first checks the pool and pops an existing idle connection if
    /// one is available.  If the warmup task used `acquire`, it would pop a
    /// connection it just pushed in a previous iteration — endlessly cycling
    /// the same stream without ever reaching the `pool_min_idle` target.
    /// `connect_new` always dials a fresh TCP socket, so every iteration
    /// genuinely adds one new idle connection to the pool.
    ///
    /// **obfs mode**: `connect_new` calls `socket::connect` which performs only
    /// the TCP 3-way handshake.  The obfs-specific prefix/XOR logic lives
    /// inside `bidi_copy_buf` and is triggered by the `OBFS_MODE` task-local
    /// variable at relay time.  Therefore the warmup task correctly pre-warms
    /// connections for both plain and obfs endpoints.
    ///
    /// **Single-endpoint forwarding**: works identically — the pool maintains
    /// a sub-pool for Token(0) (the sole remote), and the warmup task fills
    /// it exactly like the multi-peer case.
    pub fn spawn_warmup_task(
        self: &Arc<Self>,
        conn_opts: ConnectOpts,
        cancel: CancellationToken,
    ) {
        let pool = Arc::clone(self);
        let conn_opts = Arc::new(conn_opts);

        tokio::spawn(async move {
            let interval = Duration::from_secs(
                (pool.config.pool_idle_timeout_secs / 2).max(5),
            );

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    _ = cancel.cancelled() => {
                        log::debug!("[pool] warmup task — cancelled, exiting");
                        return;
                    }
                }

                let tokens: Vec<(Token, RemoteAddr)> = pool.peer_addrs.clone();

                for (token, _) in &tokens {
                    // Snapshot current idle count under the lock, then release
                    // before dialling (dialling can be slow / blocking).
                    let current_idle = {
                        let pools = pool.pools.lock().await;
                        pools.get(&token.0).map_or(0, |p| p.len())
                    };

                    let needed = pool.config.pool_min_idle.saturating_sub(current_idle);
                    if needed == 0 {
                        continue;
                    }
                    log::debug!("[pool] warmup: token={:?} needs {} more idle conn(s)", token, needed);

                    for _ in 0..needed {
                        // Always create a BRAND NEW connection — never reuse
                        // an existing idle one (that would be a no-op).
                        match pool.connect_new(*token, &conn_opts).await {
                            Ok(stream) => {
                                let mut pools = pool.pools.lock().await;
                                if let Some(p) = pools.get_mut(&token.0) {
                                    // Guard against concurrent relay workers
                                    // that may have filled the pool while we
                                    // were dialling.
                                    if p.len() < pool.config.pool_size {
                                        p.push(stream, pool.config.pool_size);
                                        log::debug!(
                                            "[pool] warmup: token={:?} pre-warmed 1 conn, pool.len={}",
                                            token, p.len()
                                        );
                                    } else {
                                        log::debug!(
                                            "[pool] warmup: token={:?} pool full after dialling, discarding new conn",
                                            token
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                log::warn!("[pool] warmup: token={:?} connect failed: {}", token, e);
                                break; // skip remaining iters for this peer; retry next cycle
                            }
                        }
                    }
                }
            }
        });
    }
}
