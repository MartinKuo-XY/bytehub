//! Active health probe — background TCP ping loop per peer.
//!
//! Each peer gets its own async task that periodically attempts a
//! lightweight TCP connect (connect + immediate drop).  On consecutive
//! failures the peer is marked unhealthy via `balancer.on_failure()`; on
//! recovery it is cleared via `balancer.on_success()`.
//!
//! This runs **in addition to** the existing passive failure counting in
//! `try_connect_with_fallback` — the two mechanisms are independent and
//! either one can mark a peer unhealthy.

use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

use bytehub_lb::{Balancer, Token};

use crate::dns::resolve_addr;
use crate::endpoint::RemoteAddr;
use crate::time::timeoutfut;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the active probe loop.
#[derive(Debug, Clone)]
pub struct ProbeConfig {
    /// Probe interval for **healthy** nodes (seconds).
    pub probe_interval_secs: u64,

    /// Probe interval for **unhealthy** nodes (seconds).
    /// Usually shorter so recovery is detected quickly.
    pub probe_unhealthy_interval_secs: u64,

    /// TCP connect timeout for each probe attempt (milliseconds).
    pub probe_timeout_ms: u64,

    /// Number of **consecutive** probe failures required to call
    /// `balancer.on_failure()` for the first time.
    /// Reuses the existing `max_fails` value from `HealthCheckConfig`.
    pub probe_max_fails: u32,
}

impl Default for ProbeConfig {
    fn default() -> Self {
        Self {
            probe_interval_secs: 10,
            probe_unhealthy_interval_secs: 5,
            probe_timeout_ms: 3000,
            probe_max_fails: 2,
        }
    }
}

// ---------------------------------------------------------------------------
// Single-shot probe
// ---------------------------------------------------------------------------

/// Attempt one TCP probe to `raddr`.
/// Returns `Ok(())` if the connection succeeded (even though it is
/// immediately dropped), or `Err` on timeout / refused / etc.
async fn probe_once(raddr: &RemoteAddr, timeout_ms: u64) -> std::io::Result<()> {
    let timeout_secs = ((timeout_ms + 999) / 1000).max(1) as usize; // ceil, min 1
    let addrs = resolve_addr(raddr).await?;
    let addr = addrs
        .iter()
        .next()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, "dns: no address"))?;

    let connect_fut = TcpStream::connect(addr);
    // timeoutfut uses connect_timeout (seconds); translate ms → secs (ceiling).
    let stream = timeoutfut(connect_fut, timeout_secs).await??;
    drop(stream); // immediate close — just testing reachability
    Ok(())
}

// ---------------------------------------------------------------------------
// Probe loop task
// ---------------------------------------------------------------------------

/// Spawn a background probe task for a single peer.
///
/// The task exits when `cancel` (or any of its parents) is cancelled.
pub fn spawn_probe_loop(
    token: Token,
    raddr: RemoteAddr,
    balancer: Arc<Balancer>,
    config: Arc<ProbeConfig>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Track consecutive probe failures independently from the passive counter.
        let mut consecutive_fails: u32 = 0;
        // Whether *this probe loop* has already called on_failure for this peer.
        let mut probe_marked_unhealthy = false;

        log::debug!("[probe] token={:?} addr={} — probe loop started", token, raddr);

        loop {
            // Adaptive interval: probe unhealthy peers more frequently.
            let interval = if probe_marked_unhealthy {
                Duration::from_secs(config.probe_unhealthy_interval_secs)
            } else {
                Duration::from_secs(config.probe_interval_secs)
            };

            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = cancel.cancelled() => {
                    log::debug!("[probe] token={:?} — cancelled, exiting", token);
                    return;
                }
            }

            match probe_once(&raddr, config.probe_timeout_ms).await {
                Ok(()) => {
                    log::debug!("[probe] token={:?} addr={} — probe OK", token, raddr);
                    consecutive_fails = 0;

                    if probe_marked_unhealthy {
                        // Peer recovered — clear the failure so the balancer can
                        // start routing traffic to it again.
                        probe_marked_unhealthy = false;
                        balancer.on_success(token);
                        log::info!(
                            "[probe] token={:?} addr={} — RECOVERED via active probe",
                            token, raddr
                        );
                    }
                }

                Err(e) => {
                    consecutive_fails += 1;
                    log::warn!(
                        "[probe] token={:?} addr={} — probe FAILED ({}/{}) : {}",
                        token, raddr, consecutive_fails, config.probe_max_fails, e
                    );

                    if consecutive_fails >= config.probe_max_fails && !probe_marked_unhealthy {
                        // Threshold reached for the first time — mark unhealthy.
                        probe_marked_unhealthy = true;
                        balancer.on_failure(token);
                        log::warn!(
                            "[probe] token={:?} addr={} — MARKED UNHEALTHY after {} consecutive probe failures",
                            token, raddr, consecutive_fails
                        );
                    }
                    // If already marked unhealthy, do nothing extra — the
                    // passive mechanism (try_connect_with_fallback) keeps its
                    // own `checked` timestamp alive via separate on_failure calls.
                }
            }
        }
    })
}
