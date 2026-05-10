use std::io::Result;
use tokio::net::TcpStream;

#[cfg(feature = "balance")]
use std::sync::Arc;
#[cfg(feature = "balance")]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[cfg(not(feature = "balance"))]
use super::socket;
use super::plain;

#[cfg(feature = "hook")]
use super::hook;

#[cfg(feature = "proxy")]
use super::proxy;

#[cfg(feature = "transport")]
use super::transport;

use crate::trick::Ref;
use crate::endpoint::{RemoteAddr, ConnectOpts};

#[cfg(feature = "balance")]
use super::pool::RemotePool;

// ---------------------------------------------------------------------------
// TimedStream: wraps a TcpStream to record the moment the first byte arrives.
// Used by connect_and_relay for relay-level latency failover.
// ---------------------------------------------------------------------------
#[cfg(feature = "balance")]
mod timed_stream {
    use std::io;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::task::{Context, Poll};
    use std::time::Instant;
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tokio::net::TcpStream;

    /// Wraps a TcpStream. On the first successful read, stores the elapsed
    /// microseconds since construction into `first_byte_us`.
    pub struct TimedStream {
        inner: TcpStream,
        start: Instant,
        first_byte_us: Arc<AtomicU64>,
    }

    impl TimedStream {
        pub fn new(inner: TcpStream, first_byte_us: Arc<AtomicU64>) -> Self {
            Self { inner, start: Instant::now(), first_byte_us }
        }
    }

    impl AsyncRead for TimedStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let filled_before = buf.filled().len();
            let result = Pin::new(&mut self.inner).poll_read(cx, buf);
            if let Poll::Ready(Ok(())) = &result {
                if buf.filled().len() > filled_before {
                    // First byte(s) arrived — record timestamp once.
                    self.first_byte_us.compare_exchange(
                        0,
                        self.start.elapsed().as_micros() as u64,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ).ok();
                }
            }
            result
        }
    }

    impl AsyncWrite for TimedStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.inner).poll_write(cx, buf)
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }
}

#[cfg(feature = "balance")]
use timed_stream::TimedStream;

#[cfg(feature = "balance")]
async fn try_connect_with_fallback(
    balancer: &bytehub_lb::Balancer,
    raddr: &RemoteAddr,
    extra_raddrs: &[RemoteAddr],
    conn_opts: &ConnectOpts,
    local: &TcpStream,
) -> Result<(TcpStream, Option<bytehub_lb::Token>)> {
    use bytehub_lb::{Token, BalanceCtx};
    use std::collections::HashSet;
    use std::time::Instant;

    let src_ip = local.peer_addr()?.ip();
    let mut token = balancer.next(BalanceCtx { src_ip: &src_ip });
    let mut tried: HashSet<u8> = HashSet::new();
    let total = balancer.total();

    // Latency threshold from health config, if configured.
    let max_latency = balancer.health_config().and_then(|c| c.max_latency_ms);

    // Track fastest slow-but-successful candidate as last-resort fallback.
    let mut fallback_stream: Option<TcpStream> = None;
    let mut fallback_token: Option<Token> = None;
    let mut fallback_ms: Option<u128> = None;

    loop {
        let selected = match token {
            None | Some(Token(0)) => raddr,
            Some(Token(idx)) => {
                let i = idx as usize;
                if i > 0 && i - 1 < extra_raddrs.len() {
                    &extra_raddrs[i - 1]
                } else {
                    raddr
                }
            }
        };

        log::debug!("[tcp]select remote peer, token: {:?}", token);

        let start = if max_latency.is_some() { Some(Instant::now()) } else { None };

        match super::socket::connect(selected, conn_opts).await {
            Ok(stream) => {
                // Latency-based failover: connect succeeded but was too slow.
                if let (Some(max), Some(start)) = (max_latency, start) {
                    let ms = start.elapsed().as_millis();
                    if ms > max as u128 {
                        log::warn!("[tcp]connect to {} succeeded but too slow ({}ms > {}ms), trying next peer",
                            selected, ms, max);
                        if let Some(tok) = token {
                            balancer.on_failure(tok);
                            tried.insert(tok.0);
                        }
                        // Remember fastest slow candidate as fallback.
                        if fallback_ms.map_or(true, |f| ms < f) {
                            fallback_stream = Some(stream);
                            fallback_token = token;
                            fallback_ms = Some(ms);
                        }
                        if total == 0 || tried.len() as u8 >= total {
                            break;
                        }
                        token = balancer.next(BalanceCtx { src_ip: &src_ip });
                        continue;
                    }
                }
                log::info!("[tcp]{} => {} as {}", local.peer_addr()?, selected, stream.peer_addr()?);
                return Ok((stream, token));
            }
            Err(e) => {
                log::warn!("[tcp]connect to {} failed: {}, trying next peer", selected, e);
                if let Some(tok) = token {
                    balancer.on_failure(tok);
                    tried.insert(tok.0);
                }
                // All peers exhausted — give up.
                if total == 0 || tried.len() as u8 >= total {
                    // If we have a slow fallback, prefer it over error.
                    if let Some(stream) = fallback_stream {
                        log::warn!("[tcp]all peers failed or too slow, using fallback ({}ms)",
                            fallback_ms.unwrap_or(0));
                        return Ok((stream, fallback_token));
                    }
                    return Err(e);
                }
                token = balancer.next(BalanceCtx { src_ip: &src_ip });
            }
        }
    }

    // All peers connected but every one exceeded the latency threshold.
    if let Some(stream) = fallback_stream {
        log::warn!("[tcp]all peers too slow, using fastest ({}ms)", fallback_ms.unwrap_or(0));
        return Ok((stream, fallback_token));
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::ConnectionRefused,
        "no peers available",
    ))
}

/// Timed plain relay with non-disruptive latency failover.
///
/// If no first byte arrives within `max_latency_ms`, the node is marked unhealthy
/// via the shared `latency_exceeded` flag, but the relay is NOT killed — the
/// connection continues so the client still gets the response (just slowly).
/// The caller checks `latency_exceeded` after relay completes to decide whether
/// to call `balancer.on_success` or `balancer.on_failure`.
#[cfg(feature = "balance")]
async fn relay_plain_timed(
    mut local: TcpStream,
    remote: TcpStream,
    first_byte_us: &Arc<AtomicU64>,
    latency_exceeded: &Arc<AtomicBool>,
    max_latency_ms: u32,
) -> Result<()> {
    use std::time::Duration;

    // TimedStream wraps remote to detect first-byte arrival.
    // We use bidi_copy (not zero_copy) because TimedStream doesn't implement
    // AsyncRawIO — the inner TcpStream does, but TimedStream intercepts reads.
    let mut timed = TimedStream::new(remote, Arc::clone(first_byte_us));
    let relay = bytehub_io::bidi_copy(&mut local, &mut timed);
    tokio::pin!(relay);

    let deadline = tokio::time::Instant::now() + Duration::from_millis(max_latency_ms as u64);

    tokio::select! {
        // Relay completed before the deadline.
        r = &mut relay => {
            // If relay finished before deadline but zero bytes arrived,
            // the backend is dead (not slow) — mark unhealthy immediately.
            let no_data = first_byte_us.load(Ordering::Relaxed) == 0;
            if no_data {
                latency_exceeded.store(true, Ordering::Relaxed);
                log::warn!("[tcp]relay completed with zero bytes from remote, marking unhealthy");
            }
            r.map(|_| ())
        }

        // Deadline fired — check whether the first byte arrived in time.
        _ = tokio::time::sleep_until(deadline) => {
            if first_byte_us.load(Ordering::Relaxed) > 0 {
                // First byte arrived within the threshold — let relay finish.
                relay.await.map(|_| ())
            } else {
                // Latency exceeded — mark unhealthy but keep the connection alive
                // so the client still gets the (slow) response.
                latency_exceeded.store(true, Ordering::Relaxed);
                log::warn!("[tcp]no first byte within {}ms, marking unhealthy (connection kept)", max_latency_ms);
                relay.await.map(|_| ())
            }
        }
    }
}

#[allow(unused)]
pub async fn connect_and_relay(
    mut local: TcpStream,
    raddr: Ref<RemoteAddr>,
    conn_opts: Ref<ConnectOpts>,
    extra_raddrs: Ref<Vec<RemoteAddr>>,
    #[cfg(feature = "balance")]
    remote_pool: Option<Arc<RemotePool>>,
) -> Result<()> {
    let ConnectOpts {
        #[cfg(feature = "proxy")]
        proxy_opts,

        #[cfg(feature = "transport")]
        transport,

        #[cfg(feature = "balance")]
        balancer,

        tcp_keepalive,
        ..
    } = conn_opts.as_ref();

    // pre-connect hook (called once before any connection attempt).
    #[cfg(feature = "hook")]
    hook::pre_connect_hook(&mut local, raddr.as_ref(), extra_raddrs.as_ref()).await?;

    // connect with optional retry (balance feature).
    #[cfg(feature = "balance")]
    let selected_token: Option<bytehub_lb::Token>;

    let mut remote = {
        #[cfg(feature = "balance")]
        {
            // If a connection pool is available, try to get an idle connection.
            // Pool internally calls try_connect_with_fallback on cache miss,
            // so the fallback behaviour is identical to the non-pool path.
            if let Some(pool) = &remote_pool {
                use bytehub_lb::{Token, BalanceCtx};
                let src_ip = local.peer_addr()?.ip();
                let tok = balancer.next(BalanceCtx { src_ip: &src_ip });
                let token = tok.unwrap_or(Token(0));
                match pool.acquire(token, conn_opts.as_ref()).await {
                    Ok(stream) => {
                        log::info!("[tcp](pool){} => token={:?}", local.peer_addr()?, token);
                        selected_token = Some(token);
                        stream
                    }
                    Err(e) => {
                        balancer.on_failure(token);
                        return Err(e);
                    }
                }
            } else {
                let (stream, tok) = try_connect_with_fallback(
                    balancer,
                    raddr.as_ref(),
                    extra_raddrs.as_ref(),
                    conn_opts.as_ref(),
                    &local,
                )
                .await?;
                selected_token = tok;
                stream
            }
        }

        #[cfg(not(feature = "balance"))]
        {
            let remote = socket::connect(raddr.as_ref(), conn_opts.as_ref()).await?;
            log::info!("[tcp]{} => {} as {}", local.peer_addr()?, raddr.as_ref(), remote.peer_addr()?);
            remote
        }
    };

    // after connected
    // ..
    #[cfg(feature = "proxy")]
    if proxy_opts.enabled() {
        proxy::handle_proxy(&mut local, &mut remote, *proxy_opts).await?;
    }

    // relay (with optional non-disruptive first-byte latency failover).
    #[cfg(feature = "balance")]
    let (max_latency_ms, first_byte_us, latency_exceeded) = {
        let ml = balancer.health_config().and_then(|c| c.max_latency_ms);
        eprintln!("[DIAG] timed_relay_enabled={}", ml.is_some());
        match ml {
            Some(ms) => (
                Some(ms),
                Some(Arc::new(AtomicU64::new(0))),
                Some(Arc::new(AtomicBool::new(false))),
            ),
            None => (None, None, None),
        }
    };

    let res = {
        #[cfg(feature = "transport")]
        {
            if let Some((ac, cc)) = transport {
                transport::run_relay(local, remote, ac, cc).await
            } else {
                #[cfg(feature = "balance")]
                {
                    if let Some(fb) = &first_byte_us {
                        relay_plain_timed(local, remote, fb, latency_exceeded.as_ref().unwrap(), max_latency_ms.unwrap()).await
                    } else {
                        plain::run_relay(local, remote).await
                    }
                }
                #[cfg(not(feature = "balance"))]
                plain::run_relay(local, remote).await
            }
        }
        #[cfg(not(feature = "transport"))]
        {
            #[cfg(feature = "balance")]
            {
                if let Some(fb) = &first_byte_us {
                    relay_plain_timed(local, remote, fb, latency_exceeded.as_ref().unwrap(), max_latency_ms.unwrap()).await
                } else {
                    plain::run_relay(local, remote).await
                }
            }
            #[cfg(not(feature = "balance"))]
            plain::run_relay(local, remote).await
        }
    };

    match res {
        Ok(()) => {
            #[cfg(feature = "balance")]
            if let Some(tok) = selected_token {
                // If latency exceeded the threshold, mark unhealthy even
                // though the relay completed successfully (no disruption).
                let slow = latency_exceeded.as_ref()
                    .map_or(false, |le| le.load(Ordering::Relaxed));
                eprintln!("[DIAG] relay Ok, token={:?}, slow={}, timed_relay={}",
                    tok, slow, first_byte_us.is_some());
                if slow {
                    log::warn!("[tcp]peer {:?} relay latency exceeded threshold, marking unhealthy", tok);
                    balancer.on_failure(tok);
                } else {
                    balancer.on_success(tok);
                    // NOTE: The relay functions consume `remote` by move, so we
                    // cannot return the stream to the pool after relay.
                    // The pool warmup task replenishes idle connections in the
                    // background; the pool value comes purely from the acquire
                    // side (avoids a TCP handshake RTT per connection).
                    //
                    // obfs mode: the pool still works on the acquire side —
                    // pre-warmed plain TCP connections are valid starting points
                    // for obfs relays because the obfs handshake happens inside
                    // bidi_copy_buf, not during TCP connection establishment.
                    let _ = &remote_pool; // suppress unused-variable warning
                }
            }
        }
        Err(e) => {
            // Relay failed (downstream unreachable, etc.):
            // mark this peer as failed so the balancer can skip it next time.
            eprintln!("[DIAG] relay Err, token={:?}, error={}", selected_token, e);
            #[cfg(feature = "balance")]
            if let Some(tok) = selected_token {
                balancer.on_failure(tok);
                // relay failed — do not return to pool
            }
            log::debug!("[tcp]forward error: {}, ignored", e);
        }
    }

    Ok(())
}
