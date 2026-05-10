//! TCP relay entrance.

pub(super) mod socket;
mod middle;
mod plain;

#[cfg(feature = "hook")]
mod hook;

#[cfg(feature = "proxy")]
mod proxy;

#[cfg(feature = "transport")]
mod transport;

#[cfg(feature = "balance")]
pub mod probe;

#[cfg(feature = "balance")]
pub mod pool;

use std::io::{ErrorKind, Result};

use crate::trick::Ref;
use crate::endpoint::Endpoint;

use middle::connect_and_relay;

/// Launch a tcp relay.
pub async fn run_tcp(endpoint: Endpoint) -> Result<()> {
    let Endpoint {
        laddr,
        raddr,
        bind_opts,
        conn_opts,
        extra_raddrs,
    } = endpoint;

    // ── Build peer address table (Token(0)=raddr, Token(i)=extra_raddrs[i-1]) ──
    #[cfg(feature = "balance")]
    let peer_addrs: Vec<(bytehub_lb::Token, crate::endpoint::RemoteAddr)> = {
        let mut v = vec![(bytehub_lb::Token(0), raddr.clone())];
        for (i, er) in extra_raddrs.iter().enumerate() {
            v.push((bytehub_lb::Token(i as u8 + 1), er.clone()));
        }
        v
    };

    // ── CancellationToken shared by probe tasks and the pool warmup task ──
    #[cfg(feature = "balance")]
    let cancel = {
        use tokio_util::sync::CancellationToken;
        CancellationToken::new()
    };

    // ── Active health probe tasks ─────────────────────────────────────────
    #[cfg(feature = "balance")]
    if let Some(probe_cfg) = &conn_opts.probe_config {
        use std::sync::Arc;
        let probe_cfg = Arc::new(probe_cfg.clone());
        let balancer = Arc::new(conn_opts.balancer.clone());

        for (idx, (token, fwd_raddr)) in peer_addrs.iter().enumerate() {
            // Resolve the effective probe address for this token:
            //   - If probe_targets[idx] exists, use it (e.g. the real remote
            //     tunnel server address, decoupled from the local forwarding hop).
            //   - Otherwise fall back to the forwarding address itself.
            let probe_addr = conn_opts
                .probe_targets
                .get(idx)
                .cloned()
                .unwrap_or_else(|| fwd_raddr.clone());

            let is_override = conn_opts.probe_targets.get(idx).is_some();
            log::info!(
                "[tcp] probe token={:?}: target={}{} fwd={}",
                token,
                probe_addr,
                if is_override { " (override)" } else { "" },
                fwd_raddr,
            );

            probe::spawn_probe_loop(
                *token,
                probe_addr,
                Arc::clone(&balancer),
                Arc::clone(&probe_cfg),
                cancel.child_token(),
            );
        }
        log::info!(
            "[tcp] started {} active-probe task(s) for {}",
            peer_addrs.len(),
            laddr
        );
    }

    // ── Connection pool ───────────────────────────────────────────────────
    #[cfg(feature = "balance")]
    let remote_pool: Option<std::sync::Arc<pool::RemotePool>> = {
        if let Some(pool_cfg) = &conn_opts.pool_config {
            let p = pool::RemotePool::new(pool_cfg.clone(), peer_addrs.clone());
            p.spawn_warmup_task(conn_opts.clone(), cancel.child_token());
            log::info!(
                "[tcp] connection pool enabled for {} (size={}, min_idle={})",
                laddr, pool_cfg.pool_size, pool_cfg.pool_min_idle
            );
            Some(p)
        } else {
            None
        }
    };

    let raddr = Ref::new(&raddr);
    let conn_opts = Ref::new(&conn_opts);
    let extra_raddrs = Ref::new(&extra_raddrs);

    let lis = socket::bind(&laddr, bind_opts).unwrap_or_else(|e| panic!("[tcp]failed to bind {}: {}", &laddr, e));
    let keepalive = socket::keepalive::build(&conn_opts);

    loop {
        let (local, addr) = match lis.accept().await {
            Ok(x) => x,
            Err(e) if e.kind() == ErrorKind::ConnectionAborted => {
                log::warn!("[tcp]failed to accept: {}", e);
                continue;
            }
            Err(e) => {
                log::error!("[tcp]failed to accept: {}", e);
                break;
            }
        };

        // Keep Nagle enabled: lets the kernel coalesce the obfs prefix
        // and first payload into one segment, reducing observable packet timing.

        if let Some(kpa) = &keepalive {
            use socket::keepalive::SockRef;
            SockRef::from(&local).set_tcp_keepalive(kpa)?;
        }

        // 挂载你设计的协议上下文
        let mode_num = match conn_opts.obfs.as_str() {
            "client" => 1,
            "server" => 2,
            _ => 0,
        };

        #[cfg(feature = "balance")]
        let pool_ref = remote_pool.clone();

        tokio::spawn(async move {
            bytehub_io::mem_copy::OBFS_MODE.scope(mode_num, async move {
                #[cfg(feature = "balance")]
                let res = connect_and_relay(local, raddr, conn_opts, extra_raddrs, pool_ref).await;
                #[cfg(not(feature = "balance"))]
                let res = connect_and_relay(local, raddr, conn_opts, extra_raddrs).await;
                match res {
                    Ok(..) => log::debug!("[tcp]{} => {}, finish", addr, raddr.as_ref()),
                    Err(e) => log::error!("[tcp]{} => {}, error: {}", addr, raddr.as_ref(), e),
                }
            }).await;
        });
    }

    // cancel drops here → all probe tasks and warmup task exit
    #[cfg(feature = "balance")]
    drop(cancel);

    Ok(())
}

