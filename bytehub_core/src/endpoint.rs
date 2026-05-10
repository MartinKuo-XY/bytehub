//! Relay endpoint.

use std::fmt::{Display, Formatter};
use std::net::SocketAddr;

#[cfg(feature = "transport")]
use kaminari::mix::{MixAccept, MixConnect};

#[cfg(feature = "balance")]
use bytehub_lb::Balancer;

/// Remote address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteAddr {
    SocketAddr(SocketAddr),
    DomainName(String, u16),
}

#[cfg(feature = "proxy")]
#[derive(Debug, Default, Clone, Copy)]
pub struct ProxyOpts {
    pub send_proxy: bool,
    pub accept_proxy: bool,
    pub send_proxy_version: usize,
    pub accept_proxy_timeout: usize,
}

#[cfg(feature = "proxy")]
impl ProxyOpts {
    #[inline]
    pub(crate) const fn enabled(&self) -> bool {
        self.send_proxy || self.accept_proxy
    }
}

#[derive(Debug, Default, Clone)]
pub struct ConnectOpts {
    pub send_mptcp: bool,
    pub connect_timeout: usize,
    pub associate_timeout: usize,
    pub tcp_keepalive: usize,
    pub tcp_keepalive_probe: usize,
    pub bind_address: Option<SocketAddr>,
    pub bind_interface: Option<String>,
    
    // ======== 新增混淆字段 ========
    pub obfs: String, 
    // =============================

    #[cfg(feature = "proxy")]
    pub proxy_opts: ProxyOpts,

    #[cfg(feature = "transport")]
    pub transport: Option<(MixAccept, MixConnect)>,

    #[cfg(feature = "balance")]
    pub balancer: Balancer,

    /// Active health probe configuration.
    /// When `Some`, a background TCP-ping task is launched for each peer.
    #[cfg(feature = "balance")]
    pub probe_config: Option<crate::tcp::probe::ProbeConfig>,

    /// Per-peer probe targets, parallel to `[raddr] + extra_raddrs`.
    ///
    /// When non-empty, the active probe pings these addresses instead of the
    /// actual forwarding addresses.  This is useful when forwarding goes
    /// through a local tunnel client (e.g. `127.0.0.1:10001`) while the real
    /// reachability check must target the remote tunnel server directly
    /// (e.g. `real-server.example.com:443`).
    ///
    /// Mapping rule:
    ///   - `probe_targets[0]` overrides the probe address for Token(0) / `raddr`
    ///   - `probe_targets[1]` overrides Token(1) / `extra_raddrs[0]`, etc.
    ///
    /// If the list is shorter than the peer list, missing entries fall back to
    /// the actual forwarding address for that token.  An empty list means all
    /// peers are probed at their forwarding address (current default behaviour).
    #[cfg(feature = "balance")]
    pub probe_targets: Vec<RemoteAddr>,

    /// Connection pool configuration.
    /// When `Some`, a pre-warmed pool of idle remote connections is maintained.
    #[cfg(feature = "balance")]
    pub pool_config: Option<crate::tcp::pool::PoolConfig>,
}

#[derive(Debug, Default, Clone)]
pub struct BindOpts {
    pub ipv6_only: bool,
    pub accept_mptcp: bool,
    pub bind_interface: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Endpoint {
    pub laddr: SocketAddr,
    pub raddr: RemoteAddr,
    pub bind_opts: BindOpts,
    pub conn_opts: ConnectOpts,
    pub extra_raddrs: Vec<RemoteAddr>,
}

impl Display for RemoteAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use RemoteAddr::*;
        match self {
            SocketAddr(addr) => write!(f, "{}", addr),
            DomainName(host, port) => write!(f, "{}:{}", host, port),
        }
    }
}

impl Display for Endpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> [{}", &self.laddr, &self.raddr)?;
        for raddr in self.extra_raddrs.iter() {
            write!(f, "|{}", raddr)?;
        }
        write!(f, "]; options: {}; {}", &self.bind_opts, &self.conn_opts)
    }
}

impl Display for BindOpts {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let BindOpts { accept_mptcp, ipv6_only, bind_interface } = self;
        if let Some(iface) = bind_interface {
            write!(f, "listen-iface={}, ", iface)?;
        }
        write!(f, "ipv6-only={}, ", ipv6_only)?;
        write!(f, "accept-mptcp={}", accept_mptcp)?;
        Ok(())
    }
}

impl Display for ConnectOpts {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let ConnectOpts {
            send_mptcp, connect_timeout, associate_timeout, tcp_keepalive, tcp_keepalive_probe,
            bind_address, bind_interface, obfs,
            #[cfg(feature = "proxy")] proxy_opts,
            #[cfg(feature = "transport")] transport,
            #[cfg(feature = "balance")] balancer,
            #[cfg(feature = "balance")] probe_config: _,
            #[cfg(feature = "balance")] probe_targets: _,
            #[cfg(feature = "balance")] pool_config: _,
        } = self;

        if let Some(iface) = bind_interface { write!(f, "send-iface={}, ", iface)?; }
        if let Some(send_through) = bind_address { write!(f, "send-through={}, ", send_through)?; }
        if !obfs.is_empty() { write!(f, "obfs={}, ", obfs)?; }
        write!(f, "send-mptcp={}; ", send_mptcp)?;

        #[cfg(feature = "proxy")]
        {
            let ProxyOpts { send_proxy, accept_proxy, send_proxy_version, accept_proxy_timeout } = proxy_opts;
            write!(f, "send-proxy={0}, send-proxy-version={2}, accept-proxy={1}, accept-proxy-timeout={3}s; ", send_proxy, accept_proxy, send_proxy_version, accept_proxy_timeout)?;
        }

        write!(f, "tcp-keepalive={}s[{}] connect-timeout={}s, associate-timeout={}s; ", tcp_keepalive, tcp_keepalive_probe, connect_timeout, associate_timeout)?;

        #[cfg(feature = "transport")]
        if let Some((ac, cc)) = transport { write!(f, "transport={}||{}; ", ac, cc)?; }

        #[cfg(feature = "balance")]
        write!(f, "balance={}", balancer.strategy())?;
        Ok(())
    }
}
