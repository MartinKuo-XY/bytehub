/// Peer token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Token(pub u8);

/// Health check configuration for downstream node failover.
#[derive(Debug, Clone, Copy)]
pub struct HealthCheckConfig {
    /// Maximum consecutive failures before marking a node unhealthy.
    pub max_fails: u32,
    /// Duration in seconds that an unhealthy node stays excluded.
    pub fail_timeout_secs: u32,
    /// Maximum connect latency in milliseconds before treating as failure.
    /// `None` disables latency-based failover (default).
    pub max_latency_ms: Option<u32>,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            max_fails: 2,
            fail_timeout_secs: 120,
            max_latency_ms: None,
        }
    }
}

/// Load balance traits.
pub trait Balance {
    type State;

    /// Constructor.
    fn new(weights: &[u8], config: Option<HealthCheckConfig>) -> Self;

    /// Get next peer.
    fn next(&self, state: &Self::State) -> Option<Token>;

    /// Total peers.
    fn total(&self) -> u8;

    /// Record success for a peer.
    fn on_success(&self, _token: Token) {}

    /// Record failure for a peer.
    fn on_failure(&self, _token: Token) {}
}

/// Downstream health tracker shared between LB and relay endpoints.
pub mod health;

/// Iphash impl.
pub mod ip_hash;

/// Round-robin impl.
pub mod round_robin;

mod balancer;
pub use balancer::{Balancer, BalanceCtx, Strategy};
