use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::HealthCheckConfig;

/// Per-downstream-node health state.
#[derive(Debug)]
pub struct DownstreamNode {
    /// Consecutive failure count.
    pub fails: AtomicU32,
    /// Unix timestamp until this node is excluded.
    /// 0 means the node is currently healthy (or has no pending timeout).
    pub checked: AtomicU32,
}

/// Shared downstream health tracker.
///
/// Used by both the LB endpoint (to skip unhealthy nodes during connection)
/// and relay endpoints (to report downstream connection success/failure).
#[derive(Debug)]
pub struct DownstreamHealth {
    nodes: Vec<DownstreamNode>,
    config: HealthCheckConfig,
}

impl DownstreamHealth {
    /// Create a new health tracker for `count` nodes.
    pub fn new(count: usize, config: HealthCheckConfig) -> Self {
        let nodes = (0..count)
            .map(|_| DownstreamNode {
                fails: AtomicU32::new(0),
                checked: AtomicU32::new(0),
            })
            .collect();
        Self { nodes, config }
    }

    #[inline]
    fn now_secs() -> u32 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as u32
    }

    /// Check if node at `idx` is currently healthy.
    ///
    /// Automatically recovers a node when its fail timeout expires.
    pub fn is_healthy(&self, idx: usize) -> bool {
        if let Some(node) = self.nodes.get(idx) {
            let fails = node.fails.load(Ordering::Relaxed);
            if fails < self.config.max_fails {
                return true;
            }

            let now = Self::now_secs();
            let checked = node.checked.load(Ordering::Relaxed);
            if checked == 0 || now >= checked {
                // Timeout expired — auto-recover
                node.fails.store(0, Ordering::Relaxed);
                node.checked.store(0, Ordering::Relaxed);
                return true;
            }
            false
        } else {
            // Out-of-range index: not healthy
            false
        }
    }

    /// Report a successful downstream connection for node at `idx`.
    /// Resets the failure counter.
    pub fn report_success(&self, idx: usize) {
        if let Some(node) = self.nodes.get(idx) {
            node.fails.store(0, Ordering::Relaxed);
            node.checked.store(0, Ordering::Relaxed);
        }
    }

    /// Report a failed downstream connection for node at `idx`.
    ///
    /// Returns `true` if the node just transitioned from healthy to unhealthy
    /// (i.e. the failure threshold was crossed), allowing callers to log the event.
    pub fn report_failure(&self, idx: usize) -> bool {
        if let Some(node) = self.nodes.get(idx) {
            let fails = node.fails.fetch_add(1, Ordering::Relaxed) + 1;
            if fails >= self.config.max_fails {
                let now = Self::now_secs();
                node.checked
                    .store(now + self.config.fail_timeout_secs, Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Get the total number of nodes tracked.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Check if there are any nodes at all.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Get a reference to the underlying config.
    pub fn config(&self) -> &HealthCheckConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_health_default_healthy() {
        let h = DownstreamHealth::new(3, HealthCheckConfig::default());
        assert!(h.is_healthy(0));
        assert!(h.is_healthy(1));
        assert!(h.is_healthy(2));
    }

    #[test]
    fn test_health_fail_threshold() {
        let h = DownstreamHealth::new(2, HealthCheckConfig {
            max_fails: 2,
            fail_timeout_secs: 3600,
        });

        assert!(!h.report_failure(0)); // fails=1, not yet unhealthy
        assert!(h.is_healthy(0));
        assert!(h.report_failure(0));  // fails=2, now unhealthy
        assert!(!h.is_healthy(0));
        assert!(h.is_healthy(1));       // other node unaffected
    }

    #[test]
    fn test_health_recovery() {
        let h = DownstreamHealth::new(1, HealthCheckConfig {
            max_fails: 1,
            fail_timeout_secs: 1, // 1 second timeout
        });

        h.report_failure(0); // immediately unhealthy
        assert!(!h.is_healthy(0));

        thread::sleep(Duration::from_secs(2));
        assert!(h.is_healthy(0)); // timeout expired, auto-recovered
    }

    #[test]
    fn test_health_success_resets() {
        let h = DownstreamHealth::new(1, HealthCheckConfig {
            max_fails: 3,
            fail_timeout_secs: 3600,
        });

        h.report_failure(0); // fails=1
        h.report_success(0); // reset
        assert!(h.is_healthy(0));
    }

    #[test]
    fn test_health_report_failure_return_value() {
        let h = DownstreamHealth::new(1, HealthCheckConfig {
            max_fails: 2,
            fail_timeout_secs: 3600,
        });
        assert!(!h.report_failure(0)); // 1/2
        assert!(h.report_failure(0));  // 2/2 -> transitioned
    }
}
