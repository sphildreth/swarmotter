// SPDX-License-Identifier: Apache-2.0

//! LEDBAT-style delay-based congestion control for uTP (BEP 29).
//!
//! LEDBAT estimates the one-way queuing delay from timestamp echoes and keeps
//! the congestion window small enough to avoid building a queue, yielding to
//! competing TCP flows. This implementation tracks a base (minimum) delay and
//! the current (recent) delay per the algorithm, computes a queuing delay, and
//! grows/shrinks the congestion window toward a target delay.
//!
//! References: BEP 29, RFC 6817 (LEDBAT). This is a self-contained, testable
//! implementation independent of any socket; the connection feeds it samples
//! and loss/ack events and reads back a window size and retransmit timeout.

use std::time::{Duration, Instant};

/// The target one-way queuing delay above which LEDBAT shrinks the window.
/// 100 ms is a common uTP default.
const TARGET_DELAY: Duration = Duration::from_millis(100);

/// Minimum congestion window (one packet).
const MIN_CWND: u32 = 1400;

/// Maximum congestion window cap to prevent unbounded growth (8 MB).
const MAX_CWND: u32 = 8 * 1024 * 1024;

/// Slow-start threshold factor: grow additively once above target, but allow
/// a limited slow-start-style doubling while far below target and no loss.
const GAIN_FACTOR: u32 = 1;

/// Number of recent delay samples kept for the "current delay" filter.
const CURRENT_HISTORY: usize = 8;

/// How long a base-delay estimate remains valid before it is rotated (the
/// LEDBAT "base delay" history window). A short rotation keeps the estimate
/// responsive in tests; production uTP uses ~15 minutes per bucket.
const BASE_HISTORY: usize = 4;

/// A snapshot of the controller's observable state (for metrics/tests).
#[derive(Debug, Clone, Copy)]
pub struct CongestionState {
    pub window: u32,
    pub base_delay_micros: u32,
    pub current_delay_micros: u32,
    pub in_slow_start: bool,
}

/// A LEDBAT congestion controller. Window is in bytes.
#[derive(Debug, Clone)]
pub struct Ledbat {
    /// Congestion window in bytes.
    cwnd: u32,
    /// Whether slow-start (exponential growth) is active.
    slow_start: bool,
    /// Base-delay history: minimum one-way delay observed in each period.
    base_history: Vec<(Instant, u32)>,
    /// Recent delay samples for the current-delay filter.
    current_samples: Vec<u32>,
    /// Most recent echo delta to send back to the peer (microseconds).
    last_echo_delta: u32,
    /// Last time we recorded a loss (for RTO backoff).
    last_loss: Option<Instant>,
    /// Current retransmit timeout.
    rto: Duration,
}

impl Ledbat {
    /// Create a new controller starting in slow-start with a one-packet
    /// window.
    pub fn new() -> Self {
        Self {
            cwnd: MIN_CWND,
            slow_start: true,
            base_history: Vec::with_capacity(BASE_HISTORY + 1),
            current_samples: Vec::with_capacity(CURRENT_HISTORY + 1),
            last_echo_delta: 0,
            last_loss: None,
            rto: Duration::from_millis(500),
        }
    }

    /// Current congestion window in bytes.
    pub fn window_bytes(&self) -> usize {
        self.cwnd as usize
    }

    /// The retransmit timeout to apply to in-flight packets.
    pub fn rto(&self) -> Duration {
        self.rto
    }

    /// Most recent timestamp-delta echo to send to the peer (microseconds).
    pub fn last_echo_delta(&self) -> u32 {
        self.last_echo_delta
    }

    /// Feed a one-way delay sample (microseconds) and the peer's timestamp.
    /// `delay` is the measured one-way delay from the peer's echoed timestamp.
    pub fn on_sample(&mut self, peer_ts: u32, delay: u32) {
        self.last_echo_delta = delay;
        let now = Instant::now();

        // Update the current-delay filter (bounded moving set).
        self.current_samples.push(delay);
        if self.current_samples.len() > CURRENT_HISTORY {
            self.current_samples.remove(0);
        }

        // Update the base-delay history: keep the minimum per period. If the
        // oldest period is older than the rotation window, push a new bucket.
        let rotation = Duration::from_secs(10);
        let needs_new_bucket = self
            .base_history
            .last()
            .map(|(t, _)| now.duration_since(*t) > rotation)
            .unwrap_or(true);
        if needs_new_bucket {
            self.base_history.push((now, delay));
            if self.base_history.len() > BASE_HISTORY {
                self.base_history.remove(0);
            }
        } else {
            let last = self.base_history.last_mut().expect("non-empty");
            if delay < last.1 {
                last.1 = delay;
            }
        }
        let _ = peer_ts;
    }

    /// The current base (minimum) delay across the history.
    fn base_delay(&self) -> u32 {
        self.base_history.iter().map(|(_, d)| *d).min().unwrap_or(0)
    }

    /// The current delay (recent filtered estimate).
    fn current_delay(&self) -> u32 {
        if self.current_samples.is_empty() {
            return 0;
        }
        // Use the minimum of recent samples as the current-delay estimate,
        // which is robust to bursts and matches LEDBAT guidance.
        *self.current_samples.iter().min().unwrap_or(&0)
    }

    /// Queuing delay = current - base.
    fn queuing_delay(&self) -> u32 {
        self.current_delay().saturating_sub(self.base_delay())
    }

    /// Called when a cumulative ACK advances the send window. Grows the
    /// window if delay is below target; shrinks if above.
    pub fn on_ack(&mut self, _ack: u16) {
        let q = self.queuing_delay();
        let target = TARGET_DELAY.as_micros() as u32;
        if self.slow_start {
            if q >= target {
                // Exit slow-start; switch to additive growth.
                self.slow_start = false;
                self.cwnd = self.cwnd.saturating_add(GAIN_FACTOR * 1400);
            } else {
                // Exponential-ish growth (double per RTT). Approximate per ack.
                self.cwnd = self.cwnd.saturating_add(1400);
            }
        } else if q < target {
            // Additive growth proportional to how far below target we are.
            let headroom = target.saturating_sub(q);
            let gain = (1400u32).saturating_mul(headroom) / target.max(1);
            self.cwnd = self.cwnd.saturating_add(gain.max(1));
        } else {
            // Above target: shrink toward the target ratio.
            let over = q.saturating_sub(target);
            let ratio = over.min(target);
            let cut = (self.cwnd as u64 * ratio as u64 / target.max(1) as u64) as u32;
            self.cwnd = self.cwnd.saturating_sub(cut.max(1400));
        }
        self.cwnd = self.cwnd.clamp(MIN_CWND, MAX_CWND);
        // After a successful ack, relax the RTO back toward the baseline.
        self.rto = Duration::from_millis(500);
    }

    /// Called when a retransmit timeout fires (loss). Halves the window and
    /// backs off the RTO.
    pub fn on_loss(&mut self) {
        self.slow_start = false;
        self.cwnd = (self.cwnd / 2).max(MIN_CWND);
        self.rto = (self.rto * 2).min(Duration::from_secs(8));
        self.last_loss = Some(Instant::now());
    }

    /// Snapshot of the controller state for metrics/tests.
    pub fn state(&self) -> CongestionState {
        CongestionState {
            window: self.cwnd,
            base_delay_micros: self.base_delay(),
            current_delay_micros: self.current_delay(),
            in_slow_start: self.slow_start,
        }
    }
}

impl Default for Ledbat {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_starts_at_one_packet_and_grows_under_low_delay() {
        let mut l = Ledbat::new();
        let start = l.window_bytes();
        assert_eq!(start, MIN_CWND as usize);
        // Feed low-delay samples so growth is permitted.
        for _ in 0..20 {
            l.on_sample(0, 1_000); // 1ms
            l.on_ack(0);
        }
        assert!(
            l.window_bytes() > start,
            "window should grow under low delay; got {}",
            l.window_bytes()
        );
    }

    #[test]
    fn window_shrinks_under_high_delay() {
        let mut l = Ledbat::new();
        // Establish a low base delay.
        for _ in 0..5 {
            l.on_sample(0, 1_000);
            l.on_ack(0);
        }
        let before = l.window_bytes();
        // Now feed high-delay samples (well above target).
        for _ in 0..20 {
            l.on_sample(0, 200_000); // 200ms > 100ms target
            l.on_ack(0);
        }
        let after = l.window_bytes();
        assert!(
            after < before || after <= MIN_CWND as usize,
            "window should shrink under high delay; before {before} after {after}"
        );
    }

    #[test]
    fn loss_halves_window_and_backs_off_rto() {
        let mut l = Ledbat::new();
        // Grow the window first.
        for _ in 0..20 {
            l.on_sample(0, 1_000);
            l.on_ack(0);
        }
        let before = l.window_bytes();
        let rto_before = l.rto();
        l.on_loss();
        assert!(l.window_bytes() <= before / 2 + MIN_CWND as usize);
        assert!(l.rto() > rto_before);
    }

    #[test]
    fn window_is_bounded() {
        let mut l = Ledbat::new();
        for _ in 0..100_000 {
            l.on_sample(0, 1);
            l.on_ack(0);
        }
        assert!(l.window_bytes() <= MAX_CWND as usize);
    }

    #[test]
    fn state_snapshot_reports_delays() {
        let mut l = Ledbat::new();
        l.on_sample(0, 5_000);
        let s = l.state();
        assert_eq!(s.window, l.window_bytes() as u32);
        assert!(s.base_delay_micros <= 5_000);
    }
}
