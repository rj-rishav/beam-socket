//! RFC 0004 mesh spike — shared plumbing. Throwaway-grade (RFC 0001 rule):
//! the point is measured dynamics, not production polish.

pub mod relay;
pub mod swim;

/// SWIM tuning rows (RFC 0004 §4.2 table). The spike measures BOTH.
#[derive(Debug, Clone, Copy)]
pub struct SwimParams {
    pub period_ms: u64,
    pub probe_timeout_ms: u64,
    pub indirect_k: usize,
    pub suspicion_ms: u64,
    /// Max piggybacked updates per packet.
    pub gossip_max: usize,
    /// Retransmissions per accepted update (λ·log₂(N+1), N≤5 → 8).
    pub retransmit: u32,
}

impl SwimParams {
    /// memberlist-ish literature defaults (sized for GC-pausing runtimes).
    pub fn literature() -> Self {
        Self {
            period_ms: 1000,
            probe_timeout_ms: 500,
            indirect_k: 3,
            suspicion_ms: 5000, // 4·T·log(N) ≈ 5 s @ N=5
            gossip_max: 8,
            retransmit: 8,
        }
    }

    /// The RFC's tuned prior for non-GC-pausing Tokio nodes (P1).
    pub fn tuned() -> Self {
        Self {
            period_ms: 500,
            probe_timeout_ms: 250,
            indirect_k: 3,
            suspicion_ms: 2500, // 2·T·log(N) ≈ 2.5 s @ N=5
            gossip_max: 8,
            retransmit: 8,
        }
    }

    pub fn by_name(name: &str) -> Self {
        match name {
            "literature" => Self::literature(),
            _ => Self::tuned(),
        }
    }
}

/// Deterministic port layout: swim UDP / relay TCP / admin TCP per node.
pub fn swim_port(base: u16, id: u16) -> u16 {
    base + id * 10
}
pub fn relay_port(base: u16, id: u16) -> u16 {
    base + id * 10 + 1
}
pub fn admin_port(base: u16, id: u16) -> u16 {
    base + id * 10 + 2
}

/// CLOCK_MONOTONIC nanos — comparable ACROSS PROCESSES on one Linux box,
/// which is what makes the one-way hop measurement honest (both spike
/// processes read the same clock; no NTP step can skew it).
pub fn mono_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// Wall-clock ms (epoch) — for coordinator-correlated event timestamps.
pub fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Reservoir sampler (Algorithm R) — same percentile discipline as the
/// RFC 0001 harness.
pub struct Reservoir {
    cap: usize,
    seen: u64,
    samples: Vec<u64>,
}

impl Reservoir {
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            seen: 0,
            samples: Vec::with_capacity(cap),
        }
    }

    pub fn push(&mut self, v: u64) {
        self.seen += 1;
        if self.samples.len() < self.cap {
            self.samples.push(v);
        } else {
            let j = rand::random::<u64>() % self.seen;
            if (j as usize) < self.cap {
                self.samples[j as usize] = v;
            }
        }
    }

    pub fn percentile(&mut self, p: f64) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        self.samples.sort_unstable();
        let idx = ((self.samples.len() as f64 - 1.0) * p).round() as usize;
        self.samples[idx]
    }

    pub fn count(&self) -> u64 {
        self.seen
    }
}
