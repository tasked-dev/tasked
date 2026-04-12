//! Lightweight per-operation timing telemetry for benchmarking.
//!
//! Accumulates nanoseconds in global atomic counters. Zero allocation.
//! Call [`reset`] before a benchmark run, then [`snapshot`] after to read results.
//!
//! Each counter tracks cumulative nanoseconds + call count for a specific operation.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

macro_rules! define_counters {
    ($($name:ident),+ $(,)?) => {
        /// Global perf counters — one pair (ns, count) per operation.
        #[allow(non_upper_case_globals)]
        pub mod counters {
            use super::*;
            $(
                pub static $name: Counter = Counter::new();
            )+
        }

        /// Reset all counters to zero.
        pub fn reset() {
            $(counters::$name.reset();)+
        }

        /// Snapshot all counters into a displayable report.
        pub fn snapshot() -> Snapshot {
            Snapshot {
                $(
                    $name: counters::$name.snapshot(),
                )+
            }
        }

        /// Point-in-time snapshot of all counters.
        pub struct Snapshot {
            $(pub $name: CounterSnapshot,)+
        }

        impl std::fmt::Display for Snapshot {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "[telemetry]")?;
                $(
                    if self.$name.count > 0 {
                        write!(f, " {}={:.0}us(x{})", stringify!($name), self.$name.avg_us(), self.$name.count)?;
                    }
                )+
                Ok(())
            }
        }
    };
}

define_counters!(
    // Dispatch phase
    fetch_ready,
    recheck_state,
    resolve_vars,
    mark_running,
    get_flow_depth,
    // Completion phase
    mark_succeeded,
    mark_failed,
    inc_flow_counter,
    resolve_ready,
    // Cycle overhead
    promote_delayed,
    get_queue_config,
    dispatch_total,
    completion_total,
);

/// A single atomic counter tracking cumulative nanoseconds and call count.
pub struct Counter {
    ns: AtomicU64,
    count: AtomicU64,
}

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}

impl Counter {
    pub const fn new() -> Self {
        Self {
            ns: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Record elapsed time since `start`.
    #[inline]
    pub fn record(&self, start: Instant) {
        let elapsed = start.elapsed().as_nanos() as u64;
        self.ns.fetch_add(elapsed, Relaxed);
        self.count.fetch_add(1, Relaxed);
    }

    pub fn reset(&self) {
        self.ns.store(0, Relaxed);
        self.count.store(0, Relaxed);
    }

    pub fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            total_ns: self.ns.load(Relaxed),
            count: self.count.load(Relaxed),
        }
    }
}

#[derive(Clone, Copy)]
pub struct CounterSnapshot {
    pub total_ns: u64,
    pub count: u64,
}

impl CounterSnapshot {
    pub fn avg_us(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            (self.total_ns as f64 / self.count as f64) / 1000.0
        }
    }
}
