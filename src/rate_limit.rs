use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// A lock-free token bucket rate limiter.
///
/// Tokens are refilled over time at `per_second` rate, up to `max_burst`.
/// Each `try_acquire` consumes one token if available.
pub struct RateLimiter {
    max_burst: u64,
    /// Tokens added per nanosecond (stored as fixed-point: tokens * 1e9)
    nanos_per_token: u64,
    /// State packed as: available_tokens (top 32 bits) | timestamp_nanos_offset (bottom 32 bits)
    /// We use separate atomics for clarity and correctness.
    tokens_nanos: AtomicU64,
    last_refill_nanos: AtomicU64,
    epoch: Instant,
}

impl RateLimiter {
    /// Create a new rate limiter.
    ///
    /// - `max_burst`: maximum tokens that can be stored (burst capacity)
    /// - `per_second`: tokens added per second (refill rate)
    pub fn new(max_burst: u64, per_second: f64) -> Self {
        assert!(per_second > 0.0, "per_second must be positive");
        assert!(max_burst > 0, "max_burst must be positive");

        let nanos_per_token = (1_000_000_000.0 / per_second) as u64;
        let epoch = Instant::now();

        Self {
            max_burst,
            nanos_per_token,
            tokens_nanos: AtomicU64::new(max_burst),
            last_refill_nanos: AtomicU64::new(0),
            epoch,
        }
    }

    /// Try to acquire a single token. Returns `true` if a token was available.
    pub fn try_acquire(&self) -> bool {
        self.refill();

        loop {
            let current = self.tokens_nanos.load(Ordering::Acquire);
            if current == 0 {
                return false;
            }
            match self.tokens_nanos.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(_) => continue,
            }
        }
    }

    /// Refill tokens based on elapsed time.
    fn refill(&self) {
        let now_nanos = self.epoch.elapsed().as_nanos() as u64;
        let last = self.last_refill_nanos.load(Ordering::Acquire);
        let elapsed = now_nanos.saturating_sub(last);

        if elapsed < self.nanos_per_token {
            return; // Not enough time has passed for even one token
        }

        let new_tokens = elapsed / self.nanos_per_token;
        if new_tokens == 0 {
            return;
        }

        // Try to update the last_refill timestamp
        let new_last = last + new_tokens * self.nanos_per_token;
        if self
            .last_refill_nanos
            .compare_exchange(last, new_last, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            // We won the race — add the tokens
            loop {
                let current = self.tokens_nanos.load(Ordering::Acquire);
                let desired = (current + new_tokens).min(self.max_burst);
                if desired == current {
                    break;
                }
                match self.tokens_nanos.compare_exchange_weak(
                    current,
                    desired,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(_) => continue,
                }
            }
        }
        // If CAS failed, another thread handled the refill — that's fine.
    }
}
