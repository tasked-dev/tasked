use std::path::PathBuf;
use std::time::Duration;

/// Configuration for the journaled storage engine.
#[derive(Debug, Clone)]
pub struct JournalConfig {
    /// Path to the journal SQLite database.
    /// `None` = in-memory only (no journal thread).
    pub journal_path: Option<PathBuf>,
    /// Path to the snapshot SQLite database.
    /// Defaults to `snapshot.db` in the same directory as the journal.
    pub snapshot_path: Option<PathBuf>,
    /// Retained for backwards compatibility. The journal channel is now
    /// unbounded so events can be enqueued while the state lock is held
    /// (preserving apply order == journal order); the writer's batch size
    /// and `health_check` failure detection bound the backlog in practice.
    pub channel_capacity: usize,
    /// Retained for backwards compatibility. The writer flushes as soon as
    /// entries are available (batching opportunistically up to
    /// `max_batch_size`) rather than waiting a fixed interval.
    pub flush_interval: Duration,
    /// Max entries per flush batch. Default: 512.
    pub max_batch_size: usize,
    /// Take a snapshot every N journal entries. Default: 50,000.
    pub snapshot_interval: u64,
    /// Take a snapshot after this duration since last snapshot. Default: 300s.
    pub snapshot_time_interval: Duration,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            journal_path: None,
            snapshot_path: None,
            channel_capacity: 8192,
            flush_interval: Duration::from_millis(5),
            max_batch_size: 512,
            snapshot_interval: 50_000,
            snapshot_time_interval: Duration::from_secs(300),
        }
    }
}
