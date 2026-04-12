use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use rusqlite::Connection;
use tokio::sync::mpsc;

use super::events::{JournalEntry, JournalEvent};
use super::snapshot;
use super::state::MemState;

pub(crate) struct JournalWriter {
    rx: mpsc::Receiver<JournalEntry>,
    conn: Connection,
    config: WriterConfig,
    flush_watermark: Arc<AtomicU64>,
    dead_flag: Arc<AtomicBool>,
    state: Arc<RwLock<MemState>>,
    snapshot_path: Option<PathBuf>,
    entries_since_snapshot: u64,
    last_snapshot: Instant,
    consecutive_failures: u32,
}

pub(crate) struct WriterConfig {
    pub flush_interval: Duration,
    pub max_batch_size: usize,
    pub snapshot_interval: u64,
    pub snapshot_time_interval: Duration,
}

impl JournalWriter {
    pub fn new(
        rx: mpsc::Receiver<JournalEntry>,
        journal_path: &PathBuf,
        config: WriterConfig,
        flush_watermark: Arc<AtomicU64>,
        dead_flag: Arc<AtomicBool>,
        state: Arc<RwLock<MemState>>,
        snapshot_path: Option<PathBuf>,
    ) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open(journal_path)?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             PRAGMA busy_timeout = 5000;
             PRAGMA cache_size = -4000;
             PRAGMA temp_store = MEMORY;",
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS journal (
                seq        INTEGER PRIMARY KEY,
                event_type INTEGER NOT NULL,
                payload    BLOB NOT NULL,
                crc32      INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )",
            [],
        )?;
        Ok(Self {
            rx,
            conn,
            config,
            flush_watermark,
            dead_flag,
            state,
            snapshot_path,
            entries_since_snapshot: 0,
            last_snapshot: Instant::now(),
            consecutive_failures: 0,
        })
    }

    /// Main writer loop. Runs until the channel is closed.
    pub async fn run(mut self) {
        let mut batch = Vec::with_capacity(self.config.max_batch_size);
        loop {
            batch.clear();

            // Wait for at least one entry (or channel close)
            match tokio::time::timeout(self.config.flush_interval, self.rx.recv()).await {
                Ok(Some(entry)) => batch.push(entry),
                Ok(None) => break,  // channel closed
                Err(_) => continue, // timeout, no entries
            }

            // Drain up to max_batch_size more without waiting
            while batch.len() < self.config.max_batch_size {
                match self.rx.try_recv() {
                    Ok(entry) => batch.push(entry),
                    Err(_) => break,
                }
            }

            // Write batch to SQLite with retry on transient errors
            if let Err(e) = self.write_batch_with_retry(&batch) {
                tracing::error!(error = %e, batch_size = batch.len(), "journal write failed after retries");
                self.dead_flag.store(true, Ordering::Release);
                break;
            }

            // Update watermark
            if let Some(last) = batch.last() {
                self.flush_watermark.store(last.seq, Ordering::Release);
            }

            metrics::histogram!("tasked_journal_write_batch_size").record(batch.len() as f64);

            // Check if a snapshot is due
            self.entries_since_snapshot += batch.len() as u64;
            if self.snapshot_path.is_some()
                && (self.entries_since_snapshot >= self.config.snapshot_interval
                    || self.last_snapshot.elapsed() >= self.config.snapshot_time_interval)
            {
                if let Err(e) = self.take_snapshot() {
                    tracing::error!(error = %e, "snapshot failed");
                    // Snapshot failure is non-fatal; journal is still intact
                } else {
                    self.entries_since_snapshot = 0;
                    self.last_snapshot = Instant::now();
                }
            }
        }
    }

    /// Write a batch with retry on transient errors.
    ///
    /// On first failure: log warning, sleep 100ms, retry once.
    /// On third consecutive failure: give up and return the error (caller sets dead_flag).
    fn write_batch_with_retry(&mut self, batch: &[JournalEntry]) -> Result<(), rusqlite::Error> {
        match self.write_batch(batch) {
            Ok(()) => {
                self.consecutive_failures = 0;
                Ok(())
            }
            Err(e) => {
                self.consecutive_failures += 1;
                if self.consecutive_failures < 3 {
                    tracing::warn!(
                        error = %e,
                        attempt = self.consecutive_failures,
                        "journal write failed, retrying"
                    );
                    std::thread::sleep(Duration::from_millis(100));
                    match self.write_batch(batch) {
                        Ok(()) => {
                            self.consecutive_failures = 0;
                            Ok(())
                        }
                        Err(e2) => {
                            self.consecutive_failures += 1;
                            Err(e2)
                        }
                    }
                } else {
                    Err(e)
                }
            }
        }
    }

    fn write_batch(&self, batch: &[JournalEntry]) -> Result<(), rusqlite::Error> {
        let start = std::time::Instant::now();

        let tx = self.conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO journal (seq, event_type, payload, crc32, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for entry in batch {
                let payload =
                    serde_json::to_vec(&entry.event).expect("JSON serialization should not fail");
                let crc = crc32fast::hash(&payload);
                let event_type = event_discriminant(&entry.event);
                stmt.execute(rusqlite::params![
                    entry.seq as i64,
                    event_type,
                    payload,
                    crc as i64,
                    entry.created_at.to_rfc3339(),
                ])?;
            }
        }
        tx.commit()?;

        let elapsed = start.elapsed();
        metrics::histogram!("tasked_journal_flush_duration_seconds").record(elapsed.as_secs_f64());

        Ok(())
    }

    fn take_snapshot(&self) -> Result<(), crate::store::StorageError> {
        let snapshot_path = self
            .snapshot_path
            .as_ref()
            .expect("take_snapshot called without snapshot_path");

        let watermark = self.flush_watermark.load(Ordering::Acquire);

        // Take a read lock on state for the duration of the snapshot write
        let state_guard = self.state.read();
        snapshot::write_snapshot(&state_guard, snapshot_path, watermark)?;
        drop(state_guard);

        // Compact journal: delete entries up to the snapshot watermark
        self.conn
            .execute(
                "DELETE FROM journal WHERE seq <= ?1",
                rusqlite::params![watermark as i64],
            )
            .map_err(|e| crate::store::StorageError::Internal(format!("journal compact: {e}")))?;

        tracing::info!(
            watermark = watermark,
            snapshot_path = %snapshot_path.display(),
            "snapshot written and journal compacted"
        );

        Ok(())
    }
}

/// Map JournalEvent variant to a stable integer discriminant.
fn event_discriminant(event: &JournalEvent) -> i32 {
    match event {
        JournalEvent::QueueCreated { .. } => 1,
        JournalEvent::QueueDeleted { .. } => 2,
        JournalEvent::FlowCreated { .. } => 3,
        JournalEvent::FlowStateChanged { .. } => 4,
        JournalEvent::TaskCompleted { .. } => 5,
        JournalEvent::TaskStateChanged { .. } => 6,
        JournalEvent::TaskOutputSet { .. } => 7,
        JournalEvent::TasksInjected { .. } => 8,
        JournalEvent::FlowsDeleted { .. } => 9,
        JournalEvent::ScheduleCreated { .. } => 10,
        JournalEvent::ScheduleUpdated { .. } => 11,
        JournalEvent::ScheduleDeleted { .. } => 12,
        JournalEvent::ScheduleTriggered { .. } => 13,
    }
}
