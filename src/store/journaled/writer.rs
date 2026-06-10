use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use rusqlite::Connection;
use tokio::sync::{mpsc, watch};

use super::events::{JournalEntry, JournalEvent};
use super::snapshot;
use super::state::MemState;

/// Journal writer. Performs synchronous SQLite commits (fsyncs), so it runs
/// on a dedicated OS thread (see `JournaledStorage::open`); the unbounded
/// tokio channel is the boundary between async producers and this thread.
pub(crate) struct JournalWriter {
    rx: mpsc::UnboundedReceiver<JournalEntry>,
    conn: Connection,
    config: WriterConfig,
    /// Watermark of the last fsync'd sequence, published after each batch.
    /// emit_durable waiters observe this; dropping the sender on exit wakes
    /// them so they can fail instead of hanging.
    watermark_tx: watch::Sender<u64>,
    dead_flag: Arc<AtomicBool>,
    state: Arc<RwLock<MemState>>,
    snapshot_path: Option<PathBuf>,
    entries_since_snapshot: u64,
    last_snapshot: Instant,
    consecutive_failures: u32,
}

pub(crate) struct WriterConfig {
    pub max_batch_size: usize,
    pub snapshot_interval: u64,
    pub snapshot_time_interval: Duration,
}

impl JournalWriter {
    pub fn new(
        rx: mpsc::UnboundedReceiver<JournalEntry>,
        journal_path: &PathBuf,
        config: WriterConfig,
        watermark_tx: watch::Sender<u64>,
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
            watermark_tx,
            dead_flag,
            state,
            snapshot_path,
            entries_since_snapshot: 0,
            last_snapshot: Instant::now(),
            consecutive_failures: 0,
        })
    }

    /// Main writer loop. Runs on a dedicated OS thread until the channel is
    /// closed (all senders dropped) and drained.
    pub fn run(mut self) {
        let mut batch = Vec::with_capacity(self.config.max_batch_size);
        loop {
            batch.clear();

            // Block for at least one entry (or channel close).
            match self.rx.blocking_recv() {
                Some(entry) => batch.push(entry),
                None => break, // channel closed and drained
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

            // Publish the new durable watermark (wakes emit_durable waiters)
            if let Some(last) = batch.last() {
                let _ = self.watermark_tx.send(last.seq);
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
        // Dropping self drops watermark_tx, which wakes any emit_durable
        // waiters so they can observe journal_dead / channel closure.
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
                // Serialization failure is surfaced like any other write
                // failure (retried, then marks the journal dead) instead of
                // panicking the writer thread.
                let payload = serde_json::to_vec(&entry.event)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
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

        // Read the watermark BEFORE cloning the state. Every event with
        // seq <= watermark was applied to memory inside the same critical
        // section that enqueued it (emit_locked), and was then fsync'd, so
        // the clone taken below is guaranteed to contain all of them. The
        // snapshot may additionally contain newer (not yet flushed)
        // mutations whose journal entries survive compaction; replay of
        // those entries is idempotent.
        let watermark = *self.watermark_tx.borrow();

        // Clone state under the read lock, then release it before the
        // (potentially slow) serialization + fsync so writers are not
        // blocked for the duration of the snapshot.
        let state_clone = { self.state.read().clone() };
        snapshot::write_snapshot(&state_clone, snapshot_path, watermark)?;

        // Compact journal: delete entries up to the snapshot watermark
        self.conn
            .execute(
                "DELETE FROM journal WHERE seq <= ?1",
                rusqlite::params![watermark as i64],
            )
            .map_err(|e| crate::store::StorageError::Internal(format!("journal compact: {e}")))?;

        // Checkpoint + truncate the WAL so the journal file actually shrinks.
        self.conn
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .map_err(|e| {
                crate::store::StorageError::Internal(format!("journal checkpoint: {e}"))
            })?;

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
