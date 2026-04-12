use crate::types::*;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Every state-change that must survive a crash.
/// Applied in sequence to empty MemState, these reproduce correct state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum JournalEvent {
    // Queue lifecycle
    QueueCreated {
        queue: Queue,
    },
    QueueDeleted {
        queue_id: QueueId,
    },

    // Flow lifecycle
    FlowCreated {
        flow: Flow,
        tasks: Vec<Task>,
        deps: HashMap<TaskId, Vec<TaskId>>,
    },
    FlowStateChanged {
        flow_id: FlowId,
        new_state: FlowState,
        updated_at: DateTime<Utc>,
    },

    // Hot path -- combined completion event
    TaskCompleted {
        task_id: TaskId,
        flow_id: FlowId,
        new_state: TaskState, // Succeeded or Failed
        output: Option<serde_json::Value>,
        error: Option<String>,
        completed_at: DateTime<Utc>,
        succeeded: bool, // for counter increment
        newly_ready: Vec<TaskId>,
    },

    // Non-completion task state transitions
    TaskStateChanged {
        task_id: TaskId,
        flow_id: FlowId,
        new_state: TaskState,
        retry_at: Option<DateTime<Utc>>,
        started_at: Option<DateTime<Utc>>,
        retries_remaining: Option<u32>,
    },

    // Task output without state change (approval executor)
    TaskOutputSet {
        task_id: TaskId,
        flow_id: FlowId,
        output: serde_json::Value,
    },

    // Dynamic task injection (spawn executor)
    TasksInjected {
        flow_id: FlowId,
        tasks: Vec<Task>,
        deps: HashMap<TaskId, Vec<TaskId>>,
        new_task_count: usize,
    },

    // Bulk deletion
    FlowsDeleted {
        queue_id: QueueId,
        flow_ids: Vec<FlowId>,
    },

    // Schedule lifecycle
    ScheduleCreated {
        schedule: Schedule,
    },
    ScheduleUpdated {
        schedule: Schedule,
    },
    ScheduleDeleted {
        schedule_id: ScheduleId,
    },
    ScheduleTriggered {
        schedule_id: ScheduleId,
        triggered_at: DateTime<Utc>,
        next_run_at: Option<DateTime<Utc>>,
    },
}

/// A journal entry ready to be written.
pub(crate) struct JournalEntry {
    pub seq: u64,
    pub event: JournalEvent,
    pub created_at: DateTime<Utc>,
}
