use std::collections::{BTreeSet, HashMap, HashSet};

use chrono::{DateTime, Utc};
use crate::types::*;

use crate::store::StorageError;

/// In-memory authoritative state for the journaled engine.
pub(crate) struct MemState {
    pub queues: HashMap<QueueId, Queue>,
    pub flows: HashMap<FlowId, Flow>,
    pub tasks: HashMap<(TaskId, FlowId), Task>,

    /// Secondary index: queue_id -> set of (created_at, task_id_str, flow_id_str)
    /// for Ready tasks. Enables O(limit) fetch_ready_tasks instead of O(all_tasks) scan.
    ///
    /// We store stringified IDs because TaskId/FlowId do not implement Ord.
    pub ready_index: HashMap<QueueId, BTreeSet<(DateTime<Utc>, String, String)>>,

    /// Secondary index: (retry_at, task_id_str, flow_id_str) for Delayed tasks.
    pub delayed_index: BTreeSet<(DateTime<Utc>, String, String)>,

    /// Secondary index: Running tasks for timeout detection.
    pub running_index: HashSet<(TaskId, FlowId)>,

    /// Forward deps: (task_id, flow_id) -> [dep_task_ids]
    pub deps: HashMap<(TaskId, FlowId), Vec<TaskId>>,

    /// Reverse deps: (task_id, flow_id) -> [dependent_task_ids]
    pub dependents: HashMap<(TaskId, FlowId), Vec<TaskId>>,

    pub schedules: HashMap<ScheduleId, Schedule>,
}

impl MemState {
    pub fn new() -> Self {
        Self {
            queues: HashMap::new(),
            flows: HashMap::new(),
            tasks: HashMap::new(),
            ready_index: HashMap::new(),
            delayed_index: BTreeSet::new(),
            running_index: HashSet::new(),
            deps: HashMap::new(),
            dependents: HashMap::new(),
            schedules: HashMap::new(),
        }
    }

    /// Add a task to the appropriate secondary index based on its current state.
    pub(crate) fn index_add(&mut self, task: &Task) {
        match task.state {
            TaskState::Ready => {
                self.ready_index
                    .entry(task.queue_id.clone())
                    .or_default()
                    .insert((
                        task.created_at,
                        task.id.as_str().to_owned(),
                        task.flow_id.as_str().to_owned(),
                    ));
            }
            TaskState::Running => {
                self.running_index
                    .insert((task.id.clone(), task.flow_id.clone()));
            }
            TaskState::Delayed => {
                if let Some(retry_at) = task.retry_at {
                    self.delayed_index.insert((
                        retry_at,
                        task.id.as_str().to_owned(),
                        task.flow_id.as_str().to_owned(),
                    ));
                }
            }
            // Pending, Succeeded, Failed, Cancelled: no secondary index
            _ => {}
        }
    }

    /// Remove a task from the secondary index for its current state.
    pub(crate) fn index_remove(&mut self, task: &Task) {
        match task.state {
            TaskState::Ready => {
                if let Some(set) = self.ready_index.get_mut(&task.queue_id) {
                    set.remove(&(
                        task.created_at,
                        task.id.as_str().to_owned(),
                        task.flow_id.as_str().to_owned(),
                    ));
                }
            }
            TaskState::Running => {
                self.running_index
                    .remove(&(task.id.clone(), task.flow_id.clone()));
            }
            TaskState::Delayed => {
                if let Some(retry_at) = task.retry_at {
                    self.delayed_index.remove(&(
                        retry_at,
                        task.id.as_str().to_owned(),
                        task.flow_id.as_str().to_owned(),
                    ));
                }
            }
            _ => {}
        }
    }

    /// Remove a task from all secondary indexes unconditionally.
    /// Useful during deletion when the task state may be stale.
    pub(crate) fn index_remove_all(&mut self, task: &Task) {
        // Ready
        if let Some(set) = self.ready_index.get_mut(&task.queue_id) {
            set.remove(&(
                task.created_at,
                task.id.as_str().to_owned(),
                task.flow_id.as_str().to_owned(),
            ));
        }
        // Running
        self.running_index
            .remove(&(task.id.clone(), task.flow_id.clone()));
        // Delayed
        if let Some(retry_at) = task.retry_at {
            self.delayed_index.remove(&(
                retry_at,
                task.id.as_str().to_owned(),
                task.flow_id.as_str().to_owned(),
            ));
        }
    }

    /// Transition a task to a new state, maintaining secondary indexes.
    /// Returns an error if the transition is invalid.
    pub(crate) fn transition_task_state(
        &mut self,
        key: &(TaskId, FlowId),
        new_state: TaskState,
    ) -> Result<(), StorageError> {
        let task = self
            .tasks
            .get(key)
            .ok_or_else(|| StorageError::TaskNotFound(key.0.to_string(), key.1.to_string()))?;

        if !task.state.can_transition_to(new_state) {
            return Err(StorageError::InvalidStateTransition(
                task.state.to_string(),
                new_state.to_string(),
            ));
        }

        // Remove from old index (need to clone data before mutable borrow)
        let old_task_snapshot = task.clone();
        self.index_remove(&old_task_snapshot);

        // Update state
        let task = self.tasks.get_mut(key).expect("checked above");
        task.state = new_state;

        // Add to new index
        let new_task_snapshot = task.clone();
        self.index_add(&new_task_snapshot);

        Ok(())
    }
}

