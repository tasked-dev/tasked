use super::{Storage, StorageError};
use crate::types::*;
use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Mutex;

/// In-memory storage backend for testing and ephemeral use.
pub struct MemoryStorage {
    inner: Mutex<Inner>,
}

struct Inner {
    queues: HashMap<QueueId, Queue>,
    flows: HashMap<FlowId, Flow>,
    tasks: HashMap<(TaskId, FlowId), Task>,
    /// task_id -> list of task_ids it depends on
    deps: HashMap<(TaskId, FlowId), Vec<TaskId>>,
    /// task_id -> list of task_ids that depend on it
    dependents: HashMap<(TaskId, FlowId), Vec<TaskId>>,
    schedules: HashMap<ScheduleId, Schedule>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                queues: HashMap::new(),
                flows: HashMap::new(),
                tasks: HashMap::new(),
                deps: HashMap::new(),
                dependents: HashMap::new(),
                schedules: HashMap::new(),
            }),
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn create_queue(&self, queue: &Queue) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.queues.contains_key(&queue.id) {
            return Err(StorageError::QueueAlreadyExists(queue.id.to_string()));
        }
        inner.queues.insert(queue.id.clone(), queue.clone());
        Ok(())
    }

    async fn get_queue(&self, id: &QueueId) -> Result<Option<Queue>, StorageError> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner.queues.get(id).cloned())
    }

    async fn list_queues(&self) -> Result<Vec<Queue>, StorageError> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner.queues.values().cloned().collect())
    }

    async fn delete_queue(&self, id: &QueueId) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.queues.remove(id);
        Ok(())
    }

    async fn create_flow(
        &self,
        flow: &Flow,
        tasks: &[Task],
        deps: &HashMap<TaskId, Vec<TaskId>>,
    ) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.flows.insert(flow.id.clone(), flow.clone());

        for task in tasks {
            inner
                .tasks
                .insert((task.id.clone(), task.flow_id.clone()), task.clone());
        }

        // Store dependency relationships
        for (task_id, dep_ids) in deps {
            inner
                .deps
                .insert((task_id.clone(), flow.id.clone()), dep_ids.clone());

            // Build reverse index
            for dep_id in dep_ids {
                inner
                    .dependents
                    .entry((dep_id.clone(), flow.id.clone()))
                    .or_default()
                    .push(task_id.clone());
            }
        }

        Ok(())
    }

    async fn get_flow(&self, id: &FlowId) -> Result<Option<Flow>, StorageError> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner.flows.get(id).cloned())
    }

    async fn list_flows(
        &self,
        queue_id: &QueueId,
        state: Option<FlowState>,
    ) -> Result<Vec<Flow>, StorageError> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner
            .flows
            .values()
            .filter(|f| f.queue_id == *queue_id && state.is_none_or(|s| f.state == s))
            .cloned()
            .collect())
    }

    async fn update_flow_state(&self, id: &FlowId, state: FlowState) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let flow = inner
            .flows
            .get_mut(id)
            .ok_or_else(|| StorageError::FlowNotFound(id.to_string()))?;
        flow.state = state;
        flow.updated_at = Utc::now();
        Ok(())
    }

    async fn increment_flow_counter(
        &self,
        id: &FlowId,
        succeeded: bool,
    ) -> Result<Flow, StorageError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let flow = inner
            .flows
            .get_mut(id)
            .ok_or_else(|| StorageError::FlowNotFound(id.to_string()))?;
        if succeeded {
            flow.tasks_succeeded += 1;
        } else {
            flow.tasks_failed += 1;
        }
        flow.updated_at = Utc::now();
        Ok(flow.clone())
    }

    async fn get_task(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<Option<Task>, StorageError> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner
            .tasks
            .get(&(task_id.clone(), flow_id.clone()))
            .cloned())
    }

    async fn get_flow_tasks(&self, flow_id: &FlowId) -> Result<Vec<Task>, StorageError> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner
            .tasks
            .values()
            .filter(|t| t.flow_id == *flow_id)
            .cloned()
            .collect())
    }

    async fn get_flow_with_tasks(
        &self,
        flow_id: &FlowId,
    ) -> Result<Option<(Flow, Vec<Task>)>, StorageError> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let flow = match inner.flows.get(flow_id) {
            Some(f) => f.clone(),
            None => return Ok(None),
        };
        let tasks: Vec<Task> = inner
            .tasks
            .values()
            .filter(|t| t.flow_id == *flow_id)
            .cloned()
            .collect();
        Ok(Some((flow, tasks)))
    }

    async fn fetch_ready_tasks(
        &self,
        queue_id: &QueueId,
        limit: usize,
    ) -> Result<Vec<Task>, StorageError> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut ready: Vec<Task> = inner
            .tasks
            .values()
            .filter(|t| t.queue_id == *queue_id && t.state == TaskState::Ready)
            .cloned()
            .collect();
        ready.sort_by_key(|t| t.created_at);
        ready.truncate(limit);
        Ok(ready)
    }

    async fn fetch_delayed_tasks_due(&self) -> Result<Vec<Task>, StorageError> {
        let now = Utc::now();
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner
            .tasks
            .values()
            .filter(|t| t.state == TaskState::Delayed && t.retry_at.is_some_and(|r| r <= now))
            .cloned()
            .collect())
    }

    async fn fetch_timed_out_tasks(&self) -> Result<Vec<Task>, StorageError> {
        let now = Utc::now();
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner
            .tasks
            .values()
            .filter(|t| {
                t.state == TaskState::Running
                    && t.started_at
                        .is_some_and(|s| (now - s).num_seconds() as u64 > t.timeout_secs)
            })
            .cloned()
            .collect())
    }

    async fn update_task_state(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        new_state: TaskState,
    ) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let key = (task_id.clone(), flow_id.clone());
        let task = inner
            .tasks
            .get_mut(&key)
            .ok_or_else(|| StorageError::TaskNotFound(task_id.to_string(), flow_id.to_string()))?;
        if !task.state.can_transition_to(new_state) {
            return Err(StorageError::InvalidStateTransition(
                task.state.to_string(),
                new_state.to_string(),
            ));
        }
        task.state = new_state;
        Ok(())
    }

    async fn mark_task_running(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let key = (task_id.clone(), flow_id.clone());
        let task = inner
            .tasks
            .get_mut(&key)
            .ok_or_else(|| StorageError::TaskNotFound(task_id.to_string(), flow_id.to_string()))?;
        if !task.state.can_transition_to(TaskState::Running) {
            return Err(StorageError::InvalidStateTransition(
                task.state.to_string(),
                TaskState::Running.to_string(),
            ));
        }
        task.state = TaskState::Running;
        task.started_at = Some(Utc::now());
        Ok(())
    }

    async fn set_task_output(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        output: serde_json::Value,
    ) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let key = (task_id.clone(), flow_id.clone());
        let task = inner
            .tasks
            .get_mut(&key)
            .ok_or_else(|| StorageError::TaskNotFound(task_id.to_string(), flow_id.to_string()))?;
        task.output = Some(output);
        Ok(())
    }

    async fn mark_task_succeeded(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        output: Option<serde_json::Value>,
    ) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let key = (task_id.clone(), flow_id.clone());
        let task = inner
            .tasks
            .get_mut(&key)
            .ok_or_else(|| StorageError::TaskNotFound(task_id.to_string(), flow_id.to_string()))?;
        if !task.state.can_transition_to(TaskState::Succeeded) {
            return Err(StorageError::InvalidStateTransition(
                task.state.to_string(),
                TaskState::Succeeded.to_string(),
            ));
        }
        task.state = TaskState::Succeeded;
        task.output = output;
        task.completed_at = Some(Utc::now());
        Ok(())
    }

    async fn mark_task_failed(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        error: &str,
    ) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let key = (task_id.clone(), flow_id.clone());
        let task = inner
            .tasks
            .get_mut(&key)
            .ok_or_else(|| StorageError::TaskNotFound(task_id.to_string(), flow_id.to_string()))?;
        if !task.state.can_transition_to(TaskState::Failed) {
            return Err(StorageError::InvalidStateTransition(
                task.state.to_string(),
                TaskState::Failed.to_string(),
            ));
        }
        task.state = TaskState::Failed;
        task.error = Some(error.to_string());
        task.completed_at = Some(Utc::now());
        Ok(())
    }

    async fn mark_task_delayed(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        retry_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let key = (task_id.clone(), flow_id.clone());
        let task = inner
            .tasks
            .get_mut(&key)
            .ok_or_else(|| StorageError::TaskNotFound(task_id.to_string(), flow_id.to_string()))?;
        if !task.state.can_transition_to(TaskState::Delayed) {
            return Err(StorageError::InvalidStateTransition(
                task.state.to_string(),
                TaskState::Delayed.to_string(),
            ));
        }
        task.state = TaskState::Delayed;
        task.retry_at = Some(retry_at);
        task.retries_remaining = task.retries_remaining.saturating_sub(1);
        task.started_at = None;
        Ok(())
    }

    async fn get_flow_dependencies(
        &self,
        flow_id: &FlowId,
    ) -> Result<HashMap<TaskId, Vec<TaskId>>, StorageError> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut result: HashMap<TaskId, Vec<TaskId>> = HashMap::new();
        for ((task_id, fid), dep_ids) in &inner.deps {
            if fid == flow_id {
                result.insert(task_id.clone(), dep_ids.clone());
            }
        }
        Ok(result)
    }

    async fn get_task_dependencies(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<Vec<TaskId>, StorageError> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner
            .deps
            .get(&(task_id.clone(), flow_id.clone()))
            .cloned()
            .unwrap_or_default())
    }

    async fn get_task_dependents(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<Vec<TaskId>, StorageError> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner
            .dependents
            .get(&(task_id.clone(), flow_id.clone()))
            .cloned()
            .unwrap_or_default())
    }

    async fn resolve_ready_tasks(&self, flow_id: &FlowId) -> Result<Vec<TaskId>, StorageError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut newly_ready = Vec::new();

        // Find all pending tasks in this flow
        let pending_tasks: Vec<TaskId> = inner
            .tasks
            .values()
            .filter(|t| t.flow_id == *flow_id && t.state == TaskState::Pending)
            .map(|t| t.id.clone())
            .collect();

        for task_id in pending_tasks {
            let dep_ids = inner
                .deps
                .get(&(task_id.clone(), flow_id.clone()))
                .cloned()
                .unwrap_or_default();

            // Check if all dependencies have succeeded
            let all_deps_succeeded = dep_ids.iter().all(|dep_id| {
                inner
                    .tasks
                    .get(&(dep_id.clone(), flow_id.clone()))
                    .is_some_and(|t| t.state == TaskState::Succeeded)
            });

            if all_deps_succeeded {
                let key = (task_id.clone(), flow_id.clone());
                if let Some(task) = inner.tasks.get_mut(&key) {
                    task.state = TaskState::Ready;
                    newly_ready.push(task_id);
                }
            }
        }

        Ok(newly_ready)
    }

    async fn inject_tasks(
        &self,
        flow_id: &FlowId,
        tasks: &[Task],
        deps: &HashMap<TaskId, Vec<TaskId>>,
    ) -> Result<Flow, StorageError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());

        // Insert tasks
        for task in tasks {
            inner
                .tasks
                .insert((task.id.clone(), task.flow_id.clone()), task.clone());
        }

        // Insert deps and reverse index
        for (task_id, dep_ids) in deps {
            inner
                .deps
                .insert((task_id.clone(), flow_id.clone()), dep_ids.clone());

            for dep_id in dep_ids {
                inner
                    .dependents
                    .entry((dep_id.clone(), flow_id.clone()))
                    .or_default()
                    .push(task_id.clone());
            }
        }

        // Update flow task_count
        let flow = inner
            .flows
            .get_mut(flow_id)
            .ok_or_else(|| StorageError::FlowNotFound(flow_id.to_string()))?;
        flow.task_count += tasks.len();
        flow.updated_at = Utc::now();

        Ok(flow.clone())
    }

    async fn get_child_flow_ids(
        &self,
        parent_flow_id: &FlowId,
    ) -> Result<Vec<FlowId>, StorageError> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner
            .flows
            .values()
            .filter(|f| f.parent_flow_id.as_ref() == Some(parent_flow_id))
            .map(|f| f.id.clone())
            .collect())
    }

    async fn delete_terminal_flows_before(
        &self,
        queue_id: &QueueId,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> Result<usize, StorageError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());

        // Find flow IDs to delete
        let to_delete: Vec<FlowId> = inner
            .flows
            .values()
            .filter(|f| f.queue_id == *queue_id && f.state.is_terminal() && f.updated_at < cutoff)
            .map(|f| f.id.clone())
            .collect();

        let count = to_delete.len();

        for flow_id in &to_delete {
            inner.flows.remove(flow_id);

            // Remove tasks belonging to this flow
            inner.tasks.retain(|(_tid, fid), _| fid != flow_id);

            // Remove deps/dependents belonging to this flow
            inner.deps.retain(|(_tid, fid), _| fid != flow_id);
            inner.dependents.retain(|(_tid, fid), _| fid != flow_id);
        }

        Ok(count)
    }

    // -- Schedules --

    async fn create_schedule(&self, schedule: &Schedule) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner
            .schedules
            .insert(schedule.id.clone(), schedule.clone());
        Ok(())
    }

    async fn get_schedule(&self, id: &ScheduleId) -> Result<Option<Schedule>, StorageError> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner.schedules.get(id).cloned())
    }

    async fn list_schedules(&self, queue_id: &QueueId) -> Result<Vec<Schedule>, StorageError> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut schedules: Vec<Schedule> = inner
            .schedules
            .values()
            .filter(|s| s.queue_id == *queue_id)
            .cloned()
            .collect();
        schedules.sort_by_key(|s| s.created_at);
        Ok(schedules)
    }

    async fn update_schedule(&self, schedule: &Schedule) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if !inner.schedules.contains_key(&schedule.id) {
            return Err(StorageError::ScheduleNotFound(schedule.id.to_string()));
        }
        inner
            .schedules
            .insert(schedule.id.clone(), schedule.clone());
        Ok(())
    }

    async fn delete_schedule(&self, id: &ScheduleId) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.schedules.remove(id);
        Ok(())
    }

    async fn fetch_due_schedules(&self) -> Result<Vec<Schedule>, StorageError> {
        let now = Utc::now();
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner
            .schedules
            .values()
            .filter(|s| s.enabled && s.next_run_at.is_some_and(|nr| nr <= now))
            .cloned()
            .collect())
    }

    async fn mark_schedule_triggered(
        &self,
        id: &ScheduleId,
        triggered_at: chrono::DateTime<chrono::Utc>,
        next_run_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let schedule = inner
            .schedules
            .get_mut(id)
            .ok_or_else(|| StorageError::ScheduleNotFound(id.to_string()))?;
        schedule.last_triggered_at = Some(triggered_at);
        schedule.next_run_at = next_run_at;
        schedule.updated_at = Utc::now();
        Ok(())
    }
}

