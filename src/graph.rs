use crate::types::{TaskDef, TaskId};
use petgraph::algo::toposort;
use petgraph::graph::{DiGraph, NodeIndex};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GraphError {
    #[error("cycle detected in task dependencies")]
    CycleDetected,
    #[error("unknown task '{0}' referenced in depends_on")]
    UnknownDependency(String),
    #[error("duplicate task id '{0}'")]
    DuplicateTaskId(String),
    #[error("flow must contain at least one task")]
    Empty,
    #[error("spawn task '{task_id}' does not declare output '{output_name}'")]
    SpawnOutputNotFound {
        task_id: String,
        output_name: String,
    },
}

/// A validated directed acyclic graph of task dependencies.
#[derive(Debug, Clone)]
pub struct TaskGraph {
    graph: DiGraph<TaskId, ()>,
    node_map: HashMap<TaskId, NodeIndex>,
}

impl TaskGraph {
    /// Build a task graph from task IDs and their dependencies.
    ///
    /// Each entry in `deps` maps a task ID to the IDs it depends on (must complete before it).
    /// Returns an error if the graph contains cycles, unknown dependencies, or duplicate IDs.
    pub fn build(
        task_ids: &[TaskId],
        deps: &HashMap<TaskId, Vec<TaskId>>,
    ) -> Result<Self, GraphError> {
        if task_ids.is_empty() {
            return Err(GraphError::Empty);
        }

        let mut graph = DiGraph::new();
        let mut node_map = HashMap::new();

        // Check for duplicate task IDs
        for id in task_ids {
            if node_map.contains_key(id) {
                return Err(GraphError::DuplicateTaskId(id.to_string()));
            }
            let idx = graph.add_node(id.clone());
            node_map.insert(id.clone(), idx);
        }

        // Add edges: dependency → dependent
        // Edge direction: if B depends on A, edge goes A → B
        // This means topological order gives us execution order.
        for (task_id, dep_ids) in deps {
            let dependent_idx = node_map
                .get(task_id)
                .ok_or_else(|| GraphError::UnknownDependency(task_id.to_string()))?;

            for dep_id in dep_ids {
                let dependency_idx = node_map
                    .get(dep_id)
                    .ok_or_else(|| GraphError::UnknownDependency(dep_id.to_string()))?;

                // Edge from dependency to dependent (A → B means "A must complete before B")
                graph.add_edge(*dependency_idx, *dependent_idx, ());
            }
        }

        // Verify acyclic
        if toposort(&graph, None).is_err() {
            return Err(GraphError::CycleDetected);
        }

        Ok(Self { graph, node_map })
    }

    /// Build a task graph, allowing deferred spawn dependencies.
    /// Dependencies containing "/" are validated against spawn_output declarations
    /// rather than requiring the target task to exist in the current set.
    pub fn build_with_spawn_deps(task_defs: &[TaskDef]) -> Result<Self, GraphError> {
        if task_defs.is_empty() {
            return Err(GraphError::Empty);
        }

        let task_ids: Vec<TaskId> = task_defs.iter().map(|d| d.id.clone()).collect();

        // Build a lookup of task_id -> spawn_output declarations
        let spawn_outputs: HashMap<&str, &[String]> = task_defs
            .iter()
            .filter(|d| !d.spawn_output.is_empty())
            .map(|d| (d.id.as_str(), d.spawn_output.as_slice()))
            .collect();

        let mut graph = DiGraph::new();
        let mut node_map = HashMap::new();

        // Check for duplicate task IDs
        for id in &task_ids {
            if node_map.contains_key(id) {
                return Err(GraphError::DuplicateTaskId(id.to_string()));
            }
            let idx = graph.add_node(id.clone());
            node_map.insert(id.clone(), idx);
        }

        // Build deps, skipping deferred spawn references
        let deps: HashMap<TaskId, Vec<TaskId>> = task_defs
            .iter()
            .filter(|t| !t.depends_on.is_empty())
            .map(|t| (t.id.clone(), t.depends_on.clone()))
            .collect();

        for (task_id, dep_ids) in &deps {
            let dependent_idx = node_map
                .get(task_id)
                .ok_or_else(|| GraphError::UnknownDependency(task_id.to_string()))?;

            for dep_id in dep_ids {
                if dep_id.as_str().contains('/') {
                    // Deferred spawn dependency: validate against spawn_output
                    let parts: Vec<&str> = dep_id.as_str().splitn(2, '/').collect();
                    let spawn_task_id = parts[0];
                    let output_name = parts[1];

                    // Verify the spawn task exists
                    if !node_map.contains_key(&TaskId::from(spawn_task_id)) {
                        return Err(GraphError::UnknownDependency(dep_id.to_string()));
                    }

                    // Verify the spawn task declares this output
                    match spawn_outputs.get(spawn_task_id) {
                        Some(outputs) if outputs.iter().any(|o| o == output_name) => {
                            // Valid deferred dep -- don't add edge (target doesn't exist yet)
                        }
                        _ => {
                            return Err(GraphError::SpawnOutputNotFound {
                                task_id: spawn_task_id.to_string(),
                                output_name: output_name.to_string(),
                            });
                        }
                    }
                } else {
                    // Normal dependency
                    let dependency_idx = node_map
                        .get(dep_id)
                        .ok_or_else(|| GraphError::UnknownDependency(dep_id.to_string()))?;
                    graph.add_edge(*dependency_idx, *dependent_idx, ());
                }
            }
        }

        // Verify acyclic
        if toposort(&graph, None).is_err() {
            return Err(GraphError::CycleDetected);
        }

        Ok(Self { graph, node_map })
    }

    /// Returns task IDs with no dependencies (roots / entry points).
    pub fn roots(&self) -> Vec<TaskId> {
        self.node_map
            .iter()
            .filter(|(_, idx)| {
                self.graph
                    .neighbors_directed(**idx, petgraph::Direction::Incoming)
                    .next()
                    .is_none()
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Returns total number of tasks.
    pub fn task_count(&self) -> usize {
        self.graph.node_count()
    }
}
