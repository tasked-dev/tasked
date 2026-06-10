//! Artifact storage for sharing files between tasks in a flow.

use async_trait::async_trait;
use std::path::{Component, Path, PathBuf};

use crate::types::FlowId;

/// Errors that can occur during artifact operations.
#[derive(Debug, thiserror::Error)]
pub enum ArtifactError {
    #[error("invalid artifact name: {0}")]
    InvalidName(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}

/// Validate that a path component (artifact name or flow ID) does not escape its parent directory.
///
/// Rejects:
/// - Empty strings
/// - Absolute paths (leading `/` or `\`)
/// - `..` path components
/// - After joining to `base`, verifies the canonical path stays within `base`
fn validate_name(name: &str, base: &Path) -> Result<(), ArtifactError> {
    if name.is_empty() {
        return Err(ArtifactError::InvalidName(
            "name must not be empty".to_string(),
        ));
    }

    // Reject absolute paths
    if name.starts_with('/') || name.starts_with('\\') {
        return Err(ArtifactError::InvalidName(format!(
            "absolute paths are not allowed: '{name}'"
        )));
    }

    // Reject any `..` components
    let path = Path::new(name);
    for component in path.components() {
        if matches!(component, Component::ParentDir) {
            return Err(ArtifactError::InvalidName(format!(
                "path traversal is not allowed: '{name}'"
            )));
        }
    }

    // Final check: the joined path must stay within the base directory.
    // This is a lexical check (no filesystem access): `..` and absolute
    // paths were already rejected above, so a name can only stay inside
    // `base` or fail this prefix test.
    let joined = base.join(name);
    if !joined.starts_with(base) {
        return Err(ArtifactError::InvalidName(format!(
            "path escapes base directory: '{name}'"
        )));
    }

    Ok(())
}

/// Artifact storage backend trait.
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    /// Upload an artifact.
    async fn upload(&self, flow_id: &FlowId, name: &str, data: &[u8]) -> Result<(), ArtifactError>;
    /// Download an artifact by name.
    async fn download(&self, flow_id: &FlowId, name: &str) -> Result<Vec<u8>, ArtifactError>;
    /// List artifact names for a flow.
    async fn list(&self, flow_id: &FlowId) -> Result<Vec<String>, ArtifactError>;
    /// Delete all artifacts for a flow.
    async fn cleanup(&self, flow_id: &FlowId) -> Result<(), ArtifactError>;
    /// Get the local directory path for a flow's artifacts (if available).
    fn local_dir(&self, flow_id: &FlowId) -> Option<PathBuf>;
}

/// Local filesystem artifact store.
///
/// Stores artifacts as files under `{base_dir}/{flow_id}/{name}`.
/// Each flow gets its own subdirectory that is cleaned up when the flow completes.
pub struct LocalArtifactStore {
    base_dir: PathBuf,
}

impl LocalArtifactStore {
    /// Create a new artifact store rooted at the given directory.
    pub fn new(base_dir: impl AsRef<Path>) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
        }
    }

    fn flow_dir(&self, flow_id: &FlowId) -> Result<PathBuf, ArtifactError> {
        validate_name(flow_id.as_str(), &self.base_dir)?;
        Ok(self.base_dir.join(flow_id.as_str()))
    }
}

#[async_trait]
impl ArtifactStore for LocalArtifactStore {
    async fn upload(&self, flow_id: &FlowId, name: &str, data: &[u8]) -> Result<(), ArtifactError> {
        let dir = self.flow_dir(flow_id)?;
        validate_name(name, &dir)?;
        let path = dir.join(name);
        // Create parent dirs if name has slashes (e.g., "build/output.tar.gz");
        // this also creates the flow directory itself.
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&path, data).await?;
        Ok(())
    }

    async fn download(&self, flow_id: &FlowId, name: &str) -> Result<Vec<u8>, ArtifactError> {
        let dir = self.flow_dir(flow_id)?;
        validate_name(name, &dir)?;
        let path = dir.join(name);
        Ok(tokio::fs::read(&path).await?)
    }

    async fn list(&self, flow_id: &FlowId) -> Result<Vec<String>, ArtifactError> {
        let dir = self.flow_dir(flow_id)?;
        if !dir.exists() {
            return Ok(vec![]);
        }
        // Recursive directory walk — run it off the async runtime threads.
        let names = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<String>> {
            let mut names = Vec::new();
            collect_files(&dir, &dir, &mut names)?;
            names.sort();
            Ok(names)
        })
        .await
        .map_err(|e| ArtifactError::Other(format!("list task panicked: {e}")))??;
        Ok(names)
    }

    async fn cleanup(&self, flow_id: &FlowId) -> Result<(), ArtifactError> {
        let dir = self.flow_dir(flow_id)?;
        if dir.exists() {
            tokio::fs::remove_dir_all(&dir).await?;
        }
        Ok(())
    }

    fn local_dir(&self, flow_id: &FlowId) -> Option<PathBuf> {
        self.flow_dir(flow_id).ok()
    }
}

/// Recursively collect file paths relative to base_dir.
fn collect_files(dir: &Path, base: &Path, names: &mut Vec<String>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, base, names)?;
        } else if let Ok(relative) = path.strip_prefix(base) {
            names.push(relative.to_string_lossy().to_string());
        }
    }
    Ok(())
}
