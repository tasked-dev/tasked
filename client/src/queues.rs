//! Queue CRUD operations.

use crate::error::{TaskedError, parse_timestamp};
use crate::{TaskedClient, encode_path};
use serde::{Deserialize, Serialize};
use tasked::types::{Queue, QueueConfig, QueueId};

/// Request body for creating a queue.
#[derive(Serialize)]
struct CreateQueueRequest {
    id: String,
    #[serde(default)]
    config: QueueConfig,
}

/// Response body for queue endpoints.
#[derive(Deserialize)]
pub(crate) struct QueueResponse {
    pub id: String,
    pub config: QueueConfig,
    pub created_at: String,
    pub updated_at: String,
}

impl QueueResponse {
    pub(crate) fn into_queue(self) -> Result<Queue, TaskedError> {
        Ok(Queue {
            id: QueueId::from(self.id),
            config: self.config,
            created_at: parse_timestamp(&self.created_at, "created_at")?,
            updated_at: parse_timestamp(&self.updated_at, "updated_at")?,
        })
    }
}

impl TaskedClient {
    /// Create a new queue with the given ID and configuration.
    pub async fn create_queue(&self, id: &str, config: QueueConfig) -> Result<Queue, TaskedError> {
        let url = format!("{}/api/v1/queues", self.base_url);
        let req = CreateQueueRequest {
            id: id.to_string(),
            config,
        };
        let body: QueueResponse = self.request_json(self.client.post(&url).json(&req)).await?;
        body.into_queue()
    }

    /// List all queues.
    pub async fn list_queues(&self) -> Result<Vec<Queue>, TaskedError> {
        let url = format!("{}/api/v1/queues", self.base_url);
        let body: Vec<QueueResponse> = self.request_json(self.client.get(&url)).await?;
        body.into_iter().map(|q| q.into_queue()).collect()
    }

    /// Get a queue by ID.
    pub async fn get_queue(&self, id: &str) -> Result<Queue, TaskedError> {
        let url = format!("{}/api/v1/queues/{}", self.base_url, encode_path(id));
        let body: QueueResponse = self.request_json(self.client.get(&url)).await?;
        body.into_queue()
    }

    /// Delete a queue by ID.
    pub async fn delete_queue(&self, id: &str) -> Result<(), TaskedError> {
        let url = format!("{}/api/v1/queues/{}", self.base_url, encode_path(id));
        self.request_empty(self.client.delete(&url)).await
    }
}
