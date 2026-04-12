//! Queue CRUD operations.

use crate::TaskedClient;
use crate::error::{ErrorResponse, TaskedError};
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
    pub(crate) fn into_queue(self) -> Queue {
        Queue {
            id: QueueId::from(self.id),
            config: self.config,
            created_at: self
                .created_at
                .parse()
                .unwrap_or_else(|_| chrono::Utc::now()),
            updated_at: self
                .updated_at
                .parse()
                .unwrap_or_else(|_| chrono::Utc::now()),
        }
    }
}

impl TaskedClient {
    /// Create a new queue with the given ID and configuration.
    pub async fn create_queue(&self, id: &str, config: QueueConfig) -> Result<Queue, TaskedError> {
        let url = format!("{}/api/v1/queues", self.base_url);
        let resp = self
            .client
            .post(&url)
            .json(&CreateQueueRequest {
                id: id.to_string(),
                config,
            })
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(self.parse_error(resp).await);
        }

        let body: QueueResponse = resp.json().await?;
        Ok(body.into_queue())
    }

    /// List all queues.
    pub async fn list_queues(&self) -> Result<Vec<Queue>, TaskedError> {
        let url = format!("{}/api/v1/queues", self.base_url);
        let resp = self.client.get(&url).send().await?;

        if !resp.status().is_success() {
            return Err(self.parse_error(resp).await);
        }

        let body: Vec<QueueResponse> = resp.json().await?;
        Ok(body.into_iter().map(|q| q.into_queue()).collect())
    }

    /// Get a queue by ID.
    pub async fn get_queue(&self, id: &str) -> Result<Queue, TaskedError> {
        let url = format!("{}/api/v1/queues/{id}", self.base_url);
        let resp = self.client.get(&url).send().await?;

        if !resp.status().is_success() {
            return Err(self.parse_error(resp).await);
        }

        let body: QueueResponse = resp.json().await?;
        Ok(body.into_queue())
    }

    /// Delete a queue by ID.
    pub async fn delete_queue(&self, id: &str) -> Result<(), TaskedError> {
        let url = format!("{}/api/v1/queues/{id}", self.base_url);
        let resp = self.client.delete(&url).send().await?;

        if !resp.status().is_success() {
            return Err(self.parse_error(resp).await);
        }

        Ok(())
    }

    /// Parse an error response from the server.
    pub(crate) async fn parse_error(&self, resp: reqwest::Response) -> TaskedError {
        let status = resp.status().as_u16();
        match resp.json::<ErrorResponse>().await {
            Ok(body) => TaskedError::Api {
                status,
                error: body.error,
                message: body.message,
            },
            Err(_) => TaskedError::Api {
                status,
                error: "unknown".to_string(),
                message: format!("Server returned status {status}"),
            },
        }
    }
}
