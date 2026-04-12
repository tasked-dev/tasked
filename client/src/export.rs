//! Flow export operations.

use crate::TaskedClient;
use crate::error::TaskedError;
use tasked::types::FlowExport;

impl TaskedClient {
    /// Export a flow's complete state for archival, compliance, or replay.
    pub async fn export_flow(
        &self,
        flow_id: &str,
        with_artifacts: bool,
    ) -> Result<FlowExport, TaskedError> {
        let url = format!(
            "{}/api/v1/flows/{flow_id}/export?with_artifacts={with_artifacts}",
            self.base_url
        );
        let resp = self.client.get(&url).send().await?;

        if !resp.status().is_success() {
            return Err(self.parse_error(resp).await);
        }

        Ok(resp.json().await?)
    }
}
