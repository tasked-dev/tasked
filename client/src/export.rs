//! Flow export operations.

use crate::error::TaskedError;
use crate::{TaskedClient, encode_path};
use tasked::types::FlowExport;

impl TaskedClient {
    /// Export a flow's complete state for archival, compliance, or replay.
    pub async fn export_flow(
        &self,
        flow_id: &str,
        with_artifacts: bool,
    ) -> Result<FlowExport, TaskedError> {
        let url = format!(
            "{}/api/v1/flows/{}/export?with_artifacts={with_artifacts}",
            self.base_url,
            encode_path(flow_id)
        );
        self.request_json(self.client.get(&url)).await
    }
}
