use serde::Deserialize;

use super::{KuboClient, verification::bounded_control_json};
use crate::error::{AppError, AppResult};

#[derive(Deserialize)]
struct IdentityResponse {
    #[serde(rename = "ID")]
    id: String,
}

impl KuboClient {
    /// Return the stable peer identity reported by this local Kubo RPC node.
    pub async fn local_node_identity(&self) -> AppResult<String> {
        let mut url = reqwest::Url::parse(&format!("{}/api/v0/id", self.base_url()))
            .map_err(|_| AppError::kubo_rpc_detail("invalid Kubo RPC URL"))?;
        url.query_pairs_mut().append_pair("peerid-base", "b58mh");
        let response: IdentityResponse =
            bounded_control_json(self.http().post(url), "local node identity").await?;

        if response.id.is_empty()
            || response.id.len() > 256
            || !response.id.bytes().all(|byte| byte.is_ascii_alphanumeric())
        {
            return Err(AppError::kubo_rpc_detail(
                "invalid Kubo node identity response",
            ));
        }
        Ok(response.id)
    }
}
