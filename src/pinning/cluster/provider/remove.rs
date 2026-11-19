use super::{
    ClusterFailureCode as Code, ClusterProtocol, ClusterProvider, ClusterWriteFailure as Failure,
    canonical_cid, stream::Progress,
};
use cid::Cid;
use reqwest::Method;
use tokio_util::sync::CancellationToken;

impl ClusterProvider {
    pub async fn unpin(&self, request_id: &str) -> Result<(), Failure> {
        let handle: Cid = request_id
            .parse()
            .map_err(|_| Failure::new(Code::InvalidCid))?;
        let cid = handle.to_string();
        let protocol = self.options.protocol;
        let request = match protocol {
            ClusterProtocol::Proxy => self.target.rpc("pin/rm", &[("arg", &cid)]),
            ClusterProtocol::Rest => {
                self.target
                    .request(Method::DELETE, &format!("/pins/{cid}"), &[])
            }
        }
        .build()
        .map_err(|_| Failure::new(Code::Protocol))?;
        let progress = Progress::new();
        tokio::time::timeout(
            self.options.transfer_limits.control_timeout,
            progress.run(
                self.options.transfer_limits.idle_timeout,
                &CancellationToken::new(),
                async {
                    let response = self.target.execute(request).await?;
                    if protocol == ClusterProtocol::Rest
                        && matches!(response.status().as_u16(), 202 | 204)
                    {
                        if !self.read_ack(response, &progress).await?.is_empty() {
                            return Err(Failure::new(Code::Protocol));
                        }
                        return Ok(());
                    }
                    let reported = self.parse_pin(response, &progress).await?;
                    if canonical_cid(&reported) != canonical_cid(&handle) {
                        return Err(Failure::new(Code::CidMismatch));
                    }
                    Ok(())
                },
            ),
        )
        .await
        .map_err(|_| Failure::new(Code::ControlTimeout))?
    }
}
