use super::{
    ClusterFailureCode as Code, ClusterProtocol, ClusterProvider, ClusterRemoteRef,
    ClusterResourceStatus, ClusterWriteEffect, ClusterWriteFailure as Failure,
    ClusterWriteObservation, ClusterWriteReceipt, stream::Progress,
};
use reqwest::Method;
use tokio_util::sync::CancellationToken;

impl ClusterProvider {
    /// Restricted primitive ONLY: parent ledger must prove ownership, zero
    /// references/in-flight writes and the current fence before invoking it.
    /// This does not infer ownership or run cleanup in submit/observe/error paths.
    pub async fn unpin_authorized_ref(
        &self,
        reference: &ClusterRemoteRef,
        cancel: &CancellationToken,
    ) -> ClusterWriteObservation {
        let mut out = ClusterWriteObservation {
            result: Err(Failure::new(Code::InvalidReference)),
            effect: ClusterWriteEffect::NotSubmitted,
            resources: vec![],
            replicas: None,
            http_status: None,
        };
        out.result = self.remove(reference, &mut out, cancel).await;
        out
    }
    async fn remove(
        &self,
        reference: &ClusterRemoteRef,
        out: &mut ClusterWriteObservation,
        cancel: &CancellationToken,
    ) -> Result<ClusterWriteReceipt, Failure> {
        let resource = self.resource_key(&reference.resource.cid)?;
        if resource != reference.resource
            || reference.protocol != self.options.protocol
            || reference.strategy != self.options.strategy
        {
            return Err(Failure::new(Code::InvalidReference));
        }
        if cancel.is_cancelled() {
            return Err(Failure::new(Code::Cancelled));
        }
        let cid = &resource.cid;
        let request = match reference.protocol {
            ClusterProtocol::Proxy => self.target.rpc("pin/rm", &[("arg", cid)]),
            ClusterProtocol::Rest => {
                self.target
                    .request(Method::DELETE, &format!("/pins/{cid}"), &[])
            }
        }
        .build()
        .map_err(|_| Failure::new(Code::Protocol))?;
        self.record(out, cid, ClusterResourceStatus::DispatchIntent)?;
        out.effect = ClusterWriteEffect::Unknown;
        let progress = Progress::new();
        let result = tokio::time::timeout(
            self.options.transfer_limits.control_timeout,
            progress.run(self.options.transfer_limits.idle_timeout, cancel, async {
                let response = self.target.execute(request).await?;
                out.http_status = Some(response.status().as_u16());
                self.parse_pin(response, out, &progress).await
            }),
        )
        .await
        .map_err(|_| Failure::new(Code::ControlTimeout))??;
        for r in &mut out.resources {
            if r.status == ClusterResourceStatus::Reported {
                r.status = ClusterResourceStatus::UnpinAccepted;
            }
        }
        if result != *cid {
            return Err(Failure::new(Code::CidMismatch));
        }
        out.effect = ClusterWriteEffect::Observed;
        Ok(ClusterWriteReceipt {
            resource,
            accepted_at: chrono::Utc::now(),
        })
    }
}
