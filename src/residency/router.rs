use crate::error::{AppError, AppResult, TierError};
use crate::kubo::{KuboClient, LocalResidencyVerificationReceipt};

use super::model::{KuboTier, ResolvedVersionResidency, VerificationState};

/// Tier clients are unrelated to remote pinning providers. Missing cold is not
/// permission to read from hot, even when the same CID remains there.
pub struct TierClients<'a> {
    pub hot: &'a KuboClient,
    pub cold: Option<&'a KuboClient>,
}

impl TierClients<'_> {
    pub fn client_for_tier(&self, tier: KuboTier) -> AppResult<&KuboClient> {
        match tier {
            KuboTier::Hot => Ok(self.hot),
            KuboTier::Cold => self
                .cold
                .ok_or(AppError::Tier(TierError::ColdNotConfigured)),
        }
    }

    /// Consume a residency snapshot selected by immutable version row, never by
    /// the current key or a shared CID. Receipts bind a node, not merely a URL.
    pub async fn resolve_read_source(
        &self,
        residency: &ResolvedVersionResidency,
    ) -> AppResult<KuboClient> {
        let client = self.client_for_tier(residency.primary.tier)?;
        if residency.physical.location != residency.primary
            || residency.identity.cid != residency.primary.cid
        {
            return Err(AppError::Tier(TierError::CidMismatch));
        }
        if residency.primary.tier == KuboTier::Cold {
            if residency.physical.verification_state != VerificationState::Verified {
                return Err(AppError::Tier(TierError::LocalCopyIncomplete));
            }
            validate_cold_receipt(
                &residency.identity.cid,
                residency.physical.node_identity.as_deref(),
                residency.physical.verification_receipt.as_deref(),
            )?;
        }
        // Legacy/pending hot continues to work without a readiness dependency.
        // Once verified, do not reuse evidence for a replacement Kubo node.
        if residency.physical.verification_state == VerificationState::Verified {
            let node = client
                .local_node_identity()
                .await
                .map_err(|_| AppError::Tier(TierError::TierUnavailable))?;
            if Some(node.as_str()) != residency.physical.node_identity.as_deref() {
                return Err(AppError::Tier(TierError::NodeIdentityMismatch));
            }
        }
        Ok(match residency.primary.tier {
            KuboTier::Cold => client.clone().with_local_reads_only(),
            KuboTier::Hot => client.clone(),
        })
    }
}

/// Pure persisted-metadata validation shared by reads and class reporting.
/// A nonempty receipt is not evidence unless it binds the exact CID and node.
/// This does not probe Kubo; runtime node validation remains a read concern.
pub(crate) fn validate_cold_receipt(
    cid: &str,
    node_identity: Option<&str>,
    receipt: Option<&str>,
) -> AppResult<()> {
    let receipt: LocalResidencyVerificationReceipt = receipt
        .and_then(|value| serde_json::from_str(value).ok())
        .ok_or(AppError::Tier(TierError::LocalCopyIncomplete))?;
    let cid = cid::Cid::try_from(cid).map_err(|_| AppError::Tier(TierError::CidMismatch))?;
    if cid::Cid::try_from(receipt.cid.as_str()).ok() != Some(cid) {
        return Err(AppError::Tier(TierError::CidMismatch));
    }
    if Some(receipt.node_identity.as_str()) != node_identity {
        return Err(AppError::Tier(TierError::NodeIdentityMismatch));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::residency::model::*;

    fn residency(tier: KuboTier) -> ResolvedVersionResidency {
        let cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
        let location = ResidencyLocation::new(tier, cid);
        ResolvedVersionResidency {
            identity: VersionResidencyIdentity::new("version", "object", cid),
            primary: location.clone(),
            storage_class: if tier == KuboTier::Hot {
                StorageClass::Standard
            } else {
                StorageClass::StandardIa
            },
            revision: 1,
            physical: PhysicalResidencySnapshot {
                location,
                verification_state: VerificationState::Pending,
                node_identity: None,
                verification_receipt: None,
                verified_at: None,
            },
        }
    }

    #[tokio::test]
    async fn legacy_hot_needs_no_probe_and_cold_never_falls_back() {
        let hot = KuboClient::new("http://127.0.0.1:1".to_owned());
        let clients = TierClients {
            hot: &hot,
            cold: None,
        };
        assert!(
            clients
                .resolve_read_source(&residency(KuboTier::Hot))
                .await
                .is_ok()
        );
        assert!(matches!(
            clients
                .resolve_read_source(&residency(KuboTier::Cold))
                .await,
            Err(AppError::Tier(TierError::ColdNotConfigured))
        ));
    }

    #[tokio::test]
    async fn cold_requires_verified_receipt_and_bound_node() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        let hot = KuboClient::new("http://127.0.0.1:1".to_owned());
        let cold = KuboClient::new(server.uri());
        let clients = TierClients {
            hot: &hot,
            cold: Some(&cold),
        };
        let mut value = residency(KuboTier::Cold);
        assert!(matches!(
            clients.resolve_read_source(&value).await,
            Err(AppError::Tier(TierError::LocalCopyIncomplete))
        ));
        value.physical.verification_state = VerificationState::Verified;
        value.physical.node_identity = Some("ColdNode".into());
        value.physical.verification_receipt = Some(
            serde_json::to_string(&LocalResidencyVerificationReceipt {
                node_identity: "ColdNode".into(),
                cid: value.identity.cid.clone(),
            })
            .unwrap(),
        );
        Mock::given(method("POST"))
            .and(path("/api/v0/id"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"ID":"ReplacementNode"})),
            )
            .mount(&server)
            .await;
        assert!(matches!(
            clients.resolve_read_source(&value).await,
            Err(AppError::Tier(TierError::NodeIdentityMismatch))
        ));
    }
}
