use crate::{
    error::{AppError, AppResult},
    kubo::LocalResidencyVerificationReceipt,
    residency::PhysicalVerification,
};

#[derive(Clone, Debug)]
pub(super) struct HotPublicationReceipt {
    verification: PhysicalVerification,
}

impl HotPublicationReceipt {
    pub(super) fn validate(
        receipt: LocalResidencyVerificationReceipt,
        expected_cid: &str,
    ) -> AppResult<Self> {
        if receipt.node_identity.is_empty() {
            return Err(AppError::Internal(
                "hot publication receipt has no node identity".to_owned(),
            ));
        }
        let expected = cid::Cid::try_from(expected_cid).map_err(|_| {
            AppError::Internal("hot publication expected CID is invalid".to_owned())
        })?;
        let observed = cid::Cid::try_from(receipt.cid.as_str())
            .map_err(|_| AppError::Internal("hot publication receipt CID is invalid".to_owned()))?;
        if observed != expected {
            return Err(AppError::Internal(
                "hot publication receipt CID does not match the object".to_owned(),
            ));
        }

        let serialized = serde_json::to_string(&receipt).map_err(|_| {
            AppError::Internal("hot publication receipt serialization failed".to_owned())
        })?;
        Ok(Self {
            verification: PhysicalVerification::verified(receipt.node_identity, serialized),
        })
    }

    pub(super) fn verification(&self) -> &PhysicalVerification {
        &self.verification
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    const OTHER_CID: &str = "bafkreigh2akiscaildc6ii5zji4bq7kly5k3s7svv6q2wx2nn5rtj5xuu4";

    #[test]
    fn rejects_a_receipt_for_another_cid() {
        let error = HotPublicationReceipt::validate(
            LocalResidencyVerificationReceipt {
                node_identity: "hot-node".to_owned(),
                cid: OTHER_CID.to_owned(),
            },
            CID,
        )
        .unwrap_err();

        assert!(matches!(error, AppError::Internal(_)));
    }
}
