//! Observation-only resources and uncertain calls remain retirement debt even
//! when their current ledger or target has disappeared.
use super::{
    spool::{fact, one},
    *,
};
use crate::store::pinning::management::native::value;
use sea_orm::ConnectionTrait;

pub(super) async fn reduce(
    tx: &impl ConnectionTrait,
    key: &NativeKey,
    row: &NativeRow,
    replica: bool,
    run: &super::spool::Run<'_>,
) -> Result<()> {
    run.check()?;
    let original: crate::pinning::identity::ProviderRouteSnapshot =
        decode(value(row, "route").ok_or(ManagementError::InvalidEvidence)?)?;
    let route = RouteIdentity::from_original(original.clone());
    let expected = crate::pinning::provider::canonical_resource_cid(
        value(row, "expected_cid").ok_or(ManagementError::InvalidEvidence)?,
    )
    .map_err(|_| ManagementError::InvalidEvidence)?;
    let epoch: i64 = value(row, "remote_epoch")
        .ok_or(ManagementError::InvalidEvidence)?
        .parse()
        .map_err(|_| ManagementError::InvalidEvidence)?;
    if epoch < 0 {
        return Err(ManagementError::InvalidEvidence);
    }
    let inflight = value(row, "outcome") == Some("in_flight");
    let mut unresolved = inflight
        || value(row, "needs_attention").is_some_and(|v| matches!(v, "1" | "true"))
        || value(row, "effect") == Some("unknown");
    let resources = if replica {
        if !matches!(
            value(row, "phase"),
            Some("submit" | "poll" | "reconcile" | "unpin")
        ) {
            return Err(ManagementError::InvalidEvidence);
        }
        let resources: Vec<crate::pinning::provider::ObservedResource> =
            decode(value(row, "resources").ok_or(ManagementError::InvalidEvidence)?)?;
        let outcome = value(row, "outcome");
        let reported = matches!(
            outcome,
            Some("ReportedQueued" | "ReportedPinning" | "ReportedPinned")
        );
        let delete_ack = outcome == Some("DeleteAcknowledged")
            && value(row, "phase") == Some("unpin")
            && resources.is_empty();
        let projectable = match value(row, "projectable") {
            Some("1" | "true") => true,
            Some("0" | "false") => false,
            _ => return Err(ManagementError::InvalidEvidence),
        };
        // Scoped GET/Submit reports are deliberately nonprojectable. Recognizing
        // an already-accounted physical resource does not turn them into live
        // replica proof, an absence claim, or permission to release its quota.
        unresolved |= (!projectable && !reported && !delete_ack)
            || matches!(outcome, Some("Unknown" | "ReportedFailed"))
            || value(row, "safe_error").is_some()
            || resources.is_empty() && !delete_ack
            || resources.iter().any(|resource| {
                resource.status == crate::pinning::provider::ObservedResourceStatus::PinError
            });
        resources
            .into_iter()
            .map(|resource| {
                crate::store::pinning::ledger::submission::resource_evidence_for_route(
                    &original, resource,
                )
            })
            .collect::<Vec<_>>()
    } else {
        let resources: Vec<crate::store::pinning::ledger::submission::ResourceEvidence> =
            decode(value(row, "resources").ok_or(ManagementError::InvalidEvidence)?)?;
        unresolved |= resources.is_empty() && value(row, "effect") == Some("observed");
        resources
    };
    for (index, evidence) in resources.into_iter().enumerate() {
        run.check()?;
        let cid = crate::pinning::provider::canonical_resource_cid(&evidence.resource.cid)
            .map_err(|_| ManagementError::InvalidEvidence)?;
        unresolved |= replica && cid != expected;
        let canonical = evidence.key.ok_or(ManagementError::InvalidEvidence)?;
        if canonical.backend != route.backend
            || canonical.scope != route.scope
            || canonical.resource_type != original.resource_type()
            || canonical.resource_id != cid
            || evidence.resource.resource_type != original.resource_type()
        {
            return Err(ManagementError::InvalidEvidence);
        }
        let remote_key = route.remote_key(&cid);
        if evidence.resource.request_id.is_empty() {
            return Err(ManagementError::InvalidEvidence);
        }
        let identity = encode(&(
            &remote_key,
            ResourceIdentity::Known(evidence.resource.request_id.clone()),
        ))?;
        if one(
            tx,
            "SELECT identity FROM quotes WHERE identity=$1",
            vec![identity.into()],
        )
        .await?
        .is_some()
        {
            continue;
        }
        let mut remote = unknown_remote(route.clone(), cid, epoch, inflight);
        remote.resource_id = Some(evidence.resource.request_id);
        remote.ownership = match evidence.resource.ownership {
            crate::pinning::identity::Ownership::ApplicationCreated
                if route.resource_type != ResourceType::RpcPin =>
            {
                Ownership::ApplicationCreated
            }
            crate::pinning::identity::Ownership::ExternalExisting => Ownership::ExternalExisting,
            _ => Ownership::Unknown,
        };
        fact(tx, "history", &encode(&(key, index))?, &remote).await?;
        super::reducer::quote(tx, &remote).await?;
    }
    if unresolved {
        let remote = unknown_remote(route, expected, epoch, inflight);
        fact(tx, "history", &encode(&(key, "unresolved"))?, &remote).await?;
        super::reducer::quote(tx, &remote).await?;
    }
    // Projections remain reconstructible from originals, including observations
    // already represented by a known resource quote.
    fact(tx, "observation", &encode(key)?, key).await?;
    Ok(())
}

fn unknown_remote(route: RouteIdentity, cid: String, epoch: i64, inflight: bool) -> RemoteSnapshot {
    RemoteSnapshot {
        key: route.remote_key(&cid),
        route,
        resource_id: None,
        epoch,
        effect: if inflight {
            SideEffect::InFlight
        } else {
            SideEffect::Unknown
        },
        ownership: Ownership::Unknown,
        application_references: None,
        inflight_operations: inflight.then_some(1),
        retained: false,
        cleanup_pending: false,
        accounted_bytes: None,
        confirmations: vec![],
        confirmed_replicas: None,
        observation_current: None,
        replica_started_at: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pinning::{identity, provider};

    #[tokio::test]
    async fn nonprojectable_known_reports_do_not_grant_projection_or_erase_unknown_debt() {
        let original = identity::ProviderRouteSnapshot {
            provider_id: "provider".into(),
            backend: "pinata".into(),
            scope: "account".into(),
            storage_domain: "storage".into(),
            credential_revision: 1,
            endpoint_revision: 1,
            secret_ref: None,
            api_profile: "pinata-psa".into(),
            strategy: "cid".into(),
            cleanup: identity::CleanupMode::Managed,
            confirmation: None,
        };
        let route = RouteIdentity::from_original(original.clone());
        let cid =
            provider::canonical_resource_cid("QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE")
                .unwrap();
        let mut workspace = Workspace::create().unwrap();
        let (path, file) = workspace.reserve().await.unwrap();
        let spool = InventorySpool::allocate(&path, file, None).await.unwrap();
        let mut remote = unknown_remote(route.clone(), cid.clone(), 1, false);
        remote.resource_id = Some("resource".into());
        remote.effect = SideEffect::Present;
        remote.ownership = Ownership::ApplicationCreated;
        remote.application_references = Some(0);
        remote.inflight_operations = Some(0);
        remote.accounted_bytes = Some(10);
        super::super::reducer::quote(&spool.db, &remote)
            .await
            .unwrap();
        let resources = vec![provider::ObservedResource {
            resource_type: original.resource_type(),
            cid: cid.clone(),
            request_id: "resource".into(),
            status: provider::ObservedResourceStatus::Reported,
            ownership: identity::Ownership::Unknown,
            replica_evidence: None,
        }];
        let mut row = NativeRow::from([
            ("route".into(), Some(encode(&original).unwrap())),
            ("expected_cid".into(), Some(cid)),
            ("remote_epoch".into(), Some("1".into())),
            ("projectable".into(), Some("0".into())),
            ("phase".into(), Some("poll".into())),
            ("outcome".into(), Some("ReportedPinned".into())),
            ("resources".into(), Some(encode(&resources).unwrap())),
            ("safe_error".into(), None),
        ]);
        let key = NativeKey {
            table: "pin_replica_observations".into(),
            values: vec!["1".into()],
        };
        let limits = RunLimits::default();
        reduce(
            &spool.db,
            &key,
            &row,
            true,
            &super::super::spool::Run::new(&limits),
        )
        .await
        .unwrap();
        assert!(
            one(
                &spool.db,
                "SELECT identity FROM facts WHERE kind='history'",
                vec![]
            )
            .await
            .unwrap()
            .is_none()
        );
        row.insert("phase".into(), Some("unpin".into()));
        row.insert("outcome".into(), Some("DeleteAcknowledged".into()));
        row.insert("resources".into(), Some("[]".into()));
        let delete_key = NativeKey {
            table: key.table.clone(),
            values: vec!["2".into()],
        };
        reduce(
            &spool.db,
            &delete_key,
            &row,
            true,
            &super::super::spool::Run::new(&limits),
        )
        .await
        .unwrap();
        let quote = one(&spool.db, "SELECT bytes,unknown FROM quotes", vec![])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            super::super::spool::get::<String>(&quote, "bytes").unwrap(),
            "10"
        );
        assert_eq!(
            super::super::spool::get::<i64>(&quote, "unknown").unwrap(),
            0
        );
        row.insert("outcome".into(), Some("Unknown".into()));
        let unknown_key = NativeKey {
            table: key.table,
            values: vec!["3".into()],
        };
        reduce(
            &spool.db,
            &unknown_key,
            &row,
            true,
            &super::super::spool::Run::new(&limits),
        )
        .await
        .unwrap();
        assert!(
            one(
                &spool.db,
                "SELECT identity FROM quotes WHERE unknown=1",
                vec![]
            )
            .await
            .unwrap()
            .is_some()
        );
        assert!(
            one(
                &spool.db,
                "SELECT identity FROM facts WHERE kind='history'",
                vec![]
            )
            .await
            .unwrap()
            .is_some()
        );
        spool.close().await.unwrap();
        workspace.cleanup().unwrap();
    }
}
