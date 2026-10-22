//! Current responsibility is a new, bounded source scan, never frozen receipt data.
use super::spool::{get, one};
use super::*;
use crate::store::pinning::management::transaction::{ManagementTransaction, TransactionFailure};
use crate::store::pinning::management::{native::value, records::ActionState};
use sea_orm::DatabaseConnection;
use std::{fs::File, path::Path};

pub struct CurrentRetirementScan {
    spool: InventorySpool,
    configuration: ConfigurationBinding,
    completed: bool,
    detail_stage: usize,
    detail_after: String,
    registered: bool,
    attention: bool,
    queued: bool,
}

impl CurrentRetirementScan {
    pub async fn close(&self) -> Result<()> {
        self.spool.close().await
    }
    /// The caller first verifies the committed action and supplies its exact
    /// canonical route. The protected file/parent contract is InventorySpool's.
    /// No provider calls, authorization renewal or receipt mutation occur here.
    pub async fn start(
        db: &DatabaseConnection,
        route: RouteIdentity,
        configuration: ConfigurationBinding,
        path: &Path,
        reservation: File,
        limits: &RunLimits,
    ) -> Result<Self> {
        super::super::validate::validate_route_identity(&route)
            .map_err(|_| ManagementError::InvalidEvidence)?;
        let run = super::spool::Run::new(limits);
        run.check()?;
        let mut tx = ManagementTransaction::begin(db, run.remaining(), limits.cancellation.clone())
            .await
            .map_err(transaction_error)?;
        let checkpoint = tx
            .run(move |tx, _| {
                Box::pin(async move { SourceCheckpoint::start_status(tx, route).await })
            })
            .await
            .map_err(transaction_error)?;
        tx.rollback().await.map_err(transaction_error)?;
        run.check()?;
        let spool = InventorySpool::create(path, reservation, &checkpoint).await?;
        Ok(Self {
            spool,
            configuration,
            completed: false,
            detail_stage: 0,
            detail_after: String::new(),
            registered: false,
            attention: false,
            queued: false,
        })
    }

    /// None is resumable progress, not completion. Supply fresh invocation limits
    /// for an explicit resume; source counters and revision identity stay frozen.
    /// A returned result cannot be reused as a subsequent current-state check.
    pub async fn advance(
        &mut self,
        db: &DatabaseConnection,
        limits: &RunLimits,
    ) -> Result<Option<CurrentRetirementInventory>> {
        let run = super::spool::Run::new(limits);
        run.check()?;
        if self.completed {
            return Err(ManagementError::InvalidEvidence);
        }
        if self.spool.scan(db, limits).await? == Progress::Pending
            || self.spool.reduce(&self.configuration, limits).await? == Progress::Pending
        {
            return Ok(None);
        }
        let fence = self.spool.frozen_review().await?.prepare_fence()?;
        let mut tx = ManagementTransaction::begin(db, run.remaining(), limits.cancellation.clone())
            .await
            .map_err(transaction_error)?;
        let mut final_limits = RunLimits::default();
        final_limits.elapsed = run.remaining();
        final_limits.cancellation = limits.cancellation.clone();
        let status = tx
            .run(move |tx, _| {
                Box::pin(async move { fence.current_status(tx, &final_limits).await })
            })
            .await
            .map_err(transaction_error)?;
        tx.rollback().await.map_err(transaction_error)?;
        run.check()?;
        self.completed = true;
        Ok(Some(status))
    }

    /// Reduces current action status outside the final-write transaction. The
    /// returned fence must be checked in the same snapshot as the caller's
    /// bounded migration-destination evaluation. Neither partial progress nor
    /// this unfenced state is a current-status result.
    pub async fn prepare_action_status(
        &mut self,
        db: &DatabaseConnection,
        limits: &RunLimits,
    ) -> Result<Option<(RetirementFence, ActionState)>> {
        let mut run = super::spool::Run::new(limits);
        run.check()?;
        if self.completed {
            return Err(ManagementError::InvalidEvidence);
        }
        if self.spool.scan(db, limits).await? == Progress::Pending
            || self.spool.reduce(&self.configuration, limits).await? == Progress::Pending
        {
            return Ok(None);
        }
        let review = self.spool.frozen_review().await?;
        self.attention |= !review.totals().attribution_complete;
        let route = &review.checkpoint().route;
        let kinds = ["registered_route", "current", "history", "job"];
        while let Some(kind) = kinds.get(self.detail_stage) {
            if !run.page(0, 0)? {
                return Ok(None);
            }
            let next = one(&self.spool.db,
                "SELECT identity,payload FROM facts WHERE kind=$1 AND identity>$2 ORDER BY identity LIMIT 1",
                vec![(*kind).into(), self.detail_after.clone().into()]).await?;
            let Some(next) = next else {
                self.detail_stage += 1;
                self.detail_after.clear();
                continue;
            };
            let id: String = get(&next, "identity")?;
            let payload: String = get(&next, "payload")?;
            match *kind {
                "registered_route" => {
                    let registered: RouteSnapshot = decode(&payload)?;
                    if same_domain(&registered.identity, route) {
                        self.registered = true;
                        let key = NativeKey {
                            table: "pin_provider_routes".into(),
                            values: vec![id.clone()],
                        };
                        let row = self
                            .spool
                            .projection_row(&key)
                            .await?
                            .ok_or(ManagementError::InvalidEvidence)?;
                        self.attention |= !matches!(value(&row, "retired"), Some("1" | "true"));
                    }
                }
                "current" | "history" => {
                    let remote: RemoteSnapshot = decode(&payload)?;
                    if same_domain(&remote.route, route) {
                        // Observation-only history has no original ledger key;
                        // it is unresolved responsibility, never absence proof.
                        if let Ok(key) = decode::<NativeKey>(&id) {
                            let row = self
                                .spool
                                .projection_row(&key)
                                .await?
                                .ok_or(ManagementError::InvalidEvidence)?;
                            if *kind == "history" {
                                let history: HistoricalAbsence = decode(
                                    value(&row, "ledger")
                                        .ok_or(ManagementError::InvalidEvidence)?,
                                )?;
                                self.attention |= !history.confirmed_absent;
                            } else if value(&row, "effect") != Some("absent") {
                                let not_created = value(&row, "effect") == Some("not_created");
                                let unknown = if not_created {
                                    remote.application_references.is_none()
                                        || remote.inflight_operations.is_none()
                                } else {
                                    remote.has_unknown_responsibility()
                                };
                                self.attention |= !matches!(
                                    value(&row, "effect"),
                                    Some("confirmed" | "retained" | "not_created")
                                ) || unknown
                                    || self.has_error(&key).await?;
                                if !not_created && !review.totals().routes_available {
                                    let route = self
                                        .spool
                                        .lookup::<RouteSnapshot>(
                                            "route_revision",
                                            &encode(&remote.route)?,
                                        )
                                        .await?
                                        .ok_or(ManagementError::InvalidEvidence)?;
                                    self.attention |= !route.available;
                                }
                            }
                        } else {
                            self.attention = true;
                        }
                    }
                }
                "job" => {
                    let row: NativeRow = decode(&payload)?;
                    if value(&row, "state") != Some("done") {
                        let key = NativeKey {
                            table: "pin_jobs".into(),
                            values: vec![id.clone()],
                        };
                        self.attention |= self.has_error(&key).await?
                            || value(&row, "state") == Some("running")
                                && value(&row, "locked_until").is_none();
                        self.queued |= value(&row, "operation") == Some("unpin")
                            || value(&row, "state") == Some("running");
                    }
                }
                _ => return Err(ManagementError::InvalidEvidence),
            }
            run.check()?;
            self.detail_after = id;
        }
        let fence = review.prepare_fence()?;
        let state = if self.attention || !self.registered {
            ActionState::NeedsAttention
        } else if self.queued {
            ActionState::Queued
        } else {
            ActionState::Accepted
        };
        run.check()?;
        self.completed = true;
        Ok(Some((fence, state)))
    }

    async fn has_error(&self, key: &NativeKey) -> Result<bool> {
        // Preserve NULL versus present (including empty) without loading an
        // arbitrarily large diagnostic cell into the resident decode budget.
        Ok(one(&self.spool.db,
            "SELECT 1 AS present FROM parts WHERE table_name=$1 AND row_key=$2 AND column_name='last_error' AND offset=0 AND length IS NOT NULL",
            vec![key.table.clone().into(), encode(key)?.into()]).await?.is_some())
    }
}

#[derive(serde::Deserialize)]
struct HistoricalAbsence {
    #[serde(default)]
    confirmed_absent: bool,
}

fn same_domain(left: &RouteIdentity, right: &RouteIdentity) -> bool {
    left.backend == right.backend
        && left.scope == right.scope
        && left.resource_type == right.resource_type
}

fn transaction_error(error: TransactionFailure) -> ManagementError {
    if error.rollback_confirmed {
        error.management_error()
    } else {
        ManagementError::Database
    }
}
