use super::*;
use ipfs_s3_gateway::{
    pinning::management::retirement::{
        ConfirmationAdvance, ConfirmationEffects, ConfirmationPhase, ConfirmationProgress,
        PreparedPlanning, RunLimits,
    },
    store::pinning::management::native::meter::Budget,
};

#[derive(thiserror::Error)]
#[error("management confirmation stopped or needs attention (details withheld)")]
pub struct FailureReport {
    pub report: Value,
    #[source]
    cause: anyhow::Error,
}

impl std::fmt::Debug for FailureReport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("management confirmation failure (details withheld)")
    }
}

pub(super) fn failure(mut report: Value, cause: anyhow::Error) -> anyhow::Error {
    report["outcome"] = json!(if report["effects"]["commit_unknown"] == true
        || report["confirmation_committed"] == true
    {
        "needs_attention"
    } else {
        "stopped"
    });
    anyhow::Error::new(FailureReport { report, cause })
}

#[derive(Default)]
struct Publication {
    phase: ConfirmationPhase,
    effects: ConfirmationEffects,
    confirmed: bool,
    exported: bool,
}

impl Publication {
    fn accept(&mut self, advance: &ConfirmationAdvance) {
        self.phase = advance.phase;
        self.effects.database_commits += advance.effects.database_commits;
        self.effects.commit_unknown |= advance.effects.commit_unknown;
    }

    fn report(&self, prepared: &PreparedPlanning, complete: bool) -> Value {
        let mut report = json!({
            "mode": "confirm", "outcome": if complete { "complete" } else { "incomplete" },
            "phase": self.phase, "effects": self.effects, "effects_scope": "invocation",
            "database_writes": self.effects.database_commits > 0 || self.effects.commit_unknown,
            "confirmation_committed": self.confirmed, "artifact_exported": self.exported,
            "source_rpc": "not_performed", "provider_rpc": "not_performed",
        });
        if let Some(path) = prepared.checkpoint_path() {
            report["checkpoint"] = json!(path);
            report["checkpoint_retained"] = json!(!complete);
        }
        report
    }
}

pub(super) struct ProviderConfirmation<'a> {
    pub db: &'a DatabaseConnection,
    pub config: &'a ManagementConfig,
    pub auth: &'a LocalAuthorization,
    pub command: &'a Command,
    pub prepared: &'a PreparedPlanning,
    pub request: &'a PlanRequest,
    pub snapshot: &'a Snapshot,
    pub limits: &'a RunLimits,
    pub needs_attention: &'a [db_snapshot::EvidenceGap],
}

impl ProviderConfirmation<'_> {
    pub(super) async fn execute(
        &self,
        budget: Budget,
        work: records::SharedDecodeBudget,
    ) -> Result<Value> {
        let mut publication = Publication::default();
        let result = self.publish(budget, work, &mut publication).await;
        match result {
            Ok(complete) => {
                let mut report = publication.report(self.prepared, complete);
                if complete {
                    report["artifact_trust"] =
                        json!("public_original_confirmation_requires_local_database_authorization");
                    report["summary"] = json!(self.prepared.plan.summary());
                    report["needs_attention"] = json!(self.needs_attention);
                    report["items"] =
                        json!(self.prepared.plan.items().iter().enumerate().map(|(index, entry)|
                    json!({"item_index": index, "status": entry.status, "step": entry.step})
                ).collect::<Vec<_>>());
                }
                Ok(report)
            }
            Err(error) => Err(failure(publication.report(self.prepared, false), error)),
        }
    }

    async fn publish(
        &self,
        mut budget: Budget,
        work: records::SharedDecodeBudget,
        publication: &mut Publication,
    ) -> Result<bool> {
        let Self {
            db,
            config,
            auth,
            command,
            prepared,
            request,
            snapshot,
            limits,
            ..
        } = *self;
        let Command::Confirm {
            output,
            acknowledgements,
            ttl_seconds,
            ..
        } = command
        else {
            return Err(input::invalid());
        };
        let caps = match prepared.reserve_capture(&budget, limits) {
            Ok(caps) => caps,
            Err(error) => {
                prepared.account_capture(&mut budget, limits).await?;
                return Err(error.into());
            }
        };
        let advanced = prepared
            .advance_confirmation(db, config, auth, request, snapshot, &caps)
            .await;
        publication.accept(&advanced);
        if advanced.progress.is_err()
            || matches!(&advanced.progress, Ok(ConfirmationProgress::Incomplete))
        {
            prepared.account_capture(&mut budget, limits).await?;
        }
        let (binding, file, budget) = match advanced.progress? {
            ConfirmationProgress::Incomplete => {
                if prepared.checkpoint_path().is_none() {
                    return Err(anyhow::anyhow!(
                        "management confirmation staging incomplete; use an explicit checkpoint for resumable planning"
                    ));
                }
                return Ok(false);
            }
            ConfirmationProgress::Ready(ready) => {
                let preflight = validate_acknowledgements(ready.plan(), acknowledgements)
                    .and_then(|()| header(auth, snapshot, None));
                let header = match preflight {
                    Ok(header) => header,
                    Err(error) => {
                        prepared.account_capture(&mut budget, limits).await?;
                        return Err(error);
                    }
                };
                let (result, budget) = ready
                    .preflight(db, budget, header, acknowledgements, limits)
                    .await;
                result.context("management confirmation export preflight failed")?;
                let file = files::reserve(output).await?.into_std().await;
                budget.check()?;
                let advanced = records::confirm_ready(
                    db,
                    config,
                    auth,
                    records::ReadyPublication {
                        ready: &ready,
                        acknowledgements,
                        ttl: chrono::Duration::seconds(i64::from(*ttl_seconds)),
                    },
                    work,
                    limits,
                )
                .await;
                publication.confirmed = advanced.effects.database_commits > 0;
                publication.accept(&advanced);
                let ConfirmationProgress::Confirmed(binding) = advanced.progress? else {
                    return Err(input::invalid());
                };
                publication.confirmed = true;
                (*binding, file, budget)
            }
            ConfirmationProgress::Confirmed(binding) => {
                publication.phase = ConfirmationPhase::Confirmed;
                publication.confirmed = true;
                prepared.account_capture(&mut budget, limits).await?;
                if i64::from(*ttl_seconds)
                    != (binding.expires_at - binding.confirmed_at).num_seconds()
                    || acknowledgements.as_slice()
                        != binding.confirmation.candidate_item_ids.as_slice()
                {
                    return Err(input::invalid());
                }
                let file = files::reserve(output).await?.into_std().await;
                (*binding, file, budget)
            }
        };
        if &*binding.request != request
            || binding.confirmation.candidate_item_ids.as_slice() != acknowledgements
        {
            return Err(input::invalid());
        }
        let (result, _) = prepared
            .export_confirmed_file(db, (file, budget), &binding, Projection::Review, limits)
            .await;
        result.context("management confirmed review export failed")?;
        publication.exported = true;
        Ok(true)
    }
}

fn validate_acknowledgements(
    plan: &management::DryRunPlan,
    acknowledgements: &[String],
) -> Result<()> {
    if plan.items().iter().any(|item| {
        matches!(item.status, PlanStatus::Blocked(_))
            || item.status == PlanStatus::Candidate
                && !acknowledgements.contains(&item.request.item_id)
    }) || acknowledgements.iter().any(|id| {
        !plan
            .items()
            .iter()
            .any(|item| item.status == PlanStatus::Candidate && &item.request.item_id == id)
    }) {
        return Err(input::invalid());
    }
    Ok(())
}
