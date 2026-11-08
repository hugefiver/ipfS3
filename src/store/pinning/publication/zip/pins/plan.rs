use super::*;
use crate::pinning::{
    config::ProviderMode,
    quota::{CapacityDecision, ProviderUsage, reservation_decision},
    zip_policy::ZipSelectedLease,
};

impl PinWriteSet {
    pub(super) fn end_owners(&mut self, owners: &[String], now: DateTime<Utc>) -> AppResult<()> {
        for owner in owners {
            let mut pairs = BTreeSet::new();
            for lease in self
                .leases
                .values_mut()
                .filter(|l| l.owner_object_id == *owner && l.state == "active")
            {
                lease.state = "cancelled".into();
                lease.generation = increment(lease.generation)?;
                lease.last_touched_at = now;
                for target in self.targets.values_mut().filter(|t| t.lease_id == lease.id) {
                    if desired(&target.state) {
                        pairs.insert((target.provider.clone(), target.cid.clone()));
                    }
                    if desired(&target.state)
                        || matches!(target.state.as_str(), "quota_waiting" | "quota_blocked")
                    {
                        target.state = "released".into();
                    }
                }
            }
            for pair in pairs {
                if let Some(remote) = self.remotes.get_mut(&pair) {
                    remote.epoch = increment(remote.epoch)?;
                    remote.last_touched_at = now;
                    self.ended.insert(pair);
                }
            }
        }
        Ok(())
    }

    pub(super) fn assign(
        &mut self,
        selected: &ZipSelectedLease,
        keys: &AllocationKeys,
        now: DateTime<Utc>,
        work: DecodeWork<'_>,
    ) -> AppResult<()> {
        let intent = &selected.intent;
        let id = publication::lease_id(&selected.owner.object_id, intent.source);
        let targets = selected
            .targets
            .iter()
            .map(|t| (t.cid.clone(), t.logical_size))
            .collect::<BTreeMap<_, _>>();
        if targets.is_empty() {
            return Ok(());
        }
        let seconds = i64::try_from(intent.duration.as_seconds()).map_err(|_| invalid())?;
        let expires_at = now
            .checked_add_signed(chrono::TimeDelta::seconds(seconds))
            .ok_or_else(invalid)?;
        if expires_at <= now || self.leases.contains_key(&id) {
            return Err(invalid());
        }
        let provider_mode = publication::provider_mode_name(intent.provider_mode).to_owned();
        let content_mode = publication::content_mode_name(intent.content_mode).to_owned();
        let contract = pin_lease_requirement::contract::BirthContract {
            provider_mode: provider_mode.clone(),
            content_mode: content_mode.clone(),
            content_cids: targets.keys().cloned().collect(),
            candidate_order: intent.providers.clone(),
            candidates: intent
                .providers
                .iter()
                .map(|p| (p.clone(), keys.routes.get(p).cloned()))
                .collect(),
        };
        let contract = serde_json::to_value(&contract).map_err(|_| invalid())?;
        work.copy(&contract).map_err(work_error)?;
        let contract = serde_json::to_string(&contract).map_err(|_| invalid())?;
        let target_bytes = selected.targets.iter().try_fold(0usize, |total, target| {
            total
                .checked_add(
                    target
                        .cid
                        .len()
                        .checked_add(768)
                        .ok_or(AppError::ManagementBudget)?
                        .checked_mul(keys.enabled.len().max(1))
                        .ok_or(AppError::ManagementBudget)?,
                )
                .ok_or(AppError::ManagementBudget)
        })?;
        let retained = contract
            .len()
            .checked_add(id.len())
            .and_then(|n| n.checked_add(selected.owner.object_id.len()))
            .and_then(|n| n.checked_add(intent.policy_id.len()))
            .and_then(|n| n.checked_add(target_bytes))
            .and_then(|n| n.checked_add(1024))
            .ok_or(AppError::ManagementBudget)?;
        self.resident.push(sql::retain_copy(work, retained)?);
        self.requirements.push(pin_lease_requirement::Model {
            lease_id: id.clone(),
            owner_object_id: selected.owner.object_id.clone(),
            contract,
            created_at: now,
        });
        self.leases.insert(
            id.clone(),
            pin_lease::Model {
                id: id.clone(),
                owner_object_id: selected.owner.object_id.clone(),
                source: publication::source_name(intent.source).into(),
                policy_id: intent.policy_id.clone(),
                provider_mode,
                content_mode,
                created_at: now,
                last_touched_at: now,
                expires_at,
                generation: 1,
                state: "active".into(),
            },
        );
        let providers = publication::ordered_enabled_providers(intent, &keys.limits)?;
        let mut assigned: BTreeMap<Pair, &'static str> = BTreeMap::new();
        for (raw, size) in targets {
            let mut unavailable = None;
            for provider in &providers {
                let cid = &keys.canonical[&(provider.clone(), raw.clone())];
                let pair = (provider.clone(), cid.clone());
                let state = if let Some(state) = assigned.get(&pair) {
                    *state
                } else {
                    self.reserve(&pair, size, keys, now)?
                };
                if intent.provider_mode == ProviderMode::All || state == "waiting" {
                    if !assigned.contains_key(&pair) {
                        self.add_target(&id, &pair, size, state, now);
                    }
                    assigned.insert(pair, state);
                    if intent.provider_mode == ProviderMode::One {
                        unavailable = None;
                        break;
                    }
                } else if unavailable.is_none() {
                    unavailable = Some((pair, state));
                }
            }
            if let Some((pair, state)) = unavailable {
                if !assigned.contains_key(&pair) {
                    self.add_target(&id, &pair, size, state, now);
                }
                assigned.insert(pair, state);
            }
        }
        Ok(())
    }

    fn add_target(&mut self, lease: &str, pair: &Pair, size: i64, state: &str, now: DateTime<Utc>) {
        let id = publication::leases::target_id(lease, &pair.0, &pair.1);
        self.targets.insert(
            id.clone(),
            pin_lease_target::Model {
                id,
                lease_id: lease.into(),
                provider: pair.0.clone(),
                cid: pair.1.clone(),
                logical_size: size,
                state: state.into(),
                last_error: None,
                created_at: now,
                last_touched_at: now,
            },
        );
        if state == "waiting" {
            self.affected.insert(pair.clone());
        }
    }

    fn reserve(
        &mut self,
        pair: &Pair,
        size: i64,
        keys: &AllocationKeys,
        now: DateTime<Utc>,
    ) -> AppResult<&'static str> {
        match self.before.barriers[&pair.0] {
            AdmissionBarrier::Temporary => return Ok("quota_waiting"),
            AdmissionBarrier::Operator => return Ok("quota_blocked"),
            AdmissionBarrier::Clear => {}
        }
        let limits = &keys.limits[&pair.0];
        if size > limits.max_bytes {
            return Ok("quota_blocked");
        }
        if let Some(remote) = self.remotes.get_mut(pair)
            && remote.status != "absent"
        {
            if !matches!(
                remote.status.as_str(),
                "reserved" | "queued" | "pinning" | "pinned" | "failed"
            ) || self.before.retired_sources.contains(pair)
            {
                return Err(invalid());
            }
            let historical = self.ledgers.get_mut(pair).ok_or_else(invalid)?;
            let old: crate::pinning::identity::ProviderRouteSnapshot =
                serde_json::from_str(historical.route.as_deref().ok_or_else(invalid)?)
                    .map_err(|_| invalid())?;
            let new = &keys.routes[&pair.0];
            if old.provider_id != new.provider_id
                || old.backend != new.backend
                || old.scope != new.scope
                || old.storage_domain != new.storage_domain
                || old.credential_revision != new.credential_revision
                || old.endpoint_revision != new.endpoint_revision
                || old.secret_ref != new.secret_ref
                || old.api_profile != new.api_profile
                || old.strategy != new.strategy
            {
                return Err(invalid());
            }
            remote.epoch = increment(remote.epoch)?;
            remote.last_touched_at = remote.last_touched_at.max(now);
            if remote.status == "pinned"
                && historical.effect == "retained"
                && historical.remote_pinned_at.is_some()
                && !self.before.unconfirmed_sources.contains(pair)
            {
                historical.effect = "confirmed".into();
            }
            return Ok("waiting");
        }
        if pair.0.starts_with("domain:") && !self.before.new_allocation_safe {
            return Err(invalid());
        }
        let usage = self.usages.get_mut(&pair.0).ok_or_else(invalid)?;
        match reservation_decision(
            ProviderUsage {
                reserved_bytes: usage.reserved_bytes,
                reserved_pins: usage.reserved_pins,
            },
            size,
            limits,
        )? {
            CapacityDecision::Block => return Ok("quota_blocked"),
            CapacityDecision::Wait => return Ok("quota_waiting"),
            CapacityDecision::Reserve => {}
        }
        let old = self.remotes.get(pair);
        if old.is_some() && self.ledgers.get(pair).is_some_and(|l| l.effect != "absent") {
            return Err(invalid());
        }
        let epoch = old.map_or(Ok(1), |r| increment(r.epoch))?;
        let reallocation = old.is_some();
        usage.reserved_bytes = usage.reserved_bytes.checked_add(size).ok_or_else(invalid)?;
        usage.reserved_pins = increment(usage.reserved_pins)?;
        self.remotes.insert(
            pair.clone(),
            remote_pin::Model {
                provider: pair.0.clone(),
                cid: pair.1.clone(),
                request_id: None,
                cid_size: size,
                status: "reserved".into(),
                epoch,
                failure_attempts: 0,
                next_retry_at: None,
                last_failed_request_id: None,
                last_touched_at: now,
                last_error_class: None,
                last_error_text: None,
            },
        );
        if reallocation || !self.ledgers.contains_key(pair) {
            self.ledgers.insert(
                pair.clone(),
                remote_pin_ledger::Model {
                    provider: pair.0.clone(),
                    cid: pair.1.clone(),
                    route: Some(
                        serde_json::to_string(&keys.routes[&pair.0]).map_err(|_| invalid())?,
                    ),
                    ownership: "unknown".into(),
                    effect: "reserved".into(),
                    first_observed_at: None,
                    last_observed_at: None,
                    remote_pinned_at: None,
                    gateway_verified_at: None,
                    content_verified_at: None,
                    first_error: None,
                    last_error: None,
                },
            );
        }
        Ok("waiting")
    }
}
