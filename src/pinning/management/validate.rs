use std::collections::BTreeSet;

use super::model::*;

pub const MAX_ITEMS: usize = 128;
pub const MAX_RECORDS: usize = 4096;
pub const MAX_TEXT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_PROVIDERS: usize = 64;

#[derive(Default)]
pub(crate) struct Budget {
    records: usize,
    bytes: usize,
}

impl Budget {
    fn rows(&mut self, count: usize) -> Result<(), SafeError> {
        self.records = self
            .records
            .checked_add(count)
            .ok_or(SafeError::LimitExceeded)?;
        if self.records > MAX_RECORDS {
            return Err(SafeError::LimitExceeded);
        }
        Ok(())
    }

    fn text(&mut self, text: &str, max: usize) -> Result<(), SafeError> {
        if text.is_empty() || text.len() > max || text.chars().any(char::is_control) {
            return Err(SafeError::InvalidInput);
        }
        self.bytes = self
            .bytes
            .checked_add(text.len())
            .ok_or(SafeError::LimitExceeded)?;
        if self.bytes > MAX_TEXT_BYTES {
            return Err(SafeError::LimitExceeded);
        }
        Ok(())
    }

    fn id(&mut self, text: &str) -> Result<(), SafeError> {
        self.text(text, 256)?;
        if !text
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.' | b':'))
        {
            return Err(SafeError::InvalidInput);
        }
        Ok(())
    }

    fn route(&mut self, route: &RouteIdentity) -> Result<(), SafeError> {
        self.rows(1)?;
        for text in [
            &route.provider_id,
            &route.backend,
            &route.scope,
            &route.storage_domain,
            &route.api_profile,
            &route.strategy,
        ] {
            self.id(text)?;
            if text.bytes().any(|c| c.is_ascii_uppercase()) {
                return Err(SafeError::InvalidInput);
            }
        }
        if route.credential_revision == 0 || route.endpoint_revision == 0 {
            return Err(SafeError::InvalidInput);
        }
        Ok(())
    }

    fn cid(&mut self, cid: &str) -> Result<(), SafeError> {
        self.text(cid, 512)?;
        // Syntactic bound only. The parent must use the existing CID parser.
        if !cid.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-') {
            return Err(SafeError::InvalidInput);
        }
        Ok(())
    }

    fn exact(&mut self, object: &ExactObject) -> Result<(), SafeError> {
        self.text(&object.bucket, 255)?;
        self.text(&object.key, 1024)?;
        if let PublicVersionId::Version(id) = &object.public_version {
            self.text(id, 256)?;
            if id == "null" {
                return Err(SafeError::InvalidInput);
            }
        }
        self.id(&object.private_version_id)?;
        self.id(&object.object_id)?;
        self.cid(&object.cid)
    }

    fn requirements(&mut self, requirements: &Requirements) -> Result<(), SafeError> {
        self.id(&requirements.policy_revision)?;
        if requirements.provider_ids.is_empty()
            || requirements.provider_ids.len() > MAX_PROVIDERS
            || requirements.min_replicas == 0
        {
            return Err(SafeError::InvalidInput);
        }
        self.rows(requirements.provider_ids.len())?;
        unique(requirements.provider_ids.iter())?;
        if requirements.levels.is_empty() || requirements.levels.len() > 6 {
            return Err(SafeError::InvalidInput);
        }
        unique(requirements.levels.iter())?;
        for id in &requirements.provider_ids {
            self.id(id)?;
        }
        Ok(())
    }

    fn object(&mut self, object: &ObjectSnapshot) -> Result<(), SafeError> {
        if let Some(proof)=&object.read_evidence {
            if proof.identity!=object.identity || object.readable!=Some(true) || proof.expires_at<=proof.observed_at || proof.idle_timeout_millis==0 || proof.proof.has_stored_byte_count()!=proof.stored_bytes.is_some()
                ||proof.proof==super::evidence::ReadProof::CompleteLocalDirectoryDagBlocksV1&&proof.stored_bytes==Some(0) {return Err(SafeError::InconsistentSnapshot);}
            self.id(&proof.node_identity)?;
        }
        self.rows(1)?;
        self.exact(&object.identity)?;
        if let Some(value) = &object.control_revision { self.id(value)?; }
        if let Some(value) = &object.config_revision { self.id(value)?; }
        if object.allowed_routes.len() > MAX_PROVIDERS {
            return Err(SafeError::LimitExceeded);
        }
        unique(object.allowed_routes.iter())?;
        for route in &object.allowed_routes {
            self.route(route)?;
        }
        if let SelectionEvidence::TrustedManifest {
            batch_id,
            entry_path,
            ..
        } = &object.evidence
        {
            self.id(batch_id)?;
            self.text(entry_path, 1024)?;
            if entry_path.starts_with('/')
                || entry_path.contains('\\')
                || entry_path
                    .split('/')
                    .any(|part| part.is_empty() || matches!(part, "." | ".."))
            {
                return Err(SafeError::InvalidInput);
            }
        }
        Ok(())
    }

    fn lease(&mut self, lease: &LeaseSnapshot) -> Result<(), SafeError> {
        self.rows(1)?;
        self.id(&lease.id)?;
        self.id(&lease.object_id)?;
        let valid_time=if let Some(tuple)=&lease.native_tuple {
            let start=crate::store::pinning::management::native::value(tuple,"created_at").and_then(|raw|crate::store::pinning::management::native::parse_time(raw).ok()).ok_or(SafeError::UnknownEvidence)?;
            let expiry=crate::store::pinning::management::native::value(tuple,"expires_at").and_then(|raw|crate::store::pinning::management::native::parse_time(raw).ok()).ok_or(SafeError::UnknownEvidence)?;
            expiry>start && start.timestamp()==lease.started_at && expiry.timestamp()==lease.expires_at
        }else{lease.expires_at>lease.started_at};
        if lease.generation <= 0 || lease.started_at < 0 || !valid_time {
            return Err(SafeError::InvalidInput);
        }
        Ok(())
    }

    fn target(&mut self, target: &TargetSnapshot) -> Result<(), SafeError> {
        self.rows(1)?;
        self.id(&target.id)?;
        self.lease(&target.lease)?;
        self.cid(&target.cid)?;
        if let Some(route)=&target.route { self.route(route)?; }
        unique(target.frozen_candidates.iter().map(|candidate|&candidate.slot))?;
        for candidate in &target.frozen_candidates {
            self.text(&candidate.slot,1024)?;
            self.text(&candidate.allocation,1024)?;
            if let Some(route)=&candidate.route { self.route(route)?; }
            if let Some(id)=&candidate.receipt_id { self.id(id)?; }
        }
        if let Some(requirements) = &target.requirements { self.requirements(requirements)?; }
        if target.native_tuple.is_empty() {
            return Err(SafeError::InconsistentSnapshot);
        }
        Ok(())
    }

    fn remote(&mut self, remote: &RemoteSnapshot) -> Result<(), SafeError> {
        self.rows(1)?;
        self.route(&remote.route)?;
        self.cid(&remote.key.cid)?;
        if remote.key != remote.route.remote_key(&remote.key.cid) || remote.epoch < 0 {
            return Err(SafeError::InconsistentSnapshot);
        }
        if let Some(id) = &remote.resource_id {
            self.id(id)?;
        }
        if remote.confirmations.len() > 6 {
            return Err(SafeError::InvalidInput);
        }
        unique(remote.confirmations.iter())?;
        if matches!(
            remote.effect,
            SideEffect::NotSubmitted | SideEffect::Rejected
        ) && (remote.resource_id.is_some()
            || remote.inflight_operations.is_some_and(|n|n > 0)
            || remote.retained
            || remote.cleanup_pending
            || remote.confirmed_replicas.is_some_and(|n|n > 0)
            || remote
                .confirmations
                .iter()
                .any(|level| *level != ConfirmationLevel::Accepted))
        {
            return Err(SafeError::InconsistentSnapshot);
        }
        if remote.effect == SideEffect::Present && remote.resource_id.is_none() {
            return Err(SafeError::InconsistentSnapshot);
        }
        Ok(())
    }

    fn inventory(&mut self, inventory: &ProviderInventory) -> Result<(), SafeError> {
        self.rows(1)?;
        self.rows(inventory.root_reference_ids.len())?;
        self.rows(inventory.leases.len())?;
        self.id(&inventory.provider_id)?;
        unique(inventory.leases.iter().map(|lease| &lease.id))?;
        unique(inventory.root_reference_ids.iter())?;
        for lease in &inventory.leases {
            self.lease(lease)?;
        }
        for root in &inventory.root_reference_ids {
            if root.table!="zip_root_references" || root.values.len()!=6 {return Err(SafeError::InconsistentSnapshot);}
            for value in &root.values {self.text(value,512)?;}
        }
        Ok(())
    }

    fn domain(&mut self, domain: &LedgerDomainSnapshot) -> Result<(), SafeError> {
        self.rows(1)?;
        self.id(&domain.backend)?;
        self.id(&domain.scope)?;
        Ok(())
    }

    fn request(&mut self, request: &ItemRequest) -> Result<(), SafeError> {
        self.rows(1)?;
        label(&request.item_id)?;
        self.id(&request.item_id)?;
        match &request.subject {
            Subject::Object { identity, target } => {
                self.exact(identity)?;
                match target {
                    TargetSelection::New(route) => self.route(route)?,
                    TargetSelection::Existing { target_id, source } => { self.id(target_id)?; if let Some(source)=source {self.exact(source)?;} },
                }
            }
            Subject::Provider { route } => self.route(route)?,
        }
        match &request.action {
            Action::Create(intent) | Action::Reapply(intent) | Action::Backfill(intent) => {
                self.requirements(&intent.requirements)?;
                if intent.started_at < 0 || intent.expires_at <= intent.started_at {
                    return Err(SafeError::InvalidInput);
                }
            }
            Action::Renew { expires_at } if *expires_at <= 0 => {
                return Err(SafeError::InvalidInput);
            }
            Action::Migrate { destination } => self.route(destination)?,
            Action::RetireWith {selection}=>match selection {
                RetirementSelection::Retain=>{},
                RetirementSelection::Migrate {targets}=> {
                    self.rows(targets.len())?;
                    if targets.is_empty()||targets.len()>MAX_RECORDS {return Err(SafeError::LimitExceeded);}
                    unique(targets.iter().map(|target|(&target.lease_id,&target.source_target_id,&target.cid)))?;
                    for target in targets {label(&target.plan_id)?;label(&target.item_id)?;self.id(&target.lease_id)?;self.id(&target.source_target_id)?;self.cid(&target.cid)?;self.route(&target.destination)?;}
                },
                RetirementSelection::ControlledCleanup {resources}=> {
                    self.rows(resources.len())?;
                    if resources.len()>MAX_RECORDS {return Err(SafeError::LimitExceeded);}
                    unique(resources.iter().map(|resource|(&resource.key,resource.epoch)))?;
                    for resource in resources {self.route(&resource.route)?;self.cid(&resource.key.cid)?;self.id(&resource.resource_id)?;if resource.epoch<1||resource.route.remote_key(&resource.key.cid)!=resource.key {return Err(SafeError::InvalidInput);}}
                },
            },
            _ => {}
        }
        Ok(())
    }

    fn capture(&mut self, capture: &Capture) -> Result<(), SafeError> {
        match capture {
            Capture::Object(capture) => {
                self.object(&capture.object)?;
                self.domain(&capture.ledger_domain)?;
                if let Some(domain) = &capture.destination_ledger_domain {
                    self.domain(domain)?;
                }
                if let Some(target) = &capture.target {
                    self.target(target)?;
                }
                for route in [
                    Some(&capture.route),
                    capture.remote_route.as_ref(),
                    capture.destination_route.as_ref(),
                    capture.destination_remote_route.as_ref(),
                ]
                .into_iter()
                .flatten()
                {
                    self.route(&route.identity)?;
                }
                for remote in [&capture.remote, &capture.destination_remote]
                    .into_iter()
                    .flatten()
                {
                    self.remote(remote)?;
                }
                for remote in &capture.historical_remotes {
                    self.remote(remote)?;
                }
                for route in &capture.historical_routes {
                    self.route(&route.identity)?;
                }
                for route in &capture.requirement_routes {self.route(&route.identity)?;}
            }
            Capture::Provider(capture) => {
                self.inventory(&capture.inventory)?;
                for route in &capture.routes {
                    self.route(&route.identity)?;
                }
                for target in &capture.targets {
                    self.target(target)?;
                }
                for remote in &capture.remotes {
                    self.remote(remote)?;
                }
                for remote in &capture.historical_remotes {
                    self.remote(remote)?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn entry(&mut self, entry: &PlanEntry) -> Result<(), SafeError> {
        self.text(entry.id.as_str(), 160)?;
        let (plan_id, item_id) = entry
            .id
            .as_str()
            .strip_prefix("management-v1:")
            .and_then(|id| id.split_once(':'))
            .ok_or(SafeError::InvalidInput)?;
        label(plan_id)?;
        label(item_id)?;
        if item_id != entry.request.item_id {
            return Err(SafeError::InvalidInput);
        }
        self.request(&entry.request)?;
        if let Some(capture) = &entry.capture {
            self.capture(capture)?;
        }
        Ok(())
    }
}

fn unique<T: Ord>(values: impl IntoIterator<Item = T>) -> Result<(), SafeError> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(SafeError::DuplicateIdentity);
        }
    }
    Ok(())
}

pub(crate) fn validate_route_identity(route: &RouteIdentity) -> Result<(), SafeError> {
    Budget::default().route(route)
}

pub(crate) fn label(value: &str) -> Result<(), SafeError> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b'-' | b'_'))
    {
        return Err(SafeError::InvalidInput);
    }
    Ok(())
}

pub fn validate_request(request: &PlanRequest) -> Result<(), SafeError> {
    label(&request.plan_id)?;
    if request.items.is_empty() || request.items.len() > MAX_ITEMS {
        return Err(SafeError::LimitExceeded);
    }
    unique(request.items.iter().map(|item| &item.item_id))?;
    let mut budget = Budget::default();
    for item in &request.items {
        budget.request(item)?;
    }
    Ok(())
}

pub fn validate_snapshot(snapshot: &Snapshot) -> Result<(), SafeError> {
    if let Some(frame)=&snapshot.native_frame {frame.validate().map_err(|error|if error==crate::store::pinning::management::ManagementError::Budget {SafeError::LimitExceeded}else{SafeError::InconsistentSnapshot})?;}
    if let Some(frame)=&snapshot.native_frame&&frame.has_private_material() {
        for proof in snapshot.objects.iter().filter_map(|object|object.read_evidence.as_ref()).filter(|proof|proof.proof==super::evidence::ReadProof::CompleteLocalDirectoryDagBlocksV1) {
            crate::store::pinning::management::actions::accounting::trusted_proof(frame,proof).map_err(|_|SafeError::InconsistentSnapshot)?;
        }
    }
    if snapshot.observed_at < 0 || chrono::DateTime::from_timestamp(snapshot.observed_at,0).is_none() {
        return Err(SafeError::InvalidInput);
    }
    // Fail before traversing oversized collections or allocating indexes.
    let mut budget = Budget::default();
    budget.rows(snapshot.objects.len())?;
    budget.rows(snapshot.routes.len())?;
    budget.rows(snapshot.targets.len())?;
    budget.rows(snapshot.remotes.len())?;
    budget.rows(snapshot.historical_remotes.len())?;
    budget.rows(snapshot.ledger_domains.len())?;
    budget.rows(snapshot.inventories.len())?;
    budget.rows(snapshot.applied.len())?;
    unique(
        snapshot
            .objects
            .iter()
            .map(|object| &object.identity.private_version_id),
    )?;
    unique(
        snapshot
            .objects
            .iter()
            .map(|object| &object.identity.object_id),
    )?;
    unique(snapshot.routes.iter().map(|route| &route.identity))?;
    unique(snapshot.targets.iter().map(|target| &target.id))?;
    unique(snapshot.remotes.iter().map(|remote| &remote.key))?;
    unique(snapshot.unknown_resources.iter())?;
    for key in &snapshot.unknown_resources {
        let frame=snapshot.native_frame.as_ref().ok_or(SafeError::InconsistentSnapshot)?;
        if !frame.frontier.contains(key) {return Err(SafeError::InconsistentSnapshot);}
    }
    unique(
        snapshot
            .historical_remotes
            .iter()
            .map(|remote| (&remote.key, remote.epoch)),
    )?;
    unique(
        snapshot
            .ledger_domains
            .iter()
            .map(|domain| (&domain.backend, &domain.scope, domain.resource_type)),
    )?;
    unique(
        snapshot
            .inventories
            .iter()
            .map(|inventory| &inventory.provider_id),
    )?;
    unique(snapshot.applied.iter().map(|receipt| &receipt.binding.id))?;
    for object in &snapshot.objects {
        budget.object(object)?;
    }
    for route in &snapshot.routes {
        budget.route(&route.identity)?;
    }
    for target in &snapshot.targets {
        budget.target(target)?;
    }
    for remote in &snapshot.remotes {
        budget.remote(remote)?;
    }
    for remote in &snapshot.historical_remotes {
        budget.remote(remote)?;
    }
    for domain in &snapshot.ledger_domains {
        budget.domain(domain)?;
    }
    for inventory in &snapshot.inventories {
        budget.inventory(inventory)?;
    }
    for receipt in &snapshot.applied {
        budget.entry(&receipt.binding)?;
        if matches!(receipt.binding.status, PlanStatus::Blocked(_))
            || receipt.binding.capture.is_none()
            || receipt.binding.step.is_none()
        {
            return Err(SafeError::InvalidInput);
        }
    }
    // Shared lease rows must carry the same durable fence and TTL everywhere.
    let mut leases = std::collections::BTreeMap::new();
    for lease in snapshot.targets.iter().map(|target| &target.lease).chain(
        snapshot
            .inventories
            .iter()
            .flat_map(|inventory| &inventory.leases),
    ) {
        if leases
            .insert(&lease.id, lease)
            .is_some_and(|old| old != lease)
        {
            return Err(SafeError::InconsistentSnapshot);
        }
    }
    let mut epochs = std::collections::BTreeMap::new();
    for remote in snapshot.remotes.iter().chain(&snapshot.historical_remotes) {
        if let Some(old) = epochs.insert((&remote.key, remote.epoch), remote) {
            return Err(if old == remote {
                SafeError::DuplicateIdentity
            } else {
                SafeError::InconsistentSnapshot
            });
        }
    }
    // Decompressed manual leases have archive owners distinct from target CID.
    Ok(())
}
