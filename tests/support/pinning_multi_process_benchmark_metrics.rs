//! Measurements and historical-call fixture preparation. Never log DB errors,
//! provider text, route snapshots, URLs, credentials or filesystem paths.

use super::{
    BenchResult, Case, RPS, W,
    fixture::{Event, Fixture},
    http,
};
use chrono::Utc;
use ipfs_s3_gateway::{
    pinning::coordinator::PinningCoordinator,
    store::{
        Store,
        entities::{
            pin_invocation_route, pin_job, pin_lease, pin_lease_target, pin_provider_route,
            pin_provider_usage, remote_pin, remote_pin_ledger,
        },
        pinning::{jobs, leases},
    },
};
use sea_orm::{
    ColumnTrait, EntityTrait, QueryFilter, QuerySelect, TransactionTrait, sea_query::Expr,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

pub(crate) fn psa_ledger_row_matches_contract(
    row: &remote_pin_ledger::Model,
    registered: &ipfs_s3_gateway::pinning::identity::ProviderRouteSnapshot,
) -> bool {
    row.route
        .as_deref()
        .and_then(|raw| {
            serde_json::from_str::<ipfs_s3_gateway::pinning::identity::ProviderRouteSnapshot>(raw)
                .ok()
        })
        .as_ref()
        == Some(registered)
        && row.ownership == "unknown"
        && row.effect == "confirmed"
}

/// Exact known error signatures only. Never persist/display the error string.
pub fn error_category(text: &str) -> &'static str {
    if text.contains("database is locked") || text.contains("SQLITE_BUSY") {
        "sqlite_busy"
    } else if text.contains("pin job claim lost") {
        "claim_lost"
    } else if text.contains("immutable owner context") {
        "missing_owner_context"
    } else if text.contains("constraint failed") || text.contains("UNIQUE constraint") {
        "database_constraint"
    } else if text.contains("needs_attention") || text.contains("operator attention") {
        "operator_attention"
    } else if text.contains("provider rate limited") {
        "provider_rate_limited"
    } else if text.contains("provider request not submitted") {
        "provider_not_submitted"
    } else {
        "other_error_suppressed"
    }
}

pub fn safe_identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_:".contains(&b))
}

fn typed_provider_category(value: Option<&str>) -> Option<&'static str> {
    match value? {
        "rate_limited" => Some("rate_limited"),
        "transient" => Some("transient"),
        "authentication" => Some("authentication"),
        "protocol" => Some("protocol"),
        "quota" => Some("quota"),
        "remote_failed" => Some("remote_failed"),
        "ambiguous" => Some("ambiguous"),
        "not_submitted" => Some("not_submitted"),
        _ => Some("other_typed_category_suppressed"),
    }
}

/// A scoped, allow-list tracing subscriber, installed only in diagnostic children.
/// It consumes existing production events without changing production sources.
#[derive(Clone, Default)]
pub struct WorkerTrace(Arc<Mutex<Vec<Value>>>, Arc<AtomicUsize>);

struct TraceFields(serde_json::Map<String, Value>);
impl tracing::field::Visit for TraceFields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.record_str(field, &format!("{value:?}"));
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        let value = value.trim_matches('"');
        match field.name() {
            "error" => {
                self.0
                    .insert("error_category".into(), json!(error_category(value)));
                for code in ["5", "517", "6", "262", "1"] {
                    if value.contains(&format!("(code: {code})")) {
                        self.0.insert("sqlite_code".into(), json!(code));
                    }
                }
            }
            "job_id" if safe_identity(value) => {
                self.0.insert("job_id".into(), json!(value));
            }
            "old_state" | "new_state" | "phase" | "outcome"
                if value.len() <= 48
                    && value.bytes().all(|b| b.is_ascii_lowercase() || b == b'_') =>
            {
                self.0.insert(field.name().into(), json!(value));
            }
            "message" => {
                let kind = match value {
                    "pinning job execution failed" => "job_execution_failed",
                    "pinning worker scan failed" => "worker_scan_failed",
                    "pinning transition" => "transition",
                    "pinning durable phase" => "durable_phase",
                    _ => "other_worker_event",
                };
                self.0.insert("kind".into(), json!(kind));
            }
            _ => {}
        }
    }
}
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WorkerTrace {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() != "ipfs_s3_gateway::pinning::worker" {
            return;
        }
        let mut fields = TraceFields(serde_json::Map::new());
        event.record(&mut fields);
        fields.0.insert(
            "observed_utc_us".into(),
            json!(Utc::now().timestamp_micros()),
        );
        fields
            .0
            .insert("source_line".into(), json!(event.metadata().line()));
        fields
            .0
            .insert("level".into(), json!(event.metadata().level().as_str()));
        let mut events = self.0.lock().unwrap();
        if events.len() < 512 {
            events.push(Value::Object(fields.0));
        } else {
            self.1.fetch_add(1, Ordering::Relaxed);
        }
    }
}
impl WorkerTrace {
    pub fn events(&self) -> Vec<Value> {
        self.0.lock().unwrap().clone()
    }
    pub fn dropped(&self) -> usize {
        self.1.load(Ordering::Relaxed)
    }
}

#[derive(Serialize)]
struct Service {
    #[serde(skip)]
    cid: String,
    class: String,
    item: usize,
    ready_us: u64,
    publication_http_us: Option<u128>,
    first_provider_service_us: Option<u64>,
    ready_to_first_provider_service_us: Option<u64>,
    first_successful_provider_response_us: Option<u64>,
    database_pinned_observed_us: Option<u64>,
    ready_to_database_pinned_observed_us: Option<u64>,
}

pub struct Measurements {
    pub pids: Vec<u32>,
    services: Vec<Service>,
    peaks: BTreeMap<&'static str, usize>,
    samples: usize,
    last_sample_us: Option<u64>,
    max_sample_gap_us: u64,
    last: Value,
    pub diagnostic: bool,
    diagnostic_changes: Vec<Value>,
    previous_jobs: BTreeMap<String, Value>,
    pub case: Case,
    observation_start_us: Option<u64>,
    observation_initial_done: usize,
    queue_timeline: Vec<Value>,
    diagnostic_changes_dropped: usize,
    cleanup_request: Option<Value>,
    cleanup_completed_us: Option<u64>,
}

impl Default for Measurements {
    fn default() -> Self {
        Self {
            pids: Vec::new(),
            services: Vec::new(),
            peaks: BTreeMap::new(),
            samples: 0,
            last_sample_us: None,
            max_sample_gap_us: 0,
            last: Value::Null,
            diagnostic: false,
            diagnostic_changes: Vec::new(),
            previous_jobs: BTreeMap::new(),
            case: Case::formal(),
            observation_start_us: None,
            observation_initial_done: 0,
            queue_timeline: Vec::new(),
            diagnostic_changes_dropped: 0,
            cleanup_request: None,
            cleanup_completed_us: None,
        }
    }
}

impl Measurements {
    pub fn begin_observation(&mut self, now: u64) {
        self.observation_start_us = Some(now);
        self.observation_initial_done = self.last["done_submits"].as_u64().unwrap_or(0) as usize;
    }
    pub fn cleanup_ready(&self) -> bool {
        self.last["cleanup_pinned_allocations"] == self.case.providers
    }
    pub fn cleanup_requested(&mut self, ready_us: u64, http_us: u128) {
        self.cleanup_request = Some(json!({"ready_us":ready_us,"publication_http_us":http_us}));
    }
    fn add(&mut self, class: &str, item: usize, ready_us: u64, http_us: Option<u128>) {
        self.services.push(Service {
            cid: http::cid(&http::payload(class, item)),
            class: class.into(),
            item,
            ready_us,
            publication_http_us: http_us,
            first_provider_service_us: None,
            ready_to_first_provider_service_us: None,
            first_successful_provider_response_us: None,
            database_pinned_observed_us: None,
            ready_to_database_pinned_observed_us: None,
        });
    }
    pub fn add_bulk(&mut self, ready_us: u64, http_us: u128) {
        for item in 0..33 {
            self.add("bulk", item, ready_us, Some(http_us));
        }
    }
    pub fn add_diagnostic_bulk(&mut self, count: usize, ready_us: u64, http_us: u128) {
        for item in 0..count {
            self.add("bulk", item, ready_us, Some(http_us));
        }
    }
    pub fn add_recovery(&mut self, ready_us: u64) {
        for item in 0..2 {
            self.add("recovery", item, ready_us, None);
        }
    }
    pub fn add_ordinary(&mut self, item: usize, ready_us: u64, http_us: u128) {
        self.add("ordinary", item, ready_us, Some(http_us));
    }

    pub async fn sample(
        &mut self,
        store: &Store,
        _allocation: &str,
        fixture: &Fixture,
    ) -> BenchResult<()> {
        let txn = store
            .db()
            .begin()
            .await
            .map_err(|_| "queue observation transaction failed")?;
        let jobs = pin_job::Entity::find()
            .limit(60_001)
            .all(&txn)
            .await
            .map_err(|_| "durable queue observation failed")?;
        let remotes = remote_pin::Entity::find()
            .limit(12_001)
            .all(&txn)
            .await
            .map_err(|_| "remote progress observation failed")?;
        let targets = pin_lease_target::Entity::find()
            .limit(22_001)
            .all(&txn)
            .await
            .map_err(|_| "logical occupancy observation failed")?;
        let usage = pin_provider_usage::Entity::find()
            .limit(3)
            .all(&txn)
            .await
            .map_err(|_| "shared usage observation failed")?;
        let evidence = remote_pin_ledger::Entity::find()
            .limit(12_001)
            .all(&txn)
            .await
            .map_err(|_| "captured ownership observation failed")?;
        let histories = jobs::history::Entity::find()
            .limit(60_001)
            .all(&txn)
            .await
            .map_err(|_| "historical invocation progress observation failed")?;
        let captured_routes = if self.diagnostic && !self.case.mixed {
            pin_invocation_route::Entity::find()
                .limit(2049)
                .all(&txn)
                .await
                .map_err(|_| "captured invocation route observation failed")?
        } else {
            Vec::new()
        };
        let lease_rows = if self.diagnostic && !self.case.mixed {
            pin_lease::Entity::find()
                .limit(101)
                .all(&txn)
                .await
                .map_err(|_| "owner generation observation failed")?
        } else {
            Vec::new()
        };
        if jobs.len() > if self.case.mixed { 60_000 } else { 2048 }
            || targets.len() > if self.case.mixed { 22_000 } else { 100 }
            || remotes.len() > if self.case.mixed { 12_000 } else { 100 }
            || histories.len() > 60_000
            || evidence.len() > 12_000
            || usage.len() > self.case.providers
        {
            return Err("bounded fixture database row limit exceeded");
        }
        let now = fixture.now_us();
        let mut unfinished_details = Vec::new();
        if self.diagnostic && !self.case.mixed {
            let mut snapshots = BTreeMap::new();
            for job in &jobs {
                let identity = fixture.identity(&job.cid);
                let remote = remotes
                    .iter()
                    .find(|r| r.cid == job.cid && r.provider == job.provider);
                let history = histories.iter().find(|h| h.job_id == job.id);
                let captured = captured_routes.iter().find(|r| r.job_id == job.id);
                let route = captured.and_then(|r| {
                    serde_json::from_str::<
                            ipfs_s3_gateway::pinning::identity::ProviderRouteSnapshot,
                        >(&r.route)
                        .ok()
                });
                let owner_lease = job
                    .lease_id
                    .as_ref()
                    .and_then(|id| lease_rows.iter().find(|l| &l.id == id));
                let key = (job.provider.clone(), job.cid.clone());
                if !snapshots.contains_key(&key) {
                    let snapshot = leases::remote_work_snapshot(&txn, &job.provider, &job.cid)
                        .await
                        .map_err(|_| "canonical desired owner observation failed")?;
                    snapshots.insert(key.clone(), snapshot);
                }
                let canonical = snapshots
                    .get(&key)
                    .and_then(|s| s.as_ref())
                    .and_then(|s| s.desired.first());
                let target = targets
                    .iter()
                    .find(|t| Some(&t.id) == job.target_id.as_ref());
                let detail = json!({"job_id":job.id,"allocation":job.provider,"class_item":identity,"operation":job.operation,"state":job.state,"submit_phase":job.submit_phase,
                    "locked_until_utc_us":job.locked_until.map(|t| t.timestamp_micros()),"updated_at_utc_us":job.updated_at.timestamp_micros(),
                    "next_attempt_at_utc_us":job.next_attempt_at.timestamp_micros(),"attempts":job.attempts,"expected_generation":job.expected_generation,
                    "expected_remote_epoch":job.expected_remote_epoch,"actual_remote_epoch":remote.map(|r| r.epoch),"remote_status":remote.map(|r| &r.status),
                    "lease_id":job.lease_id,"target_id":job.target_id,"target_state":target.map(|t| &t.state),
                    "owner_lease_generation":owner_lease.map(|l| l.generation),"owner_lease_state":owner_lease.map(|l| &l.state),
                    "canonical_desired_target":canonical.map(|d| &d.target_id),"canonical_desired_lease":canonical.map(|d| &d.lease_id),
                    "canonical_generation":canonical.map(|d| d.generation),"is_canonical_owner":canonical.map(|d| Some(&d.target_id)==job.target_id.as_ref()),
                    "captured_invocation_epoch":captured.map(|r| r.remote_epoch),
                    "captured_route":route.as_ref().map(|r| json!({"provider_id":r.provider_id,"backend":r.backend,"api_profile":r.api_profile,"strategy":r.strategy,"credential_revision":r.credential_revision,"endpoint_revision":r.endpoint_revision})),
                    "invocation_api":history.map(|h| &h.api),"invocation_strategy":history.map(|h| &h.strategy),
                    "invocation_effect":history.map(|h| &h.effect),"invocation_state":history.map(|h| &h.state),
                    "invocation_correlation":history.and_then(|h| h.correlation.as_deref()).filter(|v| safe_identity(v)),
                    "typed_remote_error_class":typed_provider_category(remote.and_then(|r| r.last_error_class.as_deref())),
                    "last_error_category":job.last_error.as_deref().map(error_category),"actual_submit_calls":history.map(|h| h.submit_calls),
                    "actual_recovery_queries":history.map(|h| h.recovery_queries)});
                if job.state != "done" {
                    unfinished_details.push(detail.clone());
                }
                if self.previous_jobs.get(&job.id) != Some(&detail) {
                    self.previous_jobs.insert(job.id.clone(), detail.clone());
                    if self.diagnostic_changes.len() < 4096 {
                        self.diagnostic_changes.push(json!({"observed_parent_us":now,"observed_utc_us":Utc::now().timestamp_micros(),"job":detail}));
                    } else {
                        self.diagnostic_changes_dropped += 1;
                    }
                }
            }
        }
        txn.commit()
            .await
            .map_err(|_| "queue observation commit failed")?;
        if let Some(previous) = self.last_sample_us {
            self.max_sample_gap_us = self.max_sample_gap_us.max(now.saturating_sub(previous));
        }
        self.last_sample_us = Some(now);
        self.samples += 1;
        let pending = jobs.iter().filter(|j| j.state == "pending").count();
        let running = jobs.iter().filter(|j| j.state == "running").count();
        let parked = jobs
            .iter()
            .filter(|j| j.state == "running" && j.locked_until.is_none())
            .count();
        let ready = jobs
            .iter()
            .filter(|j| j.state == "pending" && j.next_attempt_at <= Utc::now())
            .count();
        for (key, value) in [
            ("pending", pending),
            ("running", running),
            ("parked", parked),
            ("pending_due", ready),
            ("outstanding", pending + running),
        ] {
            let peak = self.peaks.entry(key).or_default();
            *peak = (*peak).max(value);
        }
        let events = fixture.events();
        let mut service_events: BTreeMap<(&str, usize), Vec<&Event>> = BTreeMap::new();
        for event in events.iter().filter(|e| e.process < 2) {
            service_events
                .entry((&event.class, event.item))
                .or_default()
                .push(event);
        }
        let mut pinned_by_cid: BTreeMap<&str, usize> = BTreeMap::new();
        for remote in remotes.iter().filter(|r| r.status == "pinned") {
            *pinned_by_cid.entry(&remote.cid).or_default() += 1;
        }
        for service in &mut self.services {
            let matching = service_events
                .get(&(service.class.as_str(), service.item))
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            if let Some(first) = matching.first() {
                service.first_provider_service_us = Some(first.start_us);
                service.ready_to_first_provider_service_us =
                    Some(first.start_us.saturating_sub(service.ready_us));
            }
            service.first_successful_provider_response_us = matching
                .iter()
                .filter(|e| e.status == 200)
                .find_map(|e| e.end_us);
            if service.database_pinned_observed_us.is_none()
                && pinned_by_cid
                    .get(service.cid.as_str())
                    .copied()
                    .unwrap_or(0)
                    == self.case.providers
            {
                service.database_pinned_observed_us = Some(now);
                service.ready_to_database_pinned_observed_us =
                    Some(now.saturating_sub(service.ready_us));
            }
        }
        let mut states = BTreeMap::new();
        for job in &jobs {
            let state = if ["pending", "running", "done"].contains(&job.state.as_str()) {
                job.state.as_str()
            } else {
                "unrecognized"
            };
            let phase = match job.submit_phase.as_deref() {
                Some("ready" | "calling" | "recovering" | "recovery_backoff") => {
                    job.submit_phase.as_deref().unwrap()
                }
                None => "none",
                _ => "unrecognized",
            };
            *states.entry(format!("{state}/{phase}")).or_insert(0usize) += 1;
        }
        let (reserved_pins, reserved_bytes) =
            usage.iter().fold((0i64, 0i64), |(pins, bytes), u| {
                (pins + u.reserved_pins, bytes + u.reserved_bytes)
            });
        let mut recovery_history = Vec::new();
        for item in 0..2 {
            let cid = http::cid(&http::payload("recovery", item));
            for provider in usage.iter().map(|u| &u.provider) {
                let job = jobs
                    .iter()
                    .find(|j| j.cid == cid && &j.provider == provider && j.operation == "submit");
                let history = job.and_then(|j| histories.iter().find(|h| h.job_id == j.id));
                recovery_history.push(json!({"item":item,"allocation":provider,"actual_submit_calls":history.map(|h| h.submit_calls),"actual_recovery_queries":history.map(|h| h.recovery_queries),
                "job_done":job.is_some_and(|j| j.state == "done"),"unknown_effect_still_held":history.is_some_and(|h| h.effect == "unknown")}));
            }
        }
        let attention = jobs
            .iter()
            .filter(|j| {
                j.last_error.as_deref().is_some_and(|e| {
                    e.contains("needs_attention") || e.contains("operator attention")
                })
            })
            .count();
        let cleanup_cid = http::cid(&http::payload("cleanup", 0));
        let cleanup_targets_released = targets
            .iter()
            .filter(|t| t.cid == cleanup_cid && t.state == "released")
            .count();
        let cleanup_retained = evidence
            .iter()
            .filter(|r| r.cid == cleanup_cid && r.effect == "retained")
            .count();
        let cleanup_done = jobs
            .iter()
            .filter(|j| j.cid == cleanup_cid && j.operation == "unpin" && j.state == "done")
            .count();
        if self.cleanup_request.is_some()
            && self.cleanup_completed_us.is_none()
            && cleanup_targets_released == self.case.providers
            && cleanup_retained == self.case.providers
            && cleanup_done == self.case.providers
        {
            self.cleanup_completed_us = Some(now);
        }
        let provider_progress = usage.iter().map(|u| json!({"allocation":u.provider,"reserved_pins":u.reserved_pins,"reserved_bytes":u.reserved_bytes,
            "pinned_allocations":remotes.iter().filter(|r| r.provider == u.provider && r.status == "pinned").count(),
            "unfinished_submits":jobs.iter().filter(|j| j.provider == u.provider && j.operation == "submit" && j.state != "done").count()})).collect::<Vec<_>>();
        self.last = json!({"jobs":jobs.len(), "job_state_and_submit_phase":states, "pending":pending, "running":running, "parked":parked,
            "provider_progress":provider_progress,"done_submits":jobs.iter().filter(|j| j.operation == "submit" && j.state == "done").count(),
            "bulk_pinned_targets":targets.iter().filter(|t| fixture.identity(&t.cid).is_some_and(|(class,_)| class == "bulk") && t.state == "pinned").count(),
            "cleanup_pinned_allocations":remotes.iter().filter(|r| r.cid == cleanup_cid && r.status == "pinned").count(),
            "cleanup_released_targets":cleanup_targets_released,"cleanup_retained_allocations":cleanup_retained,"cleanup_done_jobs":cleanup_done,
            "jobs_with_safe_error_present":jobs.iter().filter(|j| j.last_error.is_some()).count(), "max_attempts_observed":jobs.iter().map(|j| j.attempts).max().unwrap_or(0),
            "jobs_with_operator_attention_category":attention,"recovery_history":recovery_history,
            "ledger_rows":evidence.len(),"ledger_captured_routes":evidence.iter().filter(|r| r.route.is_some()).count(),
            "ledger_application_created":evidence.iter().filter(|r| r.ownership == "application_created").count(),
            "ledger_unknown_effect":evidence.iter().filter(|r| r.effect == "unknown").count(),"ledger_confirmed_effect":evidence.iter().filter(|r| r.effect == "confirmed").count(),
            "unfinished_submits":jobs.iter().filter(|j| j.operation == "submit" && j.state != "done").count(),
            "physical_allocations":remotes.len(), "pinned_allocations":remotes.iter().filter(|r| r.status == "pinned").count(),
            "logical_targets":targets.len(), "logical_target_bytes":targets.iter().map(|t| t.logical_size).sum::<i64>(),
            "quota_blocked_targets":targets.iter().filter(|t| t.state == "quota_blocked").count(), "reserved_pins":reserved_pins, "reserved_bytes":reserved_bytes,
            "diagnostic_unfinished_jobs":unfinished_details});
        if self.queue_timeline.len() < 512
            && self.queue_timeline.last().is_none_or(|v| {
                now.saturating_sub(v["observed_us"].as_u64().unwrap_or(0)) >= 250_000
            })
        {
            self.queue_timeline.push(json!({"observed_us":now,"pending":pending,"running":running,"pending_due":ready,"done_submits":self.last["done_submits"],
                "pinned_allocations":self.last["pinned_allocations"],"recovery_history":self.last["recovery_history"],"cleanup_retained_allocations":cleanup_retained}));
        }
        Ok(())
    }

    pub fn settled(&self) -> bool {
        self.last["pinned_allocations"] == 39
            && self.last["unfinished_submits"] == 0
            && self.services.len() == 39
    }
    pub fn diagnostic_settled(&self) -> bool {
        self.last["pinned_allocations"] == self.case.expected_allocations()
            && self.last["unfinished_submits"] == 0
            && self.services.len() == self.case.distinct + 6
    }
    pub fn mixed_settled(&self) -> bool {
        self.last["pinned_allocations"].as_u64().unwrap_or(0)
            >= ((self.case.distinct + 6) * self.case.providers) as u64
            && self.last["unfinished_submits"] == 0
            && self.services.len() == self.case.distinct + 6
            && self.last["bulk_pinned_targets"] == self.case.outputs * self.case.providers
            && self.cleanup_completed_us.is_some()
    }
    pub fn diagnostic_changes(&self) -> Value {
        json!({"events":self.diagnostic_changes,"capacity":4096,"dropped":self.diagnostic_changes_dropped})
    }

    pub fn report(&self, fixture: &Fixture, seconds: u64) -> Value {
        let latency_summary = |mut values: Vec<u128>| {
            values.sort_unstable();
            let percentile = |p: usize| {
                values
                    .get(values.len().saturating_sub(1) * p / 100)
                    .copied()
            };
            json!({"observed_count":values.len(),"p50_us":percentile(50),"p95_us":percentile(95),"max_us":values.last()})
        };
        let events = fixture
            .events()
            .into_iter()
            .filter(|e| e.process < 2)
            .collect::<Vec<_>>();
        let pace_combined = pace(&events);
        let pace_per_process = (0..self.case.processes)
            .map(|process| {
                pace(
                    &events
                        .iter()
                        .filter(|e| e.process == process)
                        .cloned()
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        let retries = events.iter().filter(|e| e.status == 429).map(|rejected| {
            let next = events.iter().find(|next| next.start_us > rejected.start_us && next.provider == rejected.provider && next.correlation == rejected.correlation && next.class == rejected.class && next.item == rejected.item && next.method == rejected.method);
            json!({"class":rejected.class,"item":rejected.item,"provider":rejected.provider,"correlation":rejected.correlation,"process":rejected.process,"response_end_us":rejected.end_us,
                "retry_after_seconds":rejected.retry_after_seconds,"next_process":next.map(|e| e.process),
                "actual_response_to_next_start_us":rejected.end_us.zip(next.map(|e| e.start_us)).map(|(end,start)| start.saturating_sub(end))})
        }).collect::<Vec<_>>();
        let progress = ["bulk", "ordinary", "recovery"].map(|class| {
            let rows = self.services.iter().filter(|s| s.class == class).collect::<Vec<_>>();
            json!({"class":class,"distinct_items":rows.len(),"actually_serviced":rows.iter().filter(|s| s.first_provider_service_us.is_some()).count(),
                "database_pinned_observed":rows.iter().filter(|s| s.database_pinned_observed_us.is_some()).count()})
        });
        let overlapping_foreground = self
            .services
            .iter()
            .filter(|s| {
                s.class == "ordinary"
                    && s.publication_http_us.is_some_and(|duration| {
                        let end = s.ready_us.saturating_add(duration as u64);
                        events.iter().any(|e| {
                            e.start_us < end
                                && e.end_us.is_none_or(|finished| finished > s.ready_us)
                        })
                    })
            })
            .count();
        let counts = fixture.counts();
        let exceeds = counts["actual_peak_in_flight_combined"]
            .as_u64()
            .unwrap_or(0)
            > W as u64
            || pace_combined["peak_starts_in_rolling_second"]
                .as_u64()
                .unwrap_or(0)
                > RPS as u64;
        let context = json!({"stage":8, "kind":"offline_test_executable_children_not_production_main", "database":"new_owned_file_sqlite", "children":self.pids,
            "shared_backend":"filebase", "shared_scope":if self.case.providers == 1 {"one_run_owned_simulated_account"} else {"two_run_owned_simulated_accounts_in_distinct_storage_domains"}, "fake_tokens":"client_attribution_only; account identity comes from route, not token",
            "configured_per_process_worker_concurrency":W,"configured_per_process_requests_per_second":RPS,"observation_safety_bound_seconds":seconds});
        let workload = json!({
             "configured_workload":{"batch_logical_outputs":self.case.outputs,"batch_distinct_contents":self.case.distinct,"ordinary_publications":4,"interrupted_historical_submissions":2*self.case.providers,"cleanup_publications":usize::from(self.case.mixed)},
             "actual_batch_outputs_acknowledged":if self.services.iter().any(|s| s.class == "bulk") {self.case.outputs} else {0},
            "actual_ordinary_publications_acknowledged":self.services.iter().filter(|s| s.class == "ordinary").count(),
            "ordinary_publications_overlapping_actual_provider_http":overlapping_foreground,
            "ordinary_foreground_publication_latency":latency_summary(self.services.iter().filter(|s| s.class == "ordinary").filter_map(|s|s.publication_http_us).collect()),
            "ordinary_ready_to_all_replicas_pinned_latency":latency_summary(self.services.iter().filter(|s| s.class == "ordinary").filter_map(|s|s.ready_to_database_pinned_observed_us.map(u128::from)).collect())});
        let provider_http = json!({"http_fixture":counts,"aggregate_exceeds_a_single_process_budget_observed":exceeds,
            "actual_pace_combined":pace_combined,"actual_pace_per_process":pace_per_process,
            "actual_pace_per_provider":(0..self.case.providers).map(|provider|json!({"provider":provider,"pace":pace(&events.iter().filter(|e| e.provider==provider).cloned().collect::<Vec<_>>())})).collect::<Vec<_>>(),
            "http_429_count":retries.len(),"retry_after_observations":retries,"provider_http_timeline":events});
        let queue = json!({"queue_peak_is_sampled_not_an_exact_high_watermark":true,"queue_sample_target_interval_us":if self.case.outputs==10_000 {250000} else {25000},"queue_sample_count":self.samples,"actual_max_queue_sample_gap_us":self.max_sample_gap_us,"sampled_queue_peaks":self.peaks,
            "database_observation_row_bounds":{"jobs":if self.case.mixed {60000} else {2048},"targets":if self.case.mixed {22000} else {100},"remotes":if self.case.mixed {12000} else {100},"history":60000,"ledger":12000},
              "final_durable_snapshot":self.last,"actual_class_progress":progress,"actual_service_latencies":self.services,
              "queue_progress_timeline":self.queue_timeline});
        let completion = json!({"workload_completed":if self.case.mixed {self.mixed_settled()} else if self.case.diagnostic {self.diagnostic_settled()} else {self.settled()},
              "done_submit_job_throughput_per_observation_second":self.observation_start_us.zip(self.last_sample_us).filter(|(start,end)|end>start).map(|(start,end)|
                  (self.last["done_submits"].as_u64().unwrap_or(0).saturating_sub(self.observation_initial_done as u64)) as f64 * 1_000_000.0 / (end-start) as f64),
              "all_replicas_pinned_distinct_content_throughput_per_observation_second":self.observation_start_us.zip(self.last_sample_us).filter(|(start,end)|end>start).map(|(start,end)|
                  self.services.iter().filter(|s|s.database_pinned_observed_us.is_some()).count() as f64 * 1_000_000.0 / (end-start) as f64),
              "logical_cleanup":{"request":self.cleanup_request,"retained_and_released_observed_us":self.cleanup_completed_us,"physical_unpin_enabled":false}});
        let evidence = json!({"limits_of_evidence":"loopback PSA simulator, production AppState/worker in test-executable children, not main.rs, real remote accounts, HA, or cross-process global-budget implementation",
            "latency_clock":"parent_monotonic_microseconds; bulk/ordinary ready is signed PUT start, recovery ready is expiry injection start; DB completion is first sampled observation",
            "in_flight_definition":"actual provider handler entry through response construction or cancellation, not claimed jobs or semaphore occupancy; timeline status 0 means no response constructed",
             "no_throughput_slo_asserted":true,"no_physical_space_release_claim":true});
        let mut report = serde_json::Map::new();
        for section in [
            context,
            workload,
            provider_http,
            queue,
            completion,
            evidence,
        ] {
            let Value::Object(fields) = section else {
                unreachable!("report sections are objects")
            };
            report.extend(fields);
        }
        Value::Object(report)
    }

    pub async fn verify(
        &self,
        store: &Store,
        allocation: &str,
        fixture: &Fixture,
        recovery: &[jobs::ClaimedPinJob],
        processes: usize,
    ) -> BenchResult<()> {
        if !self.settled()
            || self.last["logical_targets"] != 71
            || self.last["logical_target_bytes"] != 2272
            || self.last["reserved_pins"] != 39
            || self.last["reserved_bytes"] != 1248
            || self.last["quota_blocked_targets"] != 0
        {
            return Err("shared logical/physical occupancy invariant failed");
        }
        let rows = remote_pin_ledger::Entity::find()
            .filter(remote_pin_ledger::Column::Provider.eq(allocation))
            .all(store.db())
            .await
            .map_err(|_| "ledger ownership verification failed")?;
        let registered = pin_provider_route::Entity::find_by_id(allocation.to_owned())
            .one(store.db())
            .await
            .map_err(|_| "registered route verification failed")?
            .ok_or("registered route missing")?;
        let route =
            serde_json::from_str::<ipfs_s3_gateway::pinning::identity::ProviderRouteSnapshot>(
                &registered.snapshot,
            )
            .map_err(|_| "registered route snapshot invalid")?;
        if rows.len() != 39
            || rows
                .iter()
                .any(|row| !psa_ledger_row_matches_contract(row, &route))
        {
            return Err("captured route/ownership/effect fence invariant failed");
        }
        for claim in recovery {
            let history = jobs::submission_history(store.db(), &claim.model.id)
                .await
                .map_err(|_| "historical recovery evidence unavailable")?
                .ok_or("historical recovery evidence missing")?;
            if history.submit_calls != 1 || history.recovery_queries < 1 {
                return Err("historical recovery repeated an uncertain POST or made no progress");
            }
        }
        let report = self.report(fixture, 0);
        if report["http_fixture"]["protocol_errors"] != 0
            || (processes == 2
                && (report["http_429_count"] == 0
                    || report["aggregate_exceeds_a_single_process_budget_observed"] != true))
        {
            return Err(
                "actual provider evidence did not demonstrate the multi-process budget gap",
            );
        }
        let retries = report["retry_after_observations"]
            .as_array()
            .ok_or("retry observations missing")?;
        if (processes == 2
            && !retries
                .iter()
                .any(|r| r["actual_response_to_next_start_us"].is_u64()))
            || retries.iter().any(|r| {
                r["actual_response_to_next_start_us"]
                    .as_u64()
                    .is_some_and(|gap| gap < 900_000)
            })
        {
            return Err("actual Retry-After follow-up was absent or prematurely serviced");
        }
        let counts = fixture.counts();
        if counts["actual_peak_in_flight_combined"]
            .as_u64()
            .unwrap_or(0)
            > (processes * W) as u64
            || counts["actual_peak_in_flight_per_process"]
                .as_array()
                .is_none_or(|a| a.iter().any(|n| n.as_u64().unwrap_or(u64::MAX) > W as u64))
        {
            return Err("actual provider in-flight count exceeded production process-local bounds");
        }
        let events = fixture.events();
        if (0..processes).any(|process| !events.iter().any(|e| e.process == process))
            || events
                .iter()
                .any(|e| e.process < 2 && e.class == "recovery" && e.method == "POST")
        {
            return Err("both OS workers must serve HTTP without resubmitting uncertain recovery");
        }
        Ok(())
    }

    pub async fn verify_mixed(
        &self,
        fixture: &Fixture,
        recovery: &[jobs::ClaimedPinJob],
        store: &Store,
    ) -> BenchResult<()> {
        if self.last["logical_targets"] != (self.case.outputs + 7) * self.case.providers
            || self.last["physical_allocations"] != self.case.expected_allocations()
            || self.last["reserved_pins"].as_i64().unwrap_or(i64::MAX)
                > 6000 * self.case.providers as i64
            || self.last["reserved_bytes"].as_i64().unwrap_or(i64::MAX)
                > 192000 * self.case.providers as i64
            || self.last["quota_blocked_targets"] != 0
        {
            return Err("mixed logical/physical publication or capacity bound failed");
        }
        if self
            .services
            .iter()
            .filter(|s| s.class == "ordinary" && s.database_pinned_observed_us.is_some())
            .count()
            != 4
            || self
                .services
                .iter()
                .filter(|s| s.class == "recovery" && s.database_pinned_observed_us.is_some())
                .count()
                != 2
            || self.cleanup_completed_us.is_none()
            || (self.case.outputs == 65 && !self.mixed_settled())
        {
            return Err(
                "bounded mixed case did not demonstrate foreground, recovery, logical cleanup or required 65-output completion",
            );
        }
        for claim in recovery {
            let history = jobs::submission_history(store.db(), &claim.model.id)
                .await
                .map_err(|_| "mixed recovery observation failed")?
                .ok_or("mixed recovery history missing")?;
            if history.submit_calls != 1
                || history.recovery_queries < 1
                || history.state != "settled"
            {
                return Err("mixed recovery resubmitted uncertain work or did not settle");
            }
        }
        let counts = fixture.counts();
        let events = fixture.events();
        let report = self.report(fixture, 0);
        if counts["protocol_errors"] != 0
            || counts["resource_bound_rejections"] != 0
            || counts["actual_peak_in_flight_combined"]
                .as_u64()
                .unwrap_or(u64::MAX)
                > (self.case.processes * W) as u64
            || counts["actual_peak_in_flight_per_process"]
                .as_array()
                .is_none_or(|a| a.iter().any(|v| v.as_u64().unwrap_or(u64::MAX) > W as u64))
            || report["ordinary_publications_overlapping_actual_provider_http"] == 0
            || report["retry_after_observations"]
                .as_array()
                .is_none_or(|a| {
                    a.iter().any(|r| {
                        r["actual_response_to_next_start_us"]
                            .as_u64()
                            .is_some_and(|gap| gap < 900_000)
                    })
                })
            || (0..self.case.processes).any(|p| !events.iter().any(|e| e.process == p))
            || (0..self.case.providers)
                .any(|p| !events.iter().any(|e| e.provider == p && e.process < 2))
            || events
                .iter()
                .any(|e| e.process < 2 && e.class == "recovery" && e.method == "POST")
        {
            return Err("mixed provider/resource/uncertain invocation safety invariant failed");
        }
        Ok(())
    }
}

fn pace(events: &[Event]) -> Value {
    let times = events.iter().map(|e| e.start_us).collect::<Vec<_>>();
    let mut gaps = times
        .windows(2)
        .map(|p| p[1].saturating_sub(p[0]))
        .collect::<Vec<_>>();
    gaps.sort_unstable();
    let peak = times
        .iter()
        .enumerate()
        .map(|(index, time)| {
            times[index..]
                .iter()
                .take_while(|next| next.saturating_sub(*time) < 1_000_000)
                .count()
        })
        .max()
        .unwrap_or(0);
    let percentile = |percent: usize| {
        gaps.get(gaps.len().saturating_sub(1) * percent / 100)
            .copied()
    };
    let window_us = times
        .first()
        .zip(events.iter().filter_map(|e| e.end_us).max())
        .map(|(first, last)| last.saturating_sub(*first));
    let rate = |count: usize| {
        window_us
            .filter(|span| *span > 0)
            .map(|span| count as f64 * 1_000_000.0 / span as f64)
    };
    json!({"requests":events.len(),"minimum_start_gap_us":gaps.first(),"p50_start_gap_us":percentile(50),"p95_start_gap_us":percentile(95),"peak_starts_in_rolling_second":peak,
        "first_start_us":times.first(),"last_start_us":times.last(),"completed_handler_window_us":window_us,
        "actual_completed_requests_per_second_over_handler_window":rate(events.iter().filter(|e| e.end_us.is_some() && e.status != 0).count()),
        "actual_successful_responses_per_second_over_handler_window":rate(events.iter().filter(|e| e.end_us.is_some() && e.status == 200).count())})
}

/// Prepare a real claimed invocation with production route/epoch/ownership
/// capture, perform an actual fixture POST, then intentionally omit its receipt.
/// This models a crash after acceptance; it is not a fabricated Find response or
/// an RPC unknown upload downgraded to a safe retry. Only the PSA path is exercised.
pub async fn seed_interrupted_calls(
    store: &Store,
    pinning: &Arc<PinningCoordinator>,
) -> BenchResult<Vec<jobs::ClaimedPinJob>> {
    let count = 2 * pinning.effective_config().providers.len();
    let claims = jobs::claim_due_jobs(
        store.db(),
        Utc::now(),
        chrono::Duration::seconds(90),
        count as u64,
    )
    .await
    .map_err(|_| "historical fixture claim failed")?;
    if claims.len() != count
        || claims
            .iter()
            .any(|c| !(0..2).any(|i| c.model.cid == http::cid(&http::payload("recovery", i))))
    {
        return Err("historical fixture claimed unexpected work");
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|_| "historical fixture HTTP client failed")?;
    for claim in &claims {
        let endpoint = pinning
            .effective_config()
            .providers
            .iter()
            .find(|p| p.name == claim.model.provider)
            .and_then(|p| p.endpoint.as_ref())
            .ok_or("historical fixture actual route missing")?;
        let txn = store
            .db()
            .begin()
            .await
            .map_err(|_| "historical invocation transaction failed")?;
        if jobs::prepare_submit_call(&txn, claim, Utc::now())
            .await
            .map_err(|_| "historical invocation preparation failed")?
            != jobs::SubmitCallDecision::ReadyToCall
        {
            return Err("historical invocation was not eligible");
        }
        jobs::record_submit_invocation(&txn, claim, "psa", "cid", Utc::now())
            .await
            .map_err(|_| "historical route/ownership capture failed")?;
        txn.commit()
            .await
            .map_err(|_| "historical invocation commit failed")?;
        let response = client.post(format!("{endpoint}/pins")).bearer_auth("offline-fixture-seed").json(&json!({"cid":claim.model.cid,"name":"offline interrupted invocation","meta":{"gateway_job_id":claim.model.id,"gateway_lease_id":claim.model.lease_id,"gateway_target_id":claim.model.target_id}})).send().await.map_err(|_| "historical fixture POST failed")?;
        if response.status() != reqwest::StatusCode::OK {
            return Err("historical fixture POST was not accepted");
        }
        // Deliberately do not apply this accepted receipt to the DB.
    }
    Ok(claims)
}

pub async fn expire_interrupted_claims(
    store: &Store,
    recovery: &[jobs::ClaimedPinJob],
) -> BenchResult<()> {
    let txn = store
        .db()
        .begin()
        .await
        .map_err(|_| "historical crash injection transaction failed")?;
    for claim in recovery {
        let changed = pin_job::Entity::update_many()
            .col_expr(
                pin_job::Column::LockedUntil,
                Expr::value(Some(Utc::now() - chrono::Duration::seconds(1))),
            )
            .filter(pin_job::Column::Id.eq(&claim.model.id))
            .filter(pin_job::Column::State.eq("running"))
            .filter(pin_job::Column::SubmitPhase.eq("calling"))
            .filter(pin_job::Column::LockedUntil.eq(claim.model.locked_until))
            .exec(&txn)
            .await
            .map_err(|_| "historical claim expiry injection failed")?;
        if changed.rows_affected != 1 {
            return Err("historical claim ownership fence changed before crash injection");
        }
    }
    txn.commit()
        .await
        .map_err(|_| "historical crash injection commit failed")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn services(case: Case) -> Measurements {
        let mut measurements = Measurements {
            case,
            ..Default::default()
        };
        measurements.add_diagnostic_bulk(case.distinct, 0, 1);
        measurements.add_recovery(0);
        for item in 0..4 {
            measurements.add_ordinary(item, 0, 1);
        }
        measurements
    }

    #[test]
    fn formal_completion_requires_done_submits_even_if_all_content_is_pinned() {
        let mut measurements = services(Case::formal());
        measurements.last = json!({"pinned_allocations":39,"unfinished_submits":1,"running":1});
        assert!(!measurements.settled());
        measurements.last["unfinished_submits"] = json!(0);
        measurements.last["running"] = json!(0);
        assert!(measurements.settled());
    }

    #[test]
    fn equal_scale_diagnostics_require_their_full_allocation_count() {
        for name in ["equal-duplicate-cids", "equal-unique-cids"] {
            let case = Case::named(name).unwrap();
            let mut measurements = services(case);
            measurements.last =
                json!({"pinned_allocations":case.expected_allocations()-1,"unfinished_submits":0});
            assert!(!measurements.diagnostic_settled());
            measurements.last["pinned_allocations"] = json!(case.expected_allocations());
            assert!(measurements.diagnostic_settled());
            measurements.last["unfinished_submits"] = json!(1);
            assert!(!measurements.diagnostic_settled());
        }
    }

    #[test]
    fn mixed_completion_requires_all_outputs_replicas_and_logical_cleanup() {
        for name in ["dual-provider-65", "dual-provider-10k"] {
            let case = Case::named(name).unwrap();
            let mut measurements = services(case);
            measurements.last = json!({"pinned_allocations":(case.distinct+6)*case.providers,"unfinished_submits":0,"bulk_pinned_targets":case.outputs*case.providers});
            assert!(!measurements.mixed_settled());
            measurements.cleanup_completed_us = Some(1);
            assert!(measurements.mixed_settled());
            measurements.last["bulk_pinned_targets"] = json!(case.outputs * case.providers - 1);
            assert!(!measurements.mixed_settled());
            measurements.last["bulk_pinned_targets"] = json!(case.outputs * case.providers);
            measurements.last["unfinished_submits"] = json!(1);
            assert!(!measurements.mixed_settled());
        }
    }
}
