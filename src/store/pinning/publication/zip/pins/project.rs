use super::*;

impl PinWriteSet {
    pub(super) fn project(
        &mut self,
        keys: &AllocationKeys,
        now: DateTime<Utc>,
        work: DecodeWork<'_>,
    ) -> AppResult<()> {
        for pair in self.ended.clone() {
            let remote = &self.remotes[&pair];
            let active = self.targets.values().any(|t| {
                (t.provider.as_str(), t.cid.as_str()) == (pair.0.as_str(), pair.1.as_str())
                    && desired(&t.state)
                    && self.leases[&t.lease_id].state == "active"
            });
            let job = if active && remote.status != "absent" {
                Some(jobs::reconcile_job(&pair.0, &pair.1, remote.epoch, now))
            } else if !active && (remote.status != "absent" || remote.request_id.is_some()) {
                Some(jobs::unpin_job(&pair.0, &pair.1, remote.epoch, now))
            } else {
                None
            };
            if let Some(job) = job {
                self.ensure_job(job, now, work)?;
            }
        }
        for pair in self.affected.clone() {
            let mut ids = self
                .targets
                .values()
                .filter(|t| {
                    (t.provider.as_str(), t.cid.as_str()) == (pair.0.as_str(), pair.1.as_str())
                        && desired(&t.state)
                        && self.leases[&t.lease_id].state == "active"
                })
                .map(|t| (t.created_at, t.id.clone()))
                .collect::<Vec<_>>();
            ids.sort();
            let canonical = &self.targets[&ids.first().ok_or_else(invalid)?.1];
            let lease = &self.leases[&canonical.lease_id];
            let owner = (lease.id.clone(), canonical.id.clone(), lease.generation);
            let remote = self.remotes.get_mut(&pair).ok_or_else(invalid)?;
            remote.last_touched_at = ids
                .iter()
                .map(|(_, id)| self.targets[id].last_touched_at)
                .max()
                .ok_or_else(invalid)?;
            if remote.status == "failed" {
                remote.failure_attempts = 0;
                remote.next_retry_at = Some(now + chrono::TimeDelta::seconds(30));
                remote.last_touched_at = now;
            }
            let cluster = keys.routes[&pair.0].resource_type()
                == crate::pinning::identity::RemoteResourceType::ClusterPin;
            for (_, id) in &ids {
                let target = self.targets.get_mut(id).ok_or_else(invalid)?;
                let confirmed = self
                    .before
                    .cluster_confirmed
                    .contains(&(target.lease_id.clone(), pair.clone()));
                target.state = match remote.status.as_str() {
                    "pinned" if !cluster || confirmed => "pinned",
                    "pinned" => "submitted",
                    "queued" | "pinning" if remote.request_id.is_some() => {
                        if cluster && confirmed {
                            "pinned"
                        } else {
                            "submitted"
                        }
                    }
                    "reserved" if remote.request_id.is_none() => "waiting",
                    "failed" => "degraded",
                    _ => return Err(invalid()),
                }
                .into();
            }
            let job = match remote.status.as_str() {
                "pinned" if !cluster || remote.request_id.is_none() => None,
                "pinned" | "queued" | "pinning" => Some(jobs::poll_job(
                    &pair.0,
                    &pair.1,
                    &owner.0,
                    &owner.1,
                    owner.2,
                    remote.request_id.as_deref().ok_or_else(invalid)?,
                    now + chrono::TimeDelta::seconds(jobs::POLL_INTERVAL.as_secs() as i64),
                )),
                "failed" => Some(jobs::reconcile_job(
                    &pair.0,
                    &pair.1,
                    remote.epoch,
                    remote.next_retry_at.unwrap_or(now),
                )),
                "reserved" => {
                    let new = jobs::submit_job(&pair.0, &pair.1, &owner.0, &owner.1, owner.2, now);
                    let jobs::NewPinJob::Target(target) = &new else {
                        unreachable!()
                    };
                    if self.jobs.values().any(|j| {
                        j.provider == pair.0
                            && j.cid == pair.1
                            && j.id != target.id
                            && j.operation == "submit"
                            && matches!(j.state.as_str(), "pending" | "running")
                    }) {
                        Some(jobs::reconcile_job(&pair.0, &pair.1, remote.epoch, now))
                    } else {
                        Some(new)
                    }
                }
                _ => return Err(invalid()),
            };
            if let Some(job) = job {
                self.ensure_job(job, now, work)?;
            }
        }
        Ok(())
    }
}
