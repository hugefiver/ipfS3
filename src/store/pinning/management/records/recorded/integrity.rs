//! Exact converted source closure, retained only for read-only reply recovery.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
pub(super) struct Integrity {
    plans: BTreeMap<String, Plan>,
    sources: BTreeMap<native::NativeKey, Source>,
    pending: BTreeSet<native::NativeKey>,
}

struct Source {
    reference: segmented::ArchiveRef,
    cells: native::NativeRow,
    _resident: native::meter::ResidentGuard,
}

struct Plan {
    members: BTreeSet<native::NativeKey>,
    anchor: Option<native::NativeKey>,
    core_identity: Option<String>,
    catalog: Vec<String>,
    staged_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    confirmed_at: Option<DateTime<Utc>>,
    acknowledgements: Option<Vec<String>>,
}

impl Plan {
    fn matches(&self, review: &history::ReviewIndex) -> bool {
        self.anchor == review.anchor
            && self.core_identity == review.core_identity
            && self.catalog == review.catalog
            && self.staged_at == review.staged_at
            && self.expires_at == review.expires_at
            && self.confirmed_at == review.confirmed_at
            && self.acknowledgements == review.acknowledgements
    }
}

impl Integrity {
    pub(super) async fn remember<C: ConnectionTrait>(
        &mut self,
        db: &C,
        auth: &LocalAuthorization,
        source: history::ConvertedSource,
        cells: native::NativeRow,
        budget: &mut native::meter::Budget,
        decode: &mut ReadSession,
    ) -> Result<()> {
        let key = source.source;
        check_key(&key, auth)?;
        let review = source.review.ok_or(ManagementError::Confirmation)?;
        check_binding(&review, auth)?;
        if native::value(&cells, "native_identity") != Some(auth.database_identity.as_str())
            || native::value(&cells, "principal") != Some(auth.principal.as_str())
            || decode.key(&cells)? != key
        {
            return Err(ManagementError::Confirmation);
        }
        if let Some(plan) = self.plans.get(&key.values[1]) {
            if !plan.matches(&review) || !plan.members.contains(&key) {
                return Err(ManagementError::Changed);
            }
        } else {
            let request: PlanRequest = history::read_part(db, &review.request, budget).await?;
            management::validate_historical_request(&request)
                .map_err(|_| ManagementError::Confirmation)?;
            if request.plan_id != key.values[1] {
                return Err(ManagementError::Confirmation);
            }
            let members = request
                .items
                .iter()
                .map(|item| native::NativeKey {
                    table: super::super::super::schema::RECORDS.into(),
                    values: vec![
                        auth.database_scope.clone(),
                        request.plan_id.clone(),
                        item.item_id.clone(),
                        "action".into(),
                    ],
                })
                .collect::<BTreeSet<_>>();
            if !members.contains(&key) {
                return Err(ManagementError::Confirmation);
            }
            self.pending.extend(members.iter().cloned());
            self.plans.insert(
                request.plan_id,
                Plan {
                    members,
                    anchor: review.anchor,
                    core_identity: review.core_identity,
                    catalog: review.catalog,
                    staged_at: review.staged_at,
                    expires_at: review.expires_at,
                    confirmed_at: review.confirmed_at,
                    acknowledgements: review.acknowledgements,
                },
            );
        }
        let before = decode.usage().1;
        let dependencies = dependencies(&key, &cells, decode);
        budget.charge(0, 0, 0, (decode.usage().1 - before) as u64)?;
        for dependency in dependencies? {
            check_key(&dependency, auth)?;
            self.pending.insert(dependency);
        }
        let bytes = cells
            .values()
            .flatten()
            .try_fold(cells.len() * 128, |bytes, cell| {
                bytes.checked_add(cell.len()).ok_or(ManagementError::Budget)
            })?;
        let resident = budget.reserve(bytes)?;
        self.sources.insert(
            key,
            Source {
                reference: source.original,
                cells,
                _resident: resident,
            },
        );
        Ok(())
    }

    pub(super) async fn complete<C: ConnectionTrait>(
        &mut self,
        db: &C,
        auth: &LocalAuthorization,
        budget: &mut native::meter::Budget,
        decode: &mut ReadSession,
    ) -> Result<()> {
        while let Some(key) = self.pending.pop_first() {
            if self.sources.contains_key(&key) {
                continue;
            }
            let source = history::recover_source(db, &key, budget)
                .await?
                .ok_or(ManagementError::Changed)?;
            let cells = history::read_part(db, &source.original, budget).await?;
            self.remember(db, auth, source, cells, budget, decode)
                .await?;
        }
        decode.check()
    }

    pub(super) async fn check_current<C: ConnectionTrait>(
        &self,
        db: &C,
        auth: &LocalAuthorization,
        budget: &mut native::meter::Budget,
    ) -> Result<()> {
        for (plan_id, plan) in &self.plans {
            let spec =
                native::scope::ScopeSpec::new(vec![native::scope::Selector::RecordMembers {
                    database_scope: auth.database_scope.clone(),
                    plan_id: plan_id.clone(),
                }])?;
            let current = native::scope::collect_scope(db, &spec, budget).await?;
            let members = current
                .rows()
                .iter()
                .filter(|(_, row)| native::value(row, "kind") == Some("action"))
                .map(|(key, _)| key);
            if !members.eq(plan.members.iter()) {
                return Err(ManagementError::Changed);
            }
            let selector = serde_json::to_string(&(&auth.database_scope, plan_id))
                .map_err(|_| ManagementError::InvalidEvidence)?;
            for key in &plan.members {
                let cached = self.sources.get(key).ok_or(ManagementError::Changed)?;
                let source = history::recover_source(db, key, budget)
                    .await?
                    .ok_or(ManagementError::Changed)?;
                let review = source
                    .review
                    .as_ref()
                    .ok_or(ManagementError::Confirmation)?;
                check_binding(review, auth)?;
                if source.original != cached.reference
                    || !plan.matches(review)
                    || current.rows().get(key) != Some(&cached.cells)
                {
                    return Err(ManagementError::Changed);
                }
                let source_key = locator::source_key(key)?;
                budget.charge(1, 1, 1, (source_key.len() + selector.len()) as u64)?;
                if db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
                    "SELECT 1 AS present FROM pin_management_locator WHERE source_key=$1 AND selector_kind='record_members' AND selector_key=$2",
                    [source_key.into(), selector.clone().into()],
                )).await.map_err(|_| ManagementError::Database)?.is_none() {
                    return Err(ManagementError::Changed);
                }
            }
        }
        budget.check()
    }
}

fn check_key(key: &native::NativeKey, auth: &LocalAuthorization) -> Result<()> {
    if key.table != super::super::super::schema::RECORDS
        || key.values.len() != 4
        || key.values[0] != auth.database_scope
        || key.values[3] != "action"
    {
        return Err(ManagementError::Confirmation);
    }
    Ok(())
}

fn check_binding(review: &history::ReviewIndex, auth: &LocalAuthorization) -> Result<()> {
    if review.database_scope != auth.database_scope
        || review.database_identity != auth.database_identity
        || review.principal != auth.principal
    {
        return Err(ManagementError::Unauthorized);
    }
    Ok(())
}

fn dependencies(
    key: &native::NativeKey,
    cells: &native::NativeRow,
    decode: &mut ReadSession,
) -> Result<BTreeSet<native::NativeKey>> {
    let raw = native::value(cells, "native_before").ok_or(ManagementError::InvalidEvidence)?;
    let mut keys = BTreeSet::new();
    if let Some(raw) = raw.strip_prefix(archive::LOCATOR) {
        let (anchor, _, _): (archive::Key, String, String) = decode.typed(raw)?;
        keys.insert(native::NativeKey {
            table: anchor.0,
            values: anchor.1,
        });
    } else {
        let sections = archive::Sections::parse(raw, decode)?;
        let manifest: archive::Manifest = decode.typed(&raw[sections.manifest])?;
        for (_, rows) in manifest.1 {
            for row in rows {
                keys.insert(native::NativeKey {
                    table: row.1.0,
                    values: row.1.1,
                });
            }
        }
        // Frozen Text cells are data. Only source suppliers and Before cells
        // bind the live SQL original, even when a frozen row looks like an action.
        for range in sections.leaves {
            let leaf: archive::Leaf = decode.typed(&raw[range])?;
            for cell in leaf.2 {
                if let archive::Cell::Before(source, _, _) = cell {
                    keys.insert(native::NativeKey {
                        table: source.0,
                        values: source.1,
                    });
                }
            }
        }
    }
    keys.remove(key);
    Ok(keys)
}
