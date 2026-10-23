//! One decoded review per immutable core. Confirmation holds only its ACK overlay.
use super::*;
use std::sync::Arc;

pub struct Review {
    pub(super) owner: Arc<batch::StoredSource>,
    pub(super) core: Arc<Core>,
    pub(super) rows: ArchivedRows,
    pub request: SharedValue<PlanRequest>,
    pub principal: String,
    pub database_scope: String,
    pub database_identity: String,
}

pub struct ConfirmationAck {
    pub plan_id: String,
    pub bindings: SharedValue<Vec<crate::pinning::management::PlanEntry>>,
    pub candidate_item_ids: SharedValue<Vec<String>>,
    snapshot: SharedValue<SnapshotRest>,
}
impl std::ops::Deref for ConfirmationAck {
    type Target = SnapshotRest;
    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}
#[derive(Clone)]
pub struct ConfirmedPlan {
    pub(super) review: Arc<Review>,
    pub confirmation: std::sync::Arc<ConfirmationAck>,
    pub confirmed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub(crate) work: SharedDecodeBudget,
    stored_binding: Arc<batch::Binding>,
    original_binding: Arc<native::NativeRow>,
}
impl std::ops::Deref for ConfirmedPlan {
    type Target = Review;
    fn deref(&self) -> &Self::Target {
        &self.review
    }
}
impl PartialEq for ConfirmedPlan {
    fn eq(&self, other: &Self) -> bool {
        self.view() == other.view()
    }
}
impl Eq for ConfirmedPlan {}
impl ConfirmedPlan {
    pub(crate) fn original_binding(&self) -> Result<&str> {
        native::value(&self.original_binding, "binding").ok_or(ManagementError::Confirmation)
    }
}
#[derive(Clone)]
pub struct ReviewHandle {
    review: Arc<Review>,
    binding: Arc<batch::Binding>,
    work: SharedDecodeBudget,
}
impl PartialEq for ReviewHandle {
    fn eq(&self, other: &Self) -> bool {
        self.review.owner.raw() == other.review.owner.raw()
            && self.binding == other.binding
            && self
                .rows()
                .all_rows()
                .map(|(table, row)| (&table.table, row))
                .eq(other
                    .rows()
                    .all_rows()
                    .map(|(table, row)| (&table.table, row)))
    }
}
impl Eq for ReviewHandle {}
impl ReviewHandle {
    /// Coordinates of the verified original core, not a reconstructed identity.
    pub(crate) fn original_core(&self) -> Result<(native::NativeKey, String)> {
        let mut work = self
            .work
            .lock()
            .map_err(|_| ManagementError::InvalidEvidence)?;
        let head = &self.review.owner.head;
        let key = head.3.native(&mut work)?;
        work.copy(&head.5)?;
        Ok((key, head.5.clone()))
    }

    pub(crate) fn rows(&self) -> FrozenRows<'_> {
        FrozenRows::Archive(&self.review.rows)
    }
    pub fn entries(&self) -> &[crate::pinning::management::PlanEntry] {
        &self.review.core.1
    }
    pub fn request(&self) -> &crate::pinning::management::PlanRequest {
        &self.review.core.0
    }
    pub fn summary(&self) -> &crate::pinning::management::Summary {
        &self.review.core.2
    }
    pub fn migration_points(&self) -> &[MigrationPoint] {
        &self.review.core.6
    }
    pub fn candidate_item_ids(&self) -> Option<&[String]> {
        match self.binding.as_ref() {
            batch::Binding::Draft(..) => None,
            batch::Binding::Confirmed(_, _, _, acks) => Some(acks),
        }
    }
    pub(crate) fn confirmed_at(&self) -> Option<DateTime<Utc>> {
        match self.binding.as_ref() {
            batch::Binding::Draft(..) => None,
            batch::Binding::Confirmed(_, _, at, _) => Some(*at),
        }
    }
    pub(crate) fn snapshot(&self) -> &impl Serialize {
        &self.review.core.3
    }
    pub(crate) fn planning_view(&self) -> crate::pinning::management::SnapshotView<'_> {
        self.review.core.3.view(self.rows())
    }
    pub(crate) fn archive(&self) -> &ArchivedRows {
        &self.review.rows
    }
    pub(crate) fn staged_at(&self) -> DateTime<Utc> {
        self.review.core.4
    }
    pub(crate) fn expires_at(&self) -> DateTime<Utc> {
        self.review.core.5
    }
    pub(crate) fn confirmation_encoding(
        &self,
        now: DateTime<Utc>,
        acks: &[String],
    ) -> Result<String> {
        let mut work = self
            .work
            .lock()
            .map_err(|_| ManagementError::InvalidEvidence)?;
        let body = encoded(
            &(
                1,
                &self.review.owner.head.3,
                &self.review.owner.head.5,
                now,
                acks,
            ),
            &mut work,
        )?;
        work.bytes(BINDING.len() + body.len())?;
        Ok(format!("{BINDING}{body}"))
    }
    pub(crate) fn view<'a>(
        &'a self,
        confirmed_at: DateTime<Utc>,
        acks: &'a [String],
    ) -> BindingView<'a> {
        BindingView {
            rows: Some(&self.review.rows),
            work: &self.work,
            request: self.request(),
            confirmation: super::super::views::ConfirmationView {
                plan_id: &self.review.core.0.plan_id,
                bindings: &self.review.core.1,
                candidate_item_ids: acks,
                configuration: &self.review.core.3.configuration,
            },
            principal: &self.review.owner.head.2,
            database_scope: &self.review.owner.head.0,
            database_identity: &self.review.owner.head.1,
            confirmed_at,
            expires_at: self.expires_at(),
        }
    }
    pub fn original_cell(
        &self,
        table: &str,
        key: &[String],
        column: &str,
    ) -> Option<&Option<String>> {
        self.review.rows.original_cell(table, key, column)
    }
    pub fn original_value(&self, table: &str, key: &[&str], column: &str) -> Option<&str> {
        self.rows().original_value(table, key, column)
    }
    pub fn visit_original_cells(&self, mut visit: impl FnMut(&str, &[String], &str, Option<&str>)) {
        for (table, key, row) in self.review.rows.rows() {
            for (column, value) in row.iter() {
                visit(&table.table, key, column, value.as_deref());
            }
        }
    }
    pub(crate) fn scope(&self) -> &str {
        &self.review.owner.head.0
    }
    pub(crate) fn identity(&self) -> &str {
        &self.review.owner.head.1
    }
    pub(crate) fn principal(&self) -> &str {
        &self.review.owner.head.2
    }
}
pub(in crate::store::pinning::management::records) struct RootMetadata {
    owner: Arc<batch::StoredSource>,
    core: Arc<Core>,
    confirmation: Arc<batch::Binding>,
    work: SharedDecodeBudget,
}
impl RootMetadata {
    pub(in crate::store::pinning::management::records) fn view(&self) -> BindingView<'_> {
        let batch::Binding::Confirmed(_, _, confirmed_at, acks) = self.confirmation.as_ref() else {
            unreachable!("confirmed metadata")
        };
        BindingView {
            work: &self.work,
            rows: None,
            request: &self.core.0,
            confirmation: super::super::views::ConfirmationView {
                plan_id: &self.core.0.plan_id,
                bindings: &self.core.1,
                candidate_item_ids: acks,
                configuration: &self.core.3.configuration,
            },
            principal: &self.owner.head.2,
            database_scope: &self.owner.head.0,
            database_identity: &self.owner.head.1,
            confirmed_at: *confirmed_at,
            expires_at: self.core.5,
        }
    }
}
pub(in crate::store::pinning::management::records) fn classify(
    source: &evidence::Evidence,
    root: &native::NativeRow,
    inventory: &str,
    work: &mut DecodeBudget,
) -> Result<RootMetadata> {
    let owner = read::owner(source, root, work)?;
    let core = read::core(&owner, work)?;
    let confirmation = read::members(source, root, &owner, &core, work)?;
    if !matches!(confirmation.as_ref(), batch::Binding::Confirmed(..)) {
        return Err(ManagementError::Confirmation);
    }
    let (key, hash): (Key, String) = work.typed(
        inventory
            .strip_prefix(INVENTORY)
            .ok_or(ManagementError::InvalidEvidence)?,
    )?;
    if key != owner.head.3 || hash != owner.head.6 {
        return Err(ManagementError::Confirmation);
    }
    read::manifest(&owner, work)?;
    Ok(RootMetadata {
        owner,
        core,
        confirmation,
        work: source.work.clone(),
    })
}
impl ConfirmedPlan {
    pub(crate) fn archived_rows(&self) -> &ArchivedRows {
        &self.review.rows
    }
    pub fn review(&self) -> ReviewHandle {
        ReviewHandle {
            review: self.review.clone(),
            binding: self.stored_binding.clone(),
            work: self.work.clone(),
        }
    }
    pub(in crate::store::pinning::management::records) fn review_id(&self) -> usize {
        Arc::as_ptr(&self.review) as usize
    }
    pub(crate) fn view(&self) -> BindingView<'_> {
        let confirmed_at = self.confirmed_at;
        let acks = &self.confirmation.candidate_item_ids;
        let review = &self.review;
        BindingView {
            work: &self.work,
            rows: Some(&review.rows),
            request: &review.core.0,
            confirmation: super::super::views::ConfirmationView {
                plan_id: &review.core.0.plan_id,
                bindings: &review.core.1,
                candidate_item_ids: acks,
                configuration: &review.core.3.configuration,
            },
            principal: &review.owner.head.2,
            database_scope: &review.owner.head.0,
            database_identity: &review.owner.head.1,
            confirmed_at,
            expires_at: review.core.5,
        }
    }
}

pub(in crate::store::pinning::management::records) fn load_confirmed(
    source: &evidence::Evidence,
    root: &native::NativeRow,
    work: &mut DecodeBudget,
) -> Result<ConfirmedPlan> {
    let review = load_review(source, root, work)?;
    if !matches!(review.binding.as_ref(), batch::Binding::Confirmed(..)) {
        return Err(ManagementError::Confirmation);
    }
    let batch::Binding::Confirmed(_, _, confirmed_at, acks) = review.binding.as_ref() else {
        return Err(ManagementError::Confirmation);
    };
    let expires_at = review.expires_at();
    work.copy(&review.request().plan_id)?;
    work.nodes(2)?;
    let original_binding = batch::retain_row_ref(root, work)?;
    let confirmation = std::sync::Arc::new(ConfirmationAck {
        plan_id: review.request().plan_id.clone(),
        bindings: review.review.core.1.clone(),
        candidate_item_ids: acks.clone(),
        snapshot: review.review.core.3.clone(),
    });
    Ok(ConfirmedPlan {
        review: review.review,
        confirmation,
        confirmed_at: *confirmed_at,
        expires_at,
        work: source.work.clone(),
        stored_binding: review.binding.clone(),
        original_binding,
    })
}
pub(in crate::store::pinning::management::records) fn load_review(
    source: &evidence::Evidence,
    root: &native::NativeRow,
    work: &mut DecodeBudget,
) -> Result<ReviewHandle> {
    let owner = read::owner(source, root, work)?;
    let core = read::core(&owner, work)?;
    let confirmation = read::members(source, root, &owner, &core, work)?;
    // Membership is consumer-local even when all immutable sections are cached.
    let (manifest, catalog) = read::manifest(&owner, work)?;
    let cached = work
        .archive
        .reviews
        .iter()
        .find(|review| Arc::ptr_eq(&review.owner, &owner))
        .cloned();
    let review = if let Some(review) = cached {
        review.rows.recheck(source, work)?;
        review
    } else {
        let original_rows = rows::load(source, &owner, manifest, catalog, work)?;
        work.nodes(2)?;
        work.copy(&(&owner.head.0, &owner.head.1, &owner.head.2))?;
        let review = Arc::new(Review {
            request: core.0.clone(),
            principal: owner.head.2.clone(),
            database_scope: owner.head.0.clone(),
            database_identity: owner.head.1.clone(),
            owner,
            core,
            rows: original_rows,
        });
        work.archive.reviews.push(review.clone());
        review
    };
    crate::pinning::management::validate_snapshot_view_with_work(
        review.core.3.view(FrozenRows::Archive(&review.rows)),
        work,
    )
    .map_err(|error| {
        if error == crate::pinning::management::SafeError::LimitExceeded {
            ManagementError::Budget
        } else {
            ManagementError::InvalidEvidence
        }
    })?;
    Ok(ReviewHandle {
        review,
        binding: confirmation,
        work: source.work.clone(),
    })
}
