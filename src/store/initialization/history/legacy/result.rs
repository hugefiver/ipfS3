//! Checked historical result data. This does not implement worker permission.
use super::*;
use crate::store::pinning::management::actions::execution::facts::{self, Input, ItemFacts};
use serde::ser::SerializeStruct;
mod coordinates;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub binding: PlanEntry,
    pub intent_ids: Vec<String>,
    pub job_ids: Vec<String>,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordedAction {
    pub receipt: Receipt,
    pub state: records::ActionState,
    pub targets: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub root_releases: Vec<NativeKey>,
    pub jobs: Vec<records::JobAuthority>,
    pub migration_points: Vec<records::MigrationPoint>,
    pub decision: Option<records::OperatorDecision>,
}
struct WithoutState<'a>(&'a RecordedAction);
impl Serialize for WithoutState<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let action = self.0;
        let mut output = serializer.serialize_struct(
            "CommittedAction",
            if action.root_releases.is_empty() {
                5
            } else {
                6
            },
        )?;
        output.serialize_field("receipt", &action.receipt)?;
        output.serialize_field("targets", &action.targets)?;
        if !action.root_releases.is_empty() {
            output.serialize_field("root_releases", &action.root_releases)?;
        }
        output.serialize_field("jobs", &action.jobs)?;
        output.serialize_field("migration_points", &action.migration_points)?;
        output.serialize_field("decision", &action.decision)?;
        output.end()
    }
}
#[derive(Serialize, Deserialize)]
struct Envelope(RecordedAction, ItemFacts, String);
fn identity(
    scope: &str,
    identity: &str,
    principal: &str,
    plan: &str,
    envelope: &Envelope,
) -> Result<String> {
    struct Sink(Sha256);
    impl std::io::Write for Sink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut hash = Sink(Sha256::new());
    hash.0.update(b"stage7/facts/1\0");
    serde_json::to_writer(
        &mut hash,
        &(
            scope,
            identity,
            principal,
            plan,
            &envelope.0.receipt.binding.request.item_id,
            WithoutState(&envelope.0),
            &envelope.1,
        ),
    )
    .map_err(|_| ManagementError::InvalidEvidence)?;
    Ok(hex::encode(hash.0.finalize()))
}
fn decode(row: &NativeRow, work: &mut records::ReadSession) -> Result<Envelope> {
    work.typed(
        protocol::cell(row, "result")?
            .strip_prefix("S7R1\n")
            .ok_or(ManagementError::InvalidEvidence)?,
    )
}

pub(super) fn frozen(
    row: &NativeRow,
    current: &NativeRow,
    key: &NativeKey,
    entries: &[PlanEntry],
    work: &mut records::ReadSession,
) -> Result<Option<RecordedAction>> {
    if native::value(row, "state") == Some("prepared") {
        if native::value(row, "result").is_some()
            || native::value(row, "completed_at").is_some()
            || !work
                .typed::<Vec<String>>(protocol::cell(row, "intent_ids")?)?
                .is_empty()
            || !work
                .typed::<Vec<String>>(protocol::cell(row, "job_ids")?)?
                .is_empty()
        {
            return Err(ManagementError::Confirmation);
        }
        return Ok(None);
    }
    if native::value(row, "state") != Some("applied")
        || native::value(current, "state") != Some("applied")
        || native::value(row, "binding") != native::value(current, "binding")
    {
        return Err(ManagementError::Confirmation);
    }
    let original = decode(row, work)?;
    let actual = decode(current, work)?;
    let (ordinal, entry) = entries
        .iter()
        .enumerate()
        .find(|(_, entry)| entry.request.item_id == key.values[2])
        .ok_or(ManagementError::Confirmation)?;
    let scope = protocol::cell(row, "database_scope")?;
    let database_identity = protocol::cell(row, "native_identity")?;
    let principal = protocol::cell(row, "principal")?;
    if original.0.receipt.binding != *entry
        || original.1.0 != ordinal
        || original.2 != actual.2
        || original.2
            != identity(
                scope,
                database_identity,
                principal,
                &key.values[1],
                &original,
            )?
        || actual.2 != identity(scope, database_identity, principal, &key.values[1], &actual)?
        || original.0.receipt.intent_ids
            != work.typed::<Vec<String>>(protocol::cell(row, "intent_ids")?)?
        || original.0.receipt.job_ids
            != work.typed::<Vec<String>>(protocol::cell(row, "job_ids")?)?
    {
        return Err(ManagementError::Confirmation);
    }
    Ok(Some(original.0))
}

/// Resolves the complete original same-plan input/output cursor and verifies
/// exact typed receipt binding, local DB originals, identities and dependencies.
/// The returned value is immutable recovery data, not TrustedAppliedAction.
pub(super) fn recorded(review: &Review, key: &NativeKey) -> Result<Option<RecordedAction>> {
    let mut work = records::ReadSession::default();
    let root = review
        .original(key)
        .ok_or(ManagementError::InvalidEvidence)?;
    if native::value(root, "state") != Some("applied") {
        return Ok(None);
    }
    let until = review
        .entries
        .iter()
        .position(|entry| entry.request.item_id == key.values[2])
        .ok_or(ManagementError::Confirmation)?;
    let mut cursor = BTreeMap::<facts::Key, (usize, usize, String, Option<NativeRow>)>::new();
    let mut result = None;
    for (position, entry) in review.entries.iter().enumerate().take(until + 1) {
        let key = NativeKey {
            table: management::schema::RECORDS.into(),
            values: vec![
                review.database_scope.clone(),
                review.request.plan_id.clone(),
                entry.request.item_id.clone(),
                "action".into(),
            ],
        };
        let original = review
            .original(&key)
            .ok_or(ManagementError::InvalidEvidence)?;
        if native::value(original, "state") != Some("applied") {
            return Err(ManagementError::Confirmation);
        }
        let envelope = decode(original, &mut work)?;
        let Envelope(action, facts, digest) = &envelope;
        let intents: Vec<String> = work.typed(protocol::cell(original, "intent_ids")?)?;
        let jobs: Vec<String> = work.typed(protocol::cell(original, "job_ids")?)?;
        if action.receipt.binding != *entry
            || action.state == records::ActionState::Stopped
            || facts.0 != position
            || action.receipt.intent_ids != intents
            || action.receipt.job_ids != jobs
            || action.jobs.iter().map(|job| &job.job_id).ne(jobs.iter())
            || action.jobs.windows(2).any(|p| p[0].job_id >= p[1].job_id)
            || *digest
                != identity(
                    &review.database_scope,
                    &review.database_identity,
                    &review.principal,
                    &review.request.plan_id,
                    &envelope,
                )?
        {
            return Err(ManagementError::Confirmation);
        }
        if facts.1.windows(2).any(|p| p[0].0 >= p[1].0)
            || facts.2.windows(2).any(|p| p[0].0 >= p[1].0)
        {
            return Err(ManagementError::Confirmation);
        }
        let catalog = native::archived_catalog(&review.catalog)?;
        let mut reads = Vec::new();
        for read in &facts.1 {
            let table = catalog
                .iter()
                .find(|table| table.table == read.0.0)
                .ok_or(ManagementError::Schema)?;
            if read.0.1.len() != table.primary_key.len() {
                return Err(ManagementError::Confirmation);
            }
            let native = NativeKey {
                table: read.0.0.clone(),
                values: read.0.1.clone(),
            };
            if let Some((producer, slot, hash, row)) = cursor.get(&read.0) {
                if !matches!(&read.1,Input::Output(p,s,h) if p==producer && s==slot && h==hash) {
                    return Err(ManagementError::Confirmation);
                }
                reads.push(row.as_ref());
            } else if !matches!(
                (&read.1, review.rows.contains_key(&native)),
                (Input::Manifest, true) | (Input::Absent, false)
            ) {
                return Err(ManagementError::Confirmation);
            } else {
                reads.push(review.rows.get(&native));
            }
        }
        let mut outputs = Vec::new();
        for output in &facts.2 {
            if facts.1.get(output.1).is_none_or(|read| read.0 != output.0) {
                return Err(ManagementError::Confirmation);
            }
            let table = catalog
                .iter()
                .find(|table| table.table == output.0.0)
                .ok_or(ManagementError::Schema)?;
            let row = output
                .2
                .as_ref()
                .map(|cells| {
                    if cells.len() != table.columns.len() {
                        return Err(ManagementError::InvalidEvidence);
                    }
                    let row = table
                        .columns
                        .iter()
                        .cloned()
                        .zip(cells.iter().cloned())
                        .collect::<NativeRow>();
                    if table.primary_key.len() != output.0.1.len()
                        || table
                            .primary_key
                            .iter()
                            .zip(&output.0.1)
                            .any(|(column, key)| native::value(&row, column) != Some(key))
                    {
                        return Err(ManagementError::InvalidEvidence);
                    }
                    Ok(row)
                })
                .transpose()?;
            work.copy(&row)?;
            if row.as_ref() == reads[output.1] {
                return Err(ManagementError::Confirmation);
            }
            outputs.push(row);
        }
        coordinates::validate(facts, action, &reads, &outputs, &mut work)?;
        for (slot, (output, row)) in facts.2.iter().zip(outputs).enumerate() {
            cursor.insert(output.0.clone(), (position, slot, digest.clone(), row));
        }
        for dependency in &facts.6 {
            let key = NativeKey {
                table: dependency.1.0.clone(),
                values: dependency.1.1.clone(),
            };
            let row = review.rows.get(&key).ok_or(ManagementError::Confirmation)?;
            if key.table != management::schema::RECORDS
                || key.values.len() != 4
                || key.values[0] != review.database_scope
                || native::value(row, "state") != Some("applied")
                || native::value(row, "native_identity") != Some(review.database_identity.as_str())
                || native::value(row, "principal") != Some(review.principal.as_str())
            {
                return Err(ManagementError::Confirmation);
            }
            let predecessor = decode(row, &mut work)?;
            if predecessor.2 != dependency.2
                || predecessor.2
                    != identity(
                        &review.database_scope,
                        &review.database_identity,
                        &review.principal,
                        &key.values[1],
                        &predecessor,
                    )?
                || predecessor.0.receipt.binding.request.item_id != key.values[2]
            {
                return Err(ManagementError::Confirmation);
            }
        }
        result = Some(envelope.0);
    }
    Ok(result)
}
