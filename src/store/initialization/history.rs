//! Explicit conversion output. Recovery reads typed S75A1 references, not S7 text.
use crate::store::pinning::management::{self, ManagementError, Result, native, records};
use native::{NativeKey, NativeRow, meter::Budget};
use records::segmented::{ArchiveHeader, ArchiveRef, read_archive, write_archive};
use sea_orm::{ConnectionTrait, DatabaseTransaction, Statement};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::io::{Seek, SeekFrom};

pub mod legacy;
mod original;
mod spool;

#[derive(PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UpgradeStamp {
    scope: String,
    identity: String,
    source: String,
    target: String,
}

pub(crate) async fn upgrade_stamp(
    tx: &DatabaseTransaction,
    request: &super::UpgradeRequest<'_>,
    preparing: bool,
) -> Result<()> {
    let stamp = UpgradeStamp {
        scope: request.database_scope.into(),
        identity: request.native_identity.into(),
        source: request.source_version.into(),
        target: request.target_version.into(),
    };
    let mut work = Budget::default();
    if let Some(reference) = reference(tx, "history:upgrade").await? {
        if preparing || read_part::<_, UpgradeStamp>(tx, &reference, &mut work).await? != stamp {
            return Err(ManagementError::Confirmation);
        }
    } else {
        if !preparing {
            return Err(ManagementError::Confirmation);
        }
        let header = ArchiveHeader {
            database_scope: stamp.scope.clone(),
            database_identity: stamp.identity.clone(),
            principal: "explicit_local_upgrade".into(),
            catalog: vec![stamp.source.clone(), stamp.target.clone()],
            request: String::new(),
            configuration: String::new(),
            acknowledgements: vec![],
            created_at: String::new(),
            expires_at: None,
        };
        store_json(tx, "history:upgrade", &header, &stamp, &mut work).await?;
    }
    Ok(())
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowReference {
    pub key: NativeKey,
    pub original: ArchiveRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review: Option<FrozenReviewReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recorded: Option<ArchiveRef>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenReviewReference {
    pub version: ArchiveRef,
    pub anchor: NativeKey,
    pub core_identity: String,
    pub database_identity: String,
    pub principal: String,
    pub confirmed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub acknowledgements: Option<Vec<String>>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewIndex {
    #[serde(default)]
    pub anchor: Option<NativeKey>,
    #[serde(default)]
    pub core_identity: Option<String>,
    pub catalog: Vec<String>,
    pub database_scope: String,
    pub database_identity: String,
    pub principal: String,
    pub request: ArchiveRef,
    pub entries: ArchiveRef,
    pub summary: ArchiveRef,
    pub snapshot: ArchiveRef,
    pub migration_points: ArchiveRef,
    pub rows: Vec<RowReference>,
    pub staged_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    #[serde(default)]
    pub confirmed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub acknowledgements: Option<Vec<String>>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConvertedSource {
    pub source: NativeKey,
    pub source_generation: i64,
    pub original: ArchiveRef,
    pub review: Option<ReviewIndex>,
    /// Only a validated, locally committed SQL record can populate this field.
    pub committed: Option<ArchiveRef>,
    /// Historical provider captures are preserved as data, never relabelled as
    /// a current sealed inventory or exposed as TrustedAppliedAction.
    #[serde(default)]
    pub historical: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recorded: Option<ArchiveRef>,
}

pub(crate) fn archive_id(
    scope: &str,
    identity: &str,
    key: &NativeKey,
    generation: i64,
) -> Result<String> {
    let mut hash = Sha256::new();
    hash.update(b"stage75/history/1\0");
    hash.update(
        serde_json::to_vec(&(scope, identity, key, generation))
            .map_err(|_| ManagementError::InvalidEvidence)?,
    );
    Ok(format!("history:{}", hex::encode(hash.finalize())))
}

async fn reference<C: ConnectionTrait>(db: &C, id: &str) -> Result<Option<ArchiveRef>> {
    db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT digest,bytes,chunks FROM pin_management_archive WHERE archive_id=$1 AND state='sealed'", [id.into()]))
        .await.map_err(|_| ManagementError::Database)?.map(|row| Ok(ArchiveRef {
            archive_id: id.into(),
            digest: row.try_get("", "digest").map_err(|_| ManagementError::InvalidEvidence)?,
            bytes: row.try_get("", "bytes").map_err(|_| ManagementError::InvalidEvidence)?,
            chunks: row.try_get("", "chunks").map_err(|_| ManagementError::InvalidEvidence)?,
        })).transpose()
}

async fn store_json<T: Serialize + ?Sized>(
    tx: &DatabaseTransaction,
    id: &str,
    header: &ArchiveHeader,
    value: &T,
    budget: &mut Budget,
) -> Result<ArchiveRef> {
    let mut spool = spool::Spool::new()?;
    serde_json::to_writer(&mut spool.file, value).map_err(|_| ManagementError::InvalidEvidence)?;
    spool
        .file
        .seek(SeekFrom::Start(0))
        .map_err(|_| ManagementError::Database)?;
    write_archive(tx, id, header, &mut spool.file, budget).await
}

/// The archive trailer is verified before JSON is exposed. The caller's budget
/// applies to each decoded part; native originals are never written to business tables.
pub async fn read_part<C: ConnectionTrait, T: DeserializeOwned>(
    db: &C,
    part: &ArchiveRef,
    budget: &mut Budget,
) -> Result<T> {
    let mut spool = spool::Spool::new()?;
    read_archive(db, part, &mut spool.file, budget).await?;
    let bytes = usize::try_from(part.bytes).map_err(|_| ManagementError::Budget)?;
    let _resident = budget.reserve(
        bytes
            .checked_mul(3)
            .and_then(|n| n.checked_add(native::meter::CELL_SLICE))
            .ok_or(ManagementError::Budget)?,
    )?;
    spool
        .file
        .seek(SeekFrom::Start(0))
        .map_err(|_| ManagementError::Database)?;
    serde_json::from_reader(std::io::BufReader::with_capacity(
        native::meter::CELL_SLICE,
        &mut spool.file,
    ))
    .map_err(|_| ManagementError::InvalidEvidence)
}

pub(crate) async fn generation<C: ConnectionTrait>(db: &C, key: &NativeKey) -> Result<i64> {
    let source = records::locator::source_key(key)?;
    let row = db
        .query_one(Statement::from_sql_and_values(
            db.get_database_backend(),
            "SELECT source_generation FROM pin_management_source_control WHERE source_key=$1",
            [source.into()],
        ))
        .await
        .map_err(|_| ManagementError::Database)?
        .ok_or(ManagementError::InvalidEvidence)?;
    row.try_get("", "source_generation")
        .map_err(|_| ManagementError::InvalidEvidence)
}

/// This lookup is exact and does not authorize execution or renew a confirmation.
/// A foreign original remains recoverable, but cannot produce a local receipt.
/// `None` means this sealed source generation has no conversion index. Missing
/// or dirty source seals are `Changed`, not an authoritative negative lookup.
/// Schema/readiness, archive integrity and budget failures remain errors.
pub async fn recover_source<C: ConnectionTrait>(
    db: &C,
    key: &NativeKey,
    budget: &mut Budget,
) -> Result<Option<ConvertedSource>> {
    management::schema::validate(db).await?;
    let (scope, identity) = native::scope::binding(db).await?;
    let source = records::locator::source_key(key)?;
    let generation = sealed_generation(db, &source).await?;
    let index = recover_at(db, &scope, &identity, key, generation, budget).await?;
    if sealed_generation(db, &source).await? != generation {
        return Err(ManagementError::Changed);
    }
    Ok(index)
}

async fn sealed_generation<C: ConnectionTrait>(db: &C, source: &str) -> Result<i64> {
    let row=db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT source_generation FROM pin_management_source_control c WHERE c.source_key=$1 AND c.source_generation=c.sealed_source AND c.locator_generation=c.sealed_locator AND NOT EXISTS (SELECT 1 FROM pin_management_pending_scope p WHERE p.source_key=c.source_key)", [source.into()]))
        .await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
    row.try_get("", "source_generation")
        .map_err(|_| ManagementError::InvalidEvidence)
}

async fn unchanged<C: ConnectionTrait>(db: &C, index: &ConvertedSource) -> Result<()> {
    if sealed_generation(db, &records::locator::source_key(&index.source)?).await?
        != index.source_generation
    {
        return Err(ManagementError::Changed);
    }
    Ok(())
}

pub(crate) async fn recover_at<C: ConnectionTrait>(
    db: &C,
    scope: &str,
    identity: &str,
    key: &NativeKey,
    generation: i64,
    budget: &mut Budget,
) -> Result<Option<ConvertedSource>> {
    let id = archive_id(scope, identity, key, generation)?;
    let Some(reference) = reference(db, &id).await? else {
        return Ok(None);
    };
    let index: ConvertedSource = read_part(db, &reference, budget).await?;
    if index.source != *key || index.source_generation != generation {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(Some(index))
}

/// The source row must still be the original local applied fact. Configuration
/// drift and an expired confirmation do not alter this already committed result.
/// `key` is the full (scope, plan, item, kind) native key; `original_binding` is
/// the exact retained SQL binding, not an imported applied claim or plan ID.
/// Returns the frozen committed action, including its original state. Callers
/// derive current status separately and must not execute the returned action.
/// Wrong bindings/subjects return `Confirmation`; corrupt archives return
/// `InvalidEvidence`; source drift/dirty seals return `Changed`. A valid source
/// without a locally validated committed result returns `None`.
pub async fn recover_committed<C: ConnectionTrait>(
    db: &C,
    key: &NativeKey,
    original_binding: &str,
    budget: &mut Budget,
) -> Result<Option<records::CommittedAction>> {
    let Some(index) = recover_source(db, key, budget).await? else {
        return Ok(None);
    };
    let Some(result) = &index.committed else {
        return Ok(None);
    };
    let original: NativeRow = read_part(db, &index.original, budget).await?;
    let (scope, identity) = native::scope::binding(db).await?;
    if key.table != management::schema::RECORDS
        || key.values.len() != 4
        || key.values[0] != scope
        || key.values[3] != "action"
        || native::value(&original, "native_identity") != Some(identity.as_str())
        || native::value(&original, "principal") != Some("local_configuration")
        || native::value(&original, "state") != Some("applied")
        || native::value(&original, "binding") != Some(original_binding)
    {
        return Err(ManagementError::Confirmation);
    }
    let (current, _resident) = original::read(db, key, budget).await?;
    if current != original {
        return Err(ManagementError::Changed);
    }
    let action: records::CommittedAction = read_part(db, result, budget).await?;
    if action.receipt.binding.request.item_id != key.values[2] {
        return Err(ManagementError::InvalidEvidence);
    }
    unchanged(db, &index).await?;
    Ok(Some(action))
}

/// Exact locally committed historical data whose capture cannot be represented
/// by the current execution DTO. This API grants no worker/execution authority.
/// It has the same source/binding/readiness/error contract as recover_committed.
pub async fn recover_recorded<C: ConnectionTrait>(
    db: &C,
    key: &NativeKey,
    original_binding: &str,
    budget: &mut Budget,
) -> Result<Option<legacy::RecordedAction>> {
    let Some(index) = recover_source(db, key, budget).await? else {
        return Ok(None);
    };
    let Some(reference) = &index.recorded else {
        return Ok(None);
    };
    let original: NativeRow = read_part(db, &index.original, budget).await?;
    let (scope, identity) = native::scope::binding(db).await?;
    if key.table != management::schema::RECORDS
        || key.values.len() != 4
        || key.values[0] != scope
        || key.values[3] != "action"
        || native::value(&original, "native_identity") != Some(identity.as_str())
        || native::value(&original, "principal") != Some("local_configuration")
        || native::value(&original, "state") != Some("applied")
        || native::value(&original, "binding") != Some(original_binding)
    {
        return Err(ManagementError::Confirmation);
    }
    let (current, _resident) = original::read(db, key, budget).await?;
    if current != original {
        return Err(ManagementError::Changed);
    }
    let action: legacy::RecordedAction = read_part(db, reference, budget).await?;
    if action.receipt.binding.request.item_id != key.values[2] {
        return Err(ManagementError::InvalidEvidence);
    }
    unchanged(db, &index).await?;
    Ok(Some(action))
}

/// Conversion preserves an unexecuted review, not the right to execute it.
/// Confirmation/apply callers use this exact source marker before accepting a
/// historical handle; they must request a new plan rather than renew its TTL.
pub async fn requires_new_confirmation<C: ConnectionTrait>(
    db: &C,
    key: &NativeKey,
    budget: &mut Budget,
) -> Result<bool> {
    let Some(index) = recover_source(db, key, budget).await? else {
        return Ok(false);
    };
    if key.table != management::schema::RECORDS
        || key.values.get(3).map(String::as_str) != Some("action")
    {
        return Ok(false);
    }
    let original: NativeRow = read_part(db, &index.original, budget).await?;
    Ok(native::value(&original, "state") != Some("applied"))
}

/// Exact private/foreign/NULL cells from the original review, not current rows.
pub async fn original_cell<C: ConnectionTrait>(
    db: &C,
    review: &ReviewIndex,
    key: &NativeKey,
    column: &str,
    budget: &mut Budget,
) -> Result<Option<Option<String>>> {
    let Some(reference) = review.rows.iter().find(|row| row.key == *key) else {
        return Ok(None);
    };
    let mut row: NativeRow = read_part(db, &reference.original, budget).await?;
    Ok(row.remove(column))
}

/// Resolve a frozen row's review body with its own confirmation state, not the
/// current row's later ACKs. Reads only converted canonical parts, never S7 text.
pub async fn recover_frozen_review<C: ConnectionTrait>(
    db: &C,
    row: &RowReference,
    budget: &mut Budget,
) -> Result<Option<ReviewIndex>> {
    let Some(reference) = &row.review else {
        return Ok(None);
    };
    management::schema::validate(db).await?;
    let mut view: ReviewIndex = read_part(db, &reference.version, budget).await?;
    if view.anchor.as_ref() != Some(&reference.anchor)
        || view.core_identity.as_ref() != Some(&reference.core_identity)
        || view.database_identity != reference.database_identity
        || view.principal != reference.principal
        || row.key.values.first() != Some(&view.database_scope)
    {
        return Err(ManagementError::InvalidEvidence);
    }
    let entries: Vec<legacy::PlanEntry> = read_part(db, &view.entries, budget).await?;
    legacy::validate_frozen_binding(reference, view.staged_at, view.expires_at, &entries)?;
    view.acknowledgements = reference.acknowledgements.clone();
    view.confirmed_at = reference.confirmed_at;
    Ok(Some(view))
}

pub(crate) async fn convert_source(
    tx: &DatabaseTransaction,
    key: &NativeKey,
    scope: &str,
    identity: &str,
    budget: &mut Budget,
) -> Result<()> {
    let generation = generation(tx, key).await?;
    let (row, _resident) = original::read(tx, key, budget).await?;
    if let Some(index) = recover_at(tx, scope, identity, key, generation, budget).await? {
        let previous: NativeRow = read_part(tx, &index.original, budget).await?;
        if previous != row {
            return Err(ManagementError::Changed);
        }
        return Ok(());
    }
    if key.table == management::schema::RECORDS
        && native::value(&row, "kind") == Some("action")
        && let Some(historical) = legacy::encoding(tx, key, budget).await?
    {
        let review = legacy::read(tx, key, budget).await?;
        return legacy::convert(
            tx,
            (key, &row, generation),
            (scope, identity),
            review,
            historical,
            budget,
        )
        .await;
    }
    if key.table == management::schema::RETIREMENTS {
        let action = NativeKey {
            table: management::schema::RECORDS.into(),
            values: ["database_scope", "plan_id", "item_id", "record_kind"]
                .iter()
                .map(|c| {
                    native::value(&row, c)
                        .map(str::to_owned)
                        .ok_or(ManagementError::InvalidEvidence)
                })
                .collect::<Result<_>>()?,
        };
        if let Some(historical) = legacy::encoding(tx, &action, budget).await? {
            let review = legacy::retirement(tx, &row, budget).await?;
            return legacy::convert(
                tx,
                (key, &row, generation),
                (scope, identity),
                review,
                historical,
                budget,
            )
            .await;
        }
    }
    let id = archive_id(scope, identity, key, generation)?;
    let mut header = ArchiveHeader {
        database_scope: scope.into(),
        database_identity: identity.into(),
        principal: native::value(&row, "principal").unwrap_or("").into(),
        catalog: vec![management::schema::VERSION.into()],
        request: String::new(),
        configuration: String::new(),
        acknowledgements: vec![],
        created_at: native::value(&row, "created_at").unwrap_or("").into(),
        expires_at: native::value(&row, "expires_at").map(str::to_owned),
    };
    let work = records::SharedDecodeBudget::default();
    let local = native::value(&row, "database_scope") == Some(scope)
        && native::value(&row, "native_identity") == Some(identity)
        && native::value(&row, "principal") == Some("local_configuration");
    let formatted = native::value(&row, "native_before")
        .is_some_and(|raw| raw.starts_with("S7F1\n") || raw.starts_with("S7L1\n"));
    let review = if key.table == management::schema::RECORDS
        && native::value(&row, "kind") == Some("action")
        && (local || formatted)
    {
        Some(records::inspect_review_with_work(tx, &row, work.clone()).await?)
    } else if key.table == management::schema::RETIREMENTS
        && (local || native::value(&row, "inventory").is_some_and(|raw| raw.starts_with("S7I1\n")))
    {
        Some(records::inspect_retirement_review_with_work(tx, &row, work.clone()).await?)
    } else {
        None
    };
    let committed = if key.table == management::schema::RECORDS
        && native::value(&row, "kind") == Some("action")
        && native::value(&row, "state") == Some("applied")
        && local
    {
        // This is a DB-original reader, never an imported applied-claim decoder.
        let trusted = records::action_at_key_with_work(tx, key, work).await?;
        match trusted {
            Some(trusted) => Some(
                store_json(
                    tx,
                    &format!("{id}:committed"),
                    &header,
                    trusted.action_view(),
                    budget,
                )
                .await?,
            ),
            None => return Err(ManagementError::InvalidEvidence),
        }
    } else {
        None
    };
    let review = if let Some(review) = review {
        header.catalog = review.archive().schema_versions().to_vec();
        header.request = serde_json::to_string(review.request())
            .map_err(|_| ManagementError::InvalidEvidence)?;
        header.configuration = serde_json::to_string(&review.planning_view().configuration)
            .map_err(|_| ManagementError::InvalidEvidence)?;
        header.acknowledgements = review.candidate_item_ids().unwrap_or(&[]).to_vec();
        let mut rows = Vec::new();
        for (table, row) in review.rows().all_rows() {
            let key = NativeKey {
                table: table.table.clone(),
                values: table
                    .primary_key
                    .iter()
                    .map(|column| {
                        row.cell(column)
                            .map(str::to_owned)
                            .ok_or(ManagementError::InvalidEvidence)
                    })
                    .collect::<Result<_>>()?,
            };
            let original = store_json(
                tx,
                &format!("{id}:row:{}", rows.len()),
                &header,
                &row,
                budget,
            )
            .await?;
            rows.push(RowReference {
                key,
                original,
                review: None,
                recorded: None,
            });
        }
        Some(ReviewIndex {
            anchor: None,
            core_identity: None,
            catalog: review.archive().schema_versions().to_vec(),
            database_scope: review.scope().into(),
            database_identity: review.identity().into(),
            principal: review.principal().into(),
            request: store_json(
                tx,
                &format!("{id}:request"),
                &header,
                review.request(),
                budget,
            )
            .await?,
            entries: store_json(
                tx,
                &format!("{id}:entries"),
                &header,
                review.entries(),
                budget,
            )
            .await?,
            summary: store_json(
                tx,
                &format!("{id}:summary"),
                &header,
                review.summary(),
                budget,
            )
            .await?,
            snapshot: store_json(
                tx,
                &format!("{id}:snapshot"),
                &header,
                review.snapshot(),
                budget,
            )
            .await?,
            migration_points: store_json(
                tx,
                &format!("{id}:points"),
                &header,
                review.migration_points(),
                budget,
            )
            .await?,
            rows,
            staged_at: review.staged_at(),
            expires_at: review.expires_at(),
            confirmed_at: legacy::confirmation_time(native::value(&row, "binding"))?,
            acknowledgements: review.candidate_item_ids().map(<[String]>::to_vec),
        })
    } else {
        None
    };
    let original = store_json(tx, &format!("{id}:original"), &header, &row, budget).await?;
    let index = ConvertedSource {
        source: key.clone(),
        source_generation: generation,
        original,
        review,
        committed,
        historical: false,
        recorded: None,
    };
    store_json(tx, &id, &header, &index, budget).await?;
    Ok(())
}
