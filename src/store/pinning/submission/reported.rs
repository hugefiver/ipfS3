use super::*;
use crate::store::pinning::legacy_migration::ReceiptWork;

/// Discover the receipts whose raw or canonical report names one of these CIDs.
/// Only unusual encodings need application-side CID decoding.
pub(super) async fn sources<C: ConnectionTrait, W: ReceiptWork>(
    db: &C,
    table: &str,
    providers: &[&str],
    requested: &[&str],
    include_expected: bool,
    work: &W,
) -> AppResult<BTreeSet<(String, String, String)>> {
    let backend = db.get_database_backend();
    let mut values = Vec::new();
    let names = bind_admission_keys(backend, &mut values, providers);
    let wanted = bind_admission_keys(backend, &mut values, requested);
    let requested = requested.iter().copied().collect::<BTreeSet<_>>();
    let mut v0 = BTreeSet::new();
    let mut canonical = false;
    for &key in &requested {
        if let Ok(cid) = cid::Cid::try_from(key)
            && cid.version() == cid::Version::V1
            && cid.to_string() == key
        {
            canonical = true;
            if cid.codec() == 0x70
                && let Ok(old) = cid::Cid::new_v0(*cid.hash())
            {
                v0.insert(old.to_string());
            }
        }
    }
    let (scope, common) = report_elements(backend);
    let mut selected = format!("raw IN ({wanted})");
    if canonical {
        selected.push_str(&format!(
            " OR NOT ({common}) OR (substr(raw,1,1) IN ('B','b') AND lower(raw) IN ({wanted}))"
        ));
        if !v0.is_empty() {
            let keys = v0.iter().map(String::as_str).collect::<Vec<_>>();
            let old = bind_admission_keys(backend, &mut values, &keys);
            selected.push_str(&format!(" OR raw IN ({old})"));
        }
    }
    let mut sql = format!(
        "WITH outstanding AS (SELECT id,provider,expected_cid,resources FROM {table} \
         WHERE provider IN ({names}) AND needs_attention=TRUE AND expected_cid NOT IN ({wanted}) \
         AND resources<>'[]'), reports AS ({scope}) \
         SELECT DISTINCT id,provider,expected_cid,raw,payload FROM reports \
         WHERE {selected} OR payload IS NOT NULL"
    );
    if include_expected {
        sql.push_str(&format!(
            " UNION ALL SELECT id,provider,expected_cid,NULL,NULL FROM {table} \
            WHERE provider IN ({names}) AND needs_attention=TRUE AND expected_cid IN ({wanted})"
        ));
    }
    let mut matches = BTreeSet::new();
    for row in db
        .query_all(Statement::from_sql_and_values(backend, sql, values))
        .await?
    {
        if let Some(raw) = row.try_get::<Option<String>>("", "raw")? {
            work.cells(1, raw.len()).map_err(W::into_app)?;
            if !requested.contains(raw.as_str())
                && !canonical_resource_cid(&raw).is_ok_and(|key| requested.contains(key.as_str()))
            {
                continue;
            }
        } else if let Some(payload) = row.try_get::<Option<String>>("", "payload")? {
            work.cells(1, payload.len()).map_err(W::into_app)?;
            if !reported_cids(&payload).any(|key| requested.contains(key.as_str())) {
                continue;
            }
        }
        matches.insert((
            row.try_get("", "id")?,
            row.try_get("", "provider")?,
            row.try_get("", "expected_cid")?,
        ));
    }
    Ok(matches)
}

// Fixed-size base58 CIDv0 and canonical base32 SHA-256 raw/dag-pb/dag-cbor
// spelling are injective. Other historical spellings go through the CID parser.
fn report_elements(backend: sea_orm::DatabaseBackend) -> (&'static str, &'static str) {
    if backend == sea_orm::DatabaseBackend::Postgres {
        (
            // JSONB rejects some valid JSON text, e.g. a NUL escape in another field.
            "SELECT id,provider,expected_cid,\
               CASE WHEN jsonb_typeof(item #> '{resource,cid}')='string' THEN item #>> '{resource,cid}' END AS raw,\
               CASE WHEN item IS NULL AND resources IS JSON ARRAY AND NOT pg_input_is_valid(resources,'jsonb') \
                    THEN resources END AS payload \
             FROM outstanding LEFT JOIN LATERAL jsonb_array_elements(\
               CASE WHEN left(ltrim(resources,E' \\t\\n\\r'),1)='[' AND pg_input_is_valid(resources,'jsonb') \
                    THEN resources::jsonb END) AS e(item) ON TRUE",
            "raw ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$' OR \
             lower(raw) ~ '^baf(kre|ybe|yre)i[a-z2-7]{51}[aeimquy4]$'",
        )
    } else {
        (
            "SELECT o.id,o.provider,o.expected_cid,\
               json_extract(CASE WHEN e.type='object' THEN e.value END,'$.resource.cid') AS raw,CAST(NULL AS TEXT) AS payload \
             FROM outstanding o CROSS JOIN json_each(\
               CASE WHEN json_valid(o.resources) THEN \
                 CASE WHEN json_type(o.resources)='array' THEN o.resources END END) e \
             WHERE json_type(CASE WHEN e.type='object' THEN e.value END,'$.resource.cid')='text'",
            "(length(raw)=46 AND substr(raw,1,2)='Qm' AND raw NOT GLOB '*[^1-9A-HJ-NP-Za-km-z]*') OR \
             (length(raw)=59 AND lower(substr(raw,1,7)) IN ('bafkrei','bafybei','bafyrei') \
              AND lower(raw) NOT GLOB '*[^a-z2-7]*' AND lower(substr(raw,59,1)) IN ('a','e','i','m','q','u','y','4'))",
        )
    }
}
