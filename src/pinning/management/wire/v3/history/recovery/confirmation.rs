use super::*;
use crate::pinning::management::retirement::confirmation::evidence::{self, Evidence, Keys};
use std::io::BufReader;
mod declaration;
mod membership;

const HEADER_LIMIT: usize = 1024 * 1024;

/// Classification precedes integrity verification; it is never a fallback from
/// a failed ordinary archive. A claimed confirmation must pass the whole envelope.
pub(super) async fn declared(cells: &mut ArchiveCells<'_>, key: &NativeKey) -> Result<bool> {
    let _guard = cells.budget.reserve(2 * 65536 + 16384).map_err(storage)?;
    let mut probe = declaration::Probe::new();
    let sorted = sort_key(key);
    let row = one(&cells.spool.db,"SELECT length FROM cells WHERE table_name=$1 AND sort_key=$2 AND column_name='header'",vec![key.table.clone().into(),sorted.clone().into()],cells.budget).await?.ok_or(SafeError::InvalidInput)?;
    let Some(length) = row.try_get::<Option<i64>>("","length").map_err(invalid)? else {return Ok(false);};
    let length = u64::try_from(length).map_err(invalid)?;
    let mut offset = 0u64;
    while offset < length {
        let row = one(&cells.spool.db,"SELECT payload FROM parts WHERE table_name=$1 AND sort_key=$2 AND column_name='header' AND offset=$3",vec![key.table.clone().into(),sorted.clone().into(),i64::try_from(offset).map_err(invalid)?.into()],cells.budget).await?.ok_or(SafeError::InvalidInput)?;
        let bytes: Vec<u8> = row.try_get("","payload").map_err(invalid)?;
        if bytes.is_empty() || bytes.len()>65536 || bytes.len() as u64>length-offset {return Err(SafeError::InvalidInput);}
        cells.budget.charge(0,0,0,bytes.len() as u64).map_err(storage)?;
        if let Some(declared) = probe.feed(&bytes,cells.budget)? {return Ok(declared);}
        offset += bytes.len() as u64;
    }
    cells.budget.check().map_err(storage)?;
    // An incomplete prefix without a declaration remains opaque historical data.
    Ok(false)
}

pub(super) async fn verify(cells: &mut ArchiveCells<'_>, key: &NativeKey) -> Result<()> {
    let _envelope = cells.budget.reserve(2 * HEADER_LIMIT).map_err(storage)?;
    let header = cells.required(key, "header", HEADER_LIMIT).await?;
    let state = cells.required(key, "digest", HEADER_LIMIT).await?;
    let scope = cells.required(key, "database_scope", 4096).await?;
    let identity = cells.required(key, "native_identity", 4096).await?;
    let sealed = match cells.required(key, "state", 16).await?.as_str() {
        "sealed" => true,
        "staging" => false,
        _ => return Err(SafeError::InvalidInput),
    };
    if cells.required(key, "format", 16).await? != "S75A1" { return Err(SafeError::InvalidInput); }
    let id = &key.values[0];
    let mut evidence = Evidence::open(header.as_bytes(), state.as_bytes(), id, &scope, &identity, sealed, cells.budget)?;
    drop((header,state,scope,identity));
    drop(_envelope);
    let chunks: i64 = cells.required(key, "chunks", 32).await?.parse().map_err(invalid)?;
    let bytes: i64 = cells.required(key, "bytes", 32).await?.parse().map_err(invalid)?;
    if chunks < 0 || bytes < 0 { return Err(SafeError::InvalidInput); }
    let row = one(&cells.spool.db,
        "SELECT COUNT(*) AS n,COUNT(DISTINCT ordinal) AS unique_n,MIN(ordinal) AS first,MAX(ordinal) AS last FROM rows WHERE table_name='pin_management_archive_chunks' AND archive_id=$1",
        vec![id.clone().into()], cells.budget).await?.ok_or(SafeError::InvalidInput)?;
    cells.budget.charge(0,1,4,32).map_err(storage)?;
    if row.try_get::<i64>("", "n").map_err(invalid)? != chunks
        || row.try_get::<i64>("", "unique_n").map_err(invalid)? != chunks
        || chunks > 0 && (row.try_get::<Option<i64>>("", "first").map_err(invalid)? != Some(0) || row.try_get::<Option<i64>>("", "last").map_err(invalid)? != Some(chunks - 1))
    { return Err(SafeError::InvalidInput); }
    let mut file = files::stream::temporary().map_err(invalid)?;
    let mut total = 0i64;
    let _chunk_memory = cells.budget.reserve(4 * 65536).map_err(storage)?;
    for ordinal in 0..chunks {
        let key = NativeKey { table: "pin_management_archive_chunks".into(), values: vec![id.clone(),ordinal.to_string()] };
        let encoded = cells.required(&key,"payload",2 * 65536).await?;
        if encoded.is_empty() || !encoded.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) { return Err(SafeError::InvalidInput); }
        cells.budget.charge(0,0,0,encoded.len() as u64).map_err(storage)?;
        let payload = hex::decode(encoded).map_err(invalid)?;
        total = total.checked_add(payload.len() as i64).ok_or(SafeError::LimitExceeded)?;
        cells.budget.charge(0,0,0,payload.len() as u64).map_err(storage)?;
        file.write_all(&payload).map_err(invalid)?;
    }
    if total != bytes { return Err(SafeError::InvalidInput); }
    drop(_chunk_memory);
    file.seek(SeekFrom::Start(0)).map_err(invalid)?;
    membership::start(&cells.spool.db, cells.budget).await?;
    let _memory = cells.budget.reserve(8 * 1024 * 1024 + 65536).map_err(storage)?;
    let mut input = BufReader::with_capacity(65536, file);
    let mut raw = Vec::with_capacity(8 * 1024 * 1024);
    while evidence::json::next(&mut input, &mut raw, cells.budget)? {
        let _parsed = evidence::json::preflight(&raw, cells.budget)?;
        match evidence.accept(&raw,cells.budget)? {
            Keys::Source(keys) => membership::source(&cells.spool.db, &keys, cells.budget).await?,
            Keys::Wire(keys) => membership::wire(&cells.spool.db, &keys, cells.budget).await?,
            Keys::None => {},
        }
    }
    evidence.finish()?;
    cells.budget.check().map_err(storage)
}
