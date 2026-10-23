//! Bounded pages in the private history spool, never a live membership authority.
use super::*;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, Value};

const ROWS: usize = 256;
const BYTES: usize = 1024 * 1024;

pub(super) async fn start(db: &DatabaseConnection, budget: &mut Budget) -> Result<()> {
    exec(db,"CREATE TABLE IF NOT EXISTS confirmation_members(table_name TEXT NOT NULL,sort_key BLOB NOT NULL,seen INTEGER NOT NULL,PRIMARY KEY(table_name,sort_key)) WITHOUT ROWID",vec![],budget).await?;
    exec(db,"CREATE INDEX IF NOT EXISTS confirmation_unseen ON confirmation_members(seen,table_name,sort_key)",vec![],budget).await?;
    budget.charge(1,0,0,0).map_err(storage)?;
    let removed = db.execute(Statement::from_string(DatabaseBackend::Sqlite,"DELETE FROM confirmation_members")).await.map_err(invalid)?;
    budget.charge(0,removed.rows_affected(),0,0).map_err(storage)
}

fn page_len(keys: &[NativeKey], budget: &mut Budget) -> Result<usize> {
    let mut bytes = 0usize;
    let mut count = 0;
    for key in keys.iter().take(ROWS) {
        budget.check().map_err(storage)?;
        let size = key.values.iter().try_fold(key.table.len(), |size, value| {
            size.checked_add(value.len())
                .and_then(|size| size.checked_add(value.bytes().filter(|byte| *byte == 0).count()))
                .and_then(|size| size.checked_add(2))
                .ok_or(SafeError::LimitExceeded)
        })?;
        if size > BYTES { return Err(SafeError::LimitExceeded); }
        if size > BYTES - bytes { break; }
        bytes += size;
        count += 1;
    }
    Ok(count)
}

pub(super) async fn source(db: &DatabaseConnection, mut keys: &[NativeKey], budget: &mut Budget) -> Result<()> {
    while !keys.is_empty() {
        let count = page_len(keys, budget)?;
        // Params, driver copies, SQL text and row metadata coexist with the
        // already-accounted decoded JSON page, not with a whole key inventory.
        let _resident = budget.reserve(4 * BYTES + ROWS * 256).map_err(storage)?;
        let mut sql = String::from("INSERT INTO confirmation_members VALUES");
        let mut values = Vec::<Value>::with_capacity(2 * count);
        for (index,key) in keys[..count].iter().enumerate() {
            let order = sort_key(key);
            budget.charge(0,1,2,(key.table.len() + order.len()) as u64).map_err(storage)?;
            if index != 0 { sql.push(','); }
            sql.push_str(&format!("(${},${},0)", 2 * index + 1, 2 * index + 2));
            values.push(key.table.clone().into());
            values.push(order.into());
        }
        budget.charge(0,0,0,sql.len() as u64).map_err(storage)?;
        // A duplicate in this or any earlier Source page fails the insert.
        exec(db,&sql,values,budget).await?;
        keys = &keys[count..];
    }
    Ok(())
}

pub(super) async fn wire(db: &DatabaseConnection, mut keys: &[NativeKey], budget: &mut Budget) -> Result<()> {
    while !keys.is_empty() {
        let count = page_len(keys, budget)?;
        let _resident = budget.reserve(4 * BYTES + ROWS * 256).map_err(storage)?;
        budget.charge(1,0,0,0).map_err(storage)?;
        // Both the row and aggregate byte limits apply inside SQLite before any
        // strings/BLOBs cross the connection. A valid Wire page equals this exact
        // remaining ordered prefix, not just an arbitrary subset of Source.
        let rows = db.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "SELECT table_name,sort_key FROM (SELECT table_name,sort_key,SUM(length(CAST(table_name AS BLOB))+length(sort_key)) OVER (ORDER BY table_name,sort_key ROWS UNBOUNDED PRECEDING) AS page_bytes FROM (SELECT table_name,sort_key FROM confirmation_members WHERE seen=0 ORDER BY table_name,sort_key LIMIT $1)) WHERE page_bytes<=$2 ORDER BY table_name,sort_key",
            vec![(count as i64).into(),(BYTES as i64).into()])).await.map_err(invalid)?;
        budget.check().map_err(storage)?;
        let mut last = None;
        for (index,row) in rows.iter().enumerate() {
            let table: String = row.try_get("","table_name").map_err(invalid)?;
            let order: Vec<u8> = row.try_get("","sort_key").map_err(invalid)?;
            budget.charge(0,1,2,(table.len() + order.len()) as u64).map_err(storage)?;
            let expected = &keys[index];
            let expected_order = sort_key(expected);
            budget.charge(0,0,0,expected_order.len() as u64).map_err(storage)?;
            if table != expected.table || order != expected_order { return Err(SafeError::InvalidInput); }
            last = Some((table,order));
        }
        if rows.len() != count { return Err(SafeError::InvalidInput); }
        let (table,order) = last.ok_or(SafeError::InvalidInput)?;
        budget.charge(1,count as u64,0,(table.len() + order.len()) as u64).map_err(storage)?;
        let updated = db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "UPDATE confirmation_members SET seen=1 WHERE seen=0 AND (table_name,sort_key)<=($1,$2)",
            vec![table.into(),order.into()])).await.map_err(invalid)?;
        if updated.rows_affected() != count as u64 { return Err(SafeError::InvalidInput); }
        budget.check().map_err(storage)?;
        keys = &keys[count..];
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(count: usize) -> Vec<NativeKey> {
        (0..count).map(|index| NativeKey {table:"objects".into(),values:vec![format!("key-{index:06}")]}).collect()
    }

    #[tokio::test]
    async fn membership_sql_grows_by_bounded_pages_and_checks_exact_prefixes() {
        let mut budget = files::stream::budget();
        let spool = Spool::create(&mut budget).await.unwrap();
        let mut measurements = Vec::new();
        for count in [1,200,600] {
            start(&spool.db,&mut budget).await.unwrap();
            let keys = keys(count);
            let before = budget.work.sql;
            source(&spool.db,&keys,&mut budget).await.unwrap();
            wire(&spool.db,&keys,&mut budget).await.unwrap();
            let queries = budget.work.sql - before;
            eprintln!("confirmation membership keys={count} queries={queries}");
            measurements.push(queries);
        }
        assert!(measurements[1] <= measurements[0] * 2, "one page must not introduce per-member SQL");
        assert!(measurements[2] <= measurements[0] * 4, "bounded-page SQL growth");
        start(&spool.db,&mut budget).await.unwrap();
        let keys = keys(600);
        source(&spool.db,&keys,&mut budget).await.unwrap();
        assert!(source(&spool.db,&keys[255..257],&mut budget).await.is_err());
        assert!(wire(&spool.db,&keys[1..201],&mut budget).await.is_err());
        wire(&spool.db,&keys[..200],&mut budget).await.unwrap();
        assert!(wire(&spool.db,&keys[..200],&mut budget).await.is_err());
        wire(&spool.db,&keys[200..],&mut budget).await.unwrap();
        assert!(wire(&spool.db,&keys[..1],&mut budget).await.is_err());
        spool.close().await.unwrap();
    }
}
