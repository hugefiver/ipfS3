//! Bidirectional keyset comparison of every relation consumed by the reducer.
use super::{spool::get, *};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, Value};
#[cfg(test)]
mod tests;

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Cursor {
    pub table: usize,
    pub after: Vec<String>,
}

struct Relation {
    table: &'static str,
    key: &'static [&'static str],
    columns: &'static [&'static str],
}
const DERIVED: &[Relation] = &[
    Relation {
        table: "facts",
        key: &["kind", "identity"],
        columns: &["kind", "identity", "payload"],
    },
    Relation {
        table: "quotes",
        key: &["identity"],
        columns: &[
            "identity",
            "remote_key",
            "cid",
            "bytes",
            "unknown",
            "inflight",
            "cleanup",
            "retained",
        ],
    },
    Relation {
        table: "target_edges",
        key: &["target_id"],
        columns: &[
            "target_id",
            "lease_id",
            "cid",
            "allocation",
            "logical_bytes",
            "active",
            "referenced",
        ],
    },
    Relation {
        table: "jobs",
        key: &["identity"],
        columns: &["identity", "allocation", "cid", "inflight", "cleanup"],
    },
    Relation {
        table: "cids",
        key: &["cid"],
        columns: &["cid"],
    },
    Relation {
        table: "resource_counts",
        key: &["resource_key"],
        columns: &["resource_key", "refs", "inflight", "cleanup"],
    },
    Relation {
        table: "allocation_attribution",
        key: &["allocation"],
        columns: &["allocation", "domain"],
    },
    Relation {
        table: "totals_progress",
        key: &["id"],
        columns: &["id", "stage", "after_key", "payload"],
    },
];
const UNION: &[Relation] = &[
    Relation {
        table: "quotes",
        key: &["identity"],
        columns: &[
            "identity",
            "remote_key",
            "cid",
            "bytes",
            "unknown",
            "inflight",
            "cleanup",
            "retained",
        ],
    },
    Relation {
        table: "cids",
        key: &["cid"],
        columns: &["cid"],
    },
    Relation {
        table: "transfers",
        key: &["identity"],
        columns: &["identity"],
    },
    Relation {
        table: "unknowns",
        key: &["identity"],
        columns: &["identity"],
    },
    Relation {
        table: "binding",
        key: &["id"],
        columns: &["id", "payload"],
    },
    Relation {
        table: "merged",
        key: &["inventory"],
        columns: &["inventory", "kind", "after_key", "complete"],
    },
    Relation {
        table: "output",
        key: &["id"],
        columns: &["id", "base", "summary", "stage", "after_key"],
    },
];

#[derive(PartialEq, Eq, Serialize)]
pub(super) struct Row {
    key: Vec<String>,
    cells: Vec<Option<String>>,
}

struct Page {
    rows: Vec<Row>,
    eof: bool,
}

fn page_query(relation: &Relation, after: &[String]) -> Result<Statement> {
    if !after.is_empty() && after.len() != relation.key.len() {
        return Err(ManagementError::InvalidEvidence);
    }
    let key = relation.key.join(",");
    let values = after
        .iter()
        .map(|value| {
            if matches!(relation.table, "totals_progress" | "binding" | "output") {
                value
                    .parse::<i64>()
                    .map(Value::from)
                    .map_err(|_| ManagementError::InvalidEvidence)
            } else {
                Ok(Value::from(value.clone()))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let predicate = if after.is_empty() {
        String::new()
    } else if after.len() == 1 {
        format!("WHERE {key}>$1")
    } else {
        format!(
            "WHERE ({key})>({})",
            (1..=after.len())
                .map(|i| format!("${i}"))
                .collect::<Vec<_>>()
                .join(",")
        )
    };
    let payload = format!(
        "json_array({})",
        relation
            .columns
            .iter()
            .map(|c| format!("CAST({c} AS TEXT)"))
            .collect::<Vec<_>>()
            .join(",")
    );
    let keys = format!(
        "json_array({})",
        relation
            .key
            .iter()
            .map(|c| format!("CAST({c} AS TEXT)"))
            .collect::<Vec<_>>()
            .join(",")
    );
    Ok(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        format!(
            "SELECT identity,CASE WHEN page_bytes<=1048576 THEN payload END AS payload,position,window_last,page_bytes FROM (SELECT *,MAX(position) OVER() AS window_last,SUM(length(CAST(identity AS BLOB))+length(CAST(payload AS BLOB))) OVER(ORDER BY position ROWS UNBOUNDED PRECEDING) AS page_bytes FROM (SELECT {keys} AS identity,{payload} AS payload,ROW_NUMBER() OVER(ORDER BY {key}) AS position FROM (SELECT * FROM {} {predicate} ORDER BY {key} LIMIT 256))) WHERE page_bytes<=1048576 OR position=1 ORDER BY position",
            relation.table
        ),
        values,
    ))
}

async fn page(db: &DatabaseConnection, relation: &Relation, after: &[String]) -> Result<Page> {
    let rows = db
        .query_all(page_query(relation, after)?)
        .await
        .map_err(|_| ManagementError::Database)?;
    let decoded = rows
        .iter()
        .map(|row| {
            Ok(Row {
                key: decode(&get::<String>(row, "identity")?)?,
                cells: decode(&get::<String>(row, "payload")?)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let eof = if let Some(row) = rows.last() {
        let window_last = get::<i64>(row, "window_last")?;
        window_last < 256
            && get::<i64>(row, "position")? == window_last
            && get::<i64>(row, "page_bytes")? < 1048576
    } else {
        true
    };
    Ok(Page { rows: decoded, eof })
}

/// The archive stores `next` only with the accepted page, including both EOFs.
pub(super) async fn compare(
    original: &DatabaseConnection,
    canonical: &DatabaseConnection,
    cursor: &Cursor,
    union: bool,
) -> Result<(Cursor, Vec<Row>, bool)> {
    super::cache_guard::check(original).await?;
    super::cache_guard::check(canonical).await?;
    let relations = if union { UNION } else { DERIVED };
    let Some(relation) = relations.get(cursor.table) else {
        return Ok((cursor.clone(), Vec::new(), true));
    };
    let left = page(original, relation, &cursor.after).await?;
    let right = page(canonical, relation, &cursor.after).await?;
    if left.rows != right.rows {
        return Err(ManagementError::Changed);
    }
    let mut next = cursor.clone();
    if left.eof && right.eof {
        next.table += 1;
        next.after.clear();
    } else if let Some(row) = left.rows.last() {
        next.after = row.key.clone();
    }
    let done = next.table == relations.len();
    Ok((next, right.rows, done))
}
