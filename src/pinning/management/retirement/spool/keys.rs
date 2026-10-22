use super::*;

/// The query selects ordered identities with LIMIT 64. Measure that bounded
/// window before copying keys so a page never materializes oversized identities.
pub(in crate::pinning::management::retirement) async fn page_keys(
    db: &impl ConnectionTrait,
    query: &str,
    values: Vec<Value>,
) -> Result<Vec<String>> {
    let sizes = db
        .query_all(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            format!(
                "SELECT length(CAST(identity AS BLOB)) AS bytes FROM ({query}) ORDER BY identity"
            ),
            values.clone(),
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
    let mut bytes = 0usize;
    let mut count = 0;
    for size in &sizes {
        let next =
            usize::try_from(get::<i64>(size, "bytes")?).map_err(|_| ManagementError::Budget)?;
        if next > 1024 * 1024 - bytes {
            break;
        }
        bytes += next;
        count += 1;
    }
    if count == 0 {
        return if sizes.is_empty() {
            Ok(Vec::new())
        } else {
            Err(ManagementError::Budget)
        };
    }
    let rows = db
        .query_all(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            format!("SELECT identity FROM ({query}) ORDER BY identity LIMIT {count}"),
            values,
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
    if rows.len() != count {
        return Err(ManagementError::Changed);
    }
    rows.iter().map(|row| get(row, "identity")).collect()
}
