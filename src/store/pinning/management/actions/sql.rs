//! Exact indexed keys use native parameter types; raw cells remain text evidence.
use super::*;
use sea_orm::Value;

pub(super) fn exact_key(table: &native::NativeTable, key: &[&str]) -> Result<(String, Vec<Value>)> {
    if key.len() != table.primary_key.len() {
        return Err(ManagementError::InvalidEvidence);
    }
    let mut predicates = Vec::with_capacity(key.len());
    let mut values = Vec::with_capacity(key.len());
    for (column, value) in table.primary_key.iter().zip(key) {
        let definition = table
            .definitions
            .iter()
            .find(|definition| definition.name == *column)
            .ok_or(ManagementError::Schema)?;
        values.push(match definition.postgres_type.as_str() {
            "text" => (*value).into(),
            "bigint" => value
                .parse::<i64>()
                .map_err(|_| ManagementError::InvalidEvidence)?
                .into(),
            "integer" => value
                .parse::<i32>()
                .map_err(|_| ManagementError::InvalidEvidence)?
                .into(),
            "boolean" => match *value {
                "true" | "1" => true.into(),
                "false" | "0" => false.into(),
                _ => return Err(ManagementError::InvalidEvidence),
            },
            _ => return Err(ManagementError::Schema),
        });
        predicates.push(format!("\"{column}\"=${}", values.len()));
    }
    Ok((predicates.join(" AND "), values))
}
