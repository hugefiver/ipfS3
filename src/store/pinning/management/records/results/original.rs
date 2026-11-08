//! Facts identities bind the stored representation before runtime DTO projection.
use super::super::*;
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WithoutState<'a> {
    #[serde(borrow)]
    receipt: &'a RawValue,
    #[serde(rename = "state", skip_serializing)]
    _state: serde::de::IgnoredAny,
    #[serde(borrow)]
    targets: &'a RawValue,
    #[serde(default, borrow, skip_serializing_if = "Option::is_none")]
    root_releases: Option<&'a RawValue>,
    #[serde(borrow)]
    jobs: &'a RawValue,
    #[serde(borrow)]
    migration_points: &'a RawValue,
    #[serde(borrow)]
    decision: &'a RawValue,
}

pub(crate) fn verify(row: &native::NativeRow, body: &str, work: &mut DecodeBudget) -> Result<()> {
    codec::charge_json_scan(body, work)?;
    let (action, facts, expected): (WithoutState<'_>, &RawValue, &str) =
        serde_json::from_str(body).map_err(|_| ManagementError::InvalidEvidence)?;
    if identity(row, &action, facts)? != expected {
        return Err(ManagementError::Confirmation);
    }
    Ok(())
}

fn identity(
    row: &native::NativeRow,
    action: &WithoutState<'_>,
    facts: &RawValue,
) -> Result<String> {
    let field = |column| native::value(row, column).ok_or(ManagementError::InvalidEvidence);
    struct Writer(Sha256);
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer(Sha256::new());
    writer.0.update(b"stage7/facts/1\0");
    serde_json::to_writer(
        &mut writer,
        &(
            field("database_scope")?,
            field("native_identity")?,
            field("principal")?,
            field("plan_id")?,
            field("item_id")?,
            action,
            facts,
        ),
    )
    .map_err(|_| ManagementError::InvalidEvidence)?;
    Ok(hex::encode(writer.0.finalize()))
}

#[cfg(test)]
mod tests;
