use sea_orm::{DbErr, Statement, Value};
use sqlx::{Arguments, postgres::PgArguments};

pub(super) fn arguments(statement: &Statement) -> Result<PgArguments, DbErr> {
    let mut args = PgArguments::default();
    if let Some(values) = &statement.values {
        for value in &values.0 {
            let result = match value {
                Value::Bool(v) => args.add(*v),
                Value::TinyInt(v) => args.add(v.map(i16::from)),
                Value::SmallInt(v) => args.add(*v),
                Value::Int(v) => args.add(*v),
                Value::BigInt(v) => args.add(*v),
                Value::TinyUnsigned(v) => args.add(v.map(i16::from)),
                Value::SmallUnsigned(v) => args.add(v.map(i32::from)),
                Value::Unsigned(v) => args.add(v.map(i64::from)),
                Value::BigUnsigned(v) => args.add(
                    v.map(i64::try_from)
                        .transpose()
                        .map_err(|_| super::failed())?,
                ),
                Value::Float(v) => args.add(*v),
                Value::Double(v) => args.add(*v),
                Value::String(v) => args.add(v.as_deref().cloned()),
                Value::Char(v) => args.add(v.map(|v| v.to_string())),
                Value::Bytes(v) => args.add(v.as_deref().cloned()),
                Value::Json(v) => args.add(v.as_deref().cloned()),
                Value::ChronoDate(v) => args.add(v.as_deref().copied()),
                Value::ChronoTime(v) => args.add(v.as_deref().copied()),
                Value::ChronoDateTime(v) => args.add(v.as_deref().copied()),
                Value::ChronoDateTimeUtc(v) => args.add(v.as_deref().copied()),
                Value::ChronoDateTimeLocal(v) => args.add(v.as_deref().copied()),
                Value::ChronoDateTimeWithTimeZone(v) => args.add(v.as_deref().copied()),
                Value::Uuid(v) => args.add(v.as_deref().copied()),
                _ => return Err(super::failed()),
            };
            result.map_err(|_| super::failed())?;
        }
    }
    Ok(args)
}
