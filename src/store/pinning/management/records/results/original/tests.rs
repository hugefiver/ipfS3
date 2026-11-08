use super::*;
use crate::pinning::management::{Action, Capture};
use sea_orm::{ConnectionTrait, Statement};

#[tokio::test]
async fn stored_old_receipt_verifies_original_facts_before_projection() {
    let fixture =
        crate::store::pinning::management::actions::tests::fixture("old-facts-receipt").await;
    crate::store::pinning::management::actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    let frame = native::capture(&fixture.db).await.unwrap();
    let key = native::NativeKey {
        table: "pin_management_records".into(),
        values: vec![
            fixture.binding.database_scope.clone(),
            "old-facts-receipt".into(),
            "object".into(),
            "action".into(),
        ],
    };
    let mut row = frame
        .original_row(
            &key.table,
            &key.values.iter().map(String::as_str).collect::<Vec<_>>(),
        )
        .unwrap();
    let raw = native::value(&row, "result").unwrap();
    let body = raw.strip_prefix("S7R1\n").unwrap();
    let (original_action, facts, _): (&RawValue, &RawValue, &str) =
        serde_json::from_str(body).unwrap();
    let action: CommittedAction = serde_json::from_str(original_action.get()).unwrap();
    let Action::Create(intent) = &action.receipt.binding.request.action else {
        panic!("create receipt")
    };
    let Capture::Object(capture) = action.receipt.binding.capture.as_ref().unwrap() else {
        panic!("object capture")
    };
    let old_policy = format!(
        ",\"max_age_secs\":null,\"policy_id\":{}",
        serde_json::to_string(&intent.requirements.policy_id).unwrap()
    );
    let old_control = format!(
        "\"control_revision\":{},",
        serde_json::to_string(&capture.object.control_revision).unwrap()
    );
    let action_bytes = original_action
        .get()
        .replace(
            &old_policy,
            &format!(
                ",\"max_age_secs\":null,\"policy_revision\":\"{}\"",
                "a".repeat(64)
            ),
        )
        .replace(
            &old_control,
            &format!("{old_control}\"config_revision\":\"{}\",", "b".repeat(64)),
        );
    assert_ne!(action_bytes, original_action.get());
    #[derive(Serialize)]
    struct HistoricalAction<'a> {
        receipt: &'a RawValue,
        targets: &'a RawValue,
        #[serde(skip_serializing_if = "Option::is_none")]
        root_releases: Option<&'a RawValue>,
        jobs: &'a RawValue,
        migration_points: &'a RawValue,
        decision: &'a RawValue,
    }
    let fields: std::collections::BTreeMap<&str, &RawValue> =
        serde_json::from_str(&action_bytes).unwrap();
    let historical_action = HistoricalAction {
        receipt: fields["receipt"],
        targets: fields["targets"],
        root_releases: fields.get("root_releases").copied(),
        jobs: fields["jobs"],
        migration_points: fields["migration_points"],
        decision: fields["decision"],
    };
    let hash_input = serde_json::to_vec(&(
        native::value(&row, "database_scope").unwrap(),
        native::value(&row, "native_identity").unwrap(),
        native::value(&row, "principal").unwrap(),
        "old-facts-receipt",
        "object",
        historical_action,
        facts,
    ))
    .unwrap();
    let mut historical_digest = Sha256::new();
    historical_digest.update(b"stage7/facts/1\0");
    historical_digest.update(&hash_input);
    let hash = hex::encode(historical_digest.finalize());
    let historical = format!("S7R1\n[{action_bytes},{},\"{hash}\"]", facts.get());
    let historical_db = crate::store::connect_database("sqlite::memory:")
        .await
        .unwrap();
    crate::store::run_migrations(&historical_db).await.unwrap();
    row.insert("result".into(), Some(historical.clone()));
    let columns = row
        .keys()
        .map(|name| format!("\"{name}\""))
        .collect::<Vec<_>>()
        .join(",");
    let placeholders = (1..=row.len())
        .map(|index| format!("${index}"))
        .collect::<Vec<_>>()
        .join(",");
    historical_db
        .execute(Statement::from_sql_and_values(
            historical_db.get_database_backend(),
            format!("INSERT INTO pin_management_records ({columns}) VALUES ({placeholders})"),
            row.values().cloned().map(sea_orm::Value::from),
        ))
        .await
        .unwrap();
    let stored = historical_db.query_one(Statement::from_sql_and_values(historical_db.get_database_backend(),
        "SELECT result FROM pin_management_records WHERE plan_id=$1 AND item_id='object' AND kind='action'", ["old-facts-receipt".into()])).await.unwrap().unwrap();
    let stored = stored.try_get::<String>("", "result").unwrap();
    assert_eq!(stored, historical);
    row.insert("result".into(), Some(stored));
    let decoded = DecodeBudget::default().result(&row).unwrap();
    let super::super::DecodedResult::Facts(projected, _, retained_hash) = decoded.as_ref();
    assert_eq!(retained_hash, &hash);
    let Action::Create(intent) = &projected.receipt.binding.request.action else {
        panic!("create projection")
    };
    assert_eq!(intent.requirements.policy_id, None);
    let output = serde_json::to_value(projected).unwrap();
    assert!(
        output["receipt"]["binding"]["capture"]["object"]["object"]
            .get("config_revision")
            .is_none()
    );
    row.insert(
        "result".into(),
        Some(historical.replace(&"a".repeat(64), &"c".repeat(64))),
    );
    assert!(matches!(
        DecodeBudget::default().result(&row),
        Err(ManagementError::Confirmation)
    ));
}
