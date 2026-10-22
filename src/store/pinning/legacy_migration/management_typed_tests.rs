use super::{management_catalog_tests::receipt_rows, migration};
use crate::store::pinning::management::{
    actions::{job_authority, provider_points},
    native, records, schema,
};
use sea_orm::{ConnectionTrait, EntityTrait, Statement, TransactionTrait};

#[path = "management_typed_fixture.rs"]
mod fixture;
use super::management_raw_tests::fixture as retired_fixture;
#[path = "live_budget_tests.rs"]
mod live_budget_tests;
#[path = "loaded_job_receipt_tests.rs"]
mod loaded_job_receipt_tests;

async fn complete(
    db: &sea_orm::DatabaseConnection,
    mut declaration: migration::Declaration,
) -> migration::Declaration {
    let versions = migration::snapshot::schema(db, false).await.unwrap();
    let before = migration::snapshot::read(db, &versions).await.unwrap();
    let closure = migration::management_closure::collect(db, &before, &declaration, &versions)
        .await
        .unwrap();
    for ns in &mut declaration.namespaces {
        ns.bindings = migration::projection::bindings(&before, ns, &closure).unwrap();
    }
    declaration
}

fn change(ns: &migration::Namespace) -> provider_points::NamespaceChange {
    provider_points::NamespaceChange {
        source: provider_points::Domain {
            allocation: ns.source.clone(),
            backend: ns.backend.clone(),
            scope: ns.scope.clone(),
            resource_type: ns.resource_type,
        },
        destination: provider_points::Domain {
            allocation: ns.target.clone(),
            backend: ns.backend.clone(),
            scope: ns.scope.clone(),
            resource_type: ns.resource_type,
        },
        destination_route: ns.target_route.clone(),
    }
}

#[tokio::test]
async fn staged_records_require_complete_typed_bindings_and_keep_original_frames() {
    for (phase, class) in [(fixture::Phase::Staged, provider_points::RecordClass::Staged)] {
        let fixture::Fixture {
            directory,
            server,
            db,
            config,
            auth,
            binding,
            declaration,
        } = fixture::fixture(phase).await;
        let before = native::capture(&db).await.unwrap();
        assert!(
            migration::inventory(&db, &declaration).await.is_err(),
            "real source references require complete bindings"
        );
        assert!(before == native::capture(&db).await.unwrap());
        let declaration = complete(&db, declaration).await;
        assert!(
            declaration.namespaces[0]
                .bindings
                .iter()
                .any(|binding| binding.table == schema::RECORDS && binding.pointer.is_some())
        );
        let plan = migration::inventory(&db, &declaration).await.unwrap();
        let original = plan
            .before
            .iter()
            .find(|table| table.table == schema::RECORDS)
            .unwrap()
            .clone();
        let row = original
            .rows
            .iter()
            .find(|row| row["plan_id"].as_deref() == Some("typed-plan"))
            .unwrap();
        let key = native::NativeKey {
            table: original.table.clone(),
            values: original
                .primary_key
                .iter()
                .map(|column| row[column].clone().unwrap())
                .collect(),
        };
        let inspected =
            provider_points::inspect(&db, &key, row, &change(&declaration.namespaces[0]))
                .await
                .unwrap();
        assert!(inspected.class() == class);
        assert!(!inspected.points().is_empty());
        let calls = server.received_requests().await.unwrap().len();
        let applied = migration::apply(&db, &declaration, &plan).await.unwrap();
        assert_eq!(
            migration::apply(&db, &declaration, &plan).await.unwrap(),
            applied
        );
        let current = migration::snapshot::read(&db, &plan.schema).await.unwrap();
        assert_eq!(
            current
                .iter()
                .find(|table| table.table == schema::RECORDS)
                .unwrap(),
            &original
        );
        let tx = db.begin().await.unwrap();
        assert!(
            records::revalidate_confirmed_in_transaction(&tx, &config, &auth, &binding)
                .await
                .is_err(),
            "R26 operational changes naturally invalidate the prepared full frame"
        );
        tx.rollback().await.unwrap();
        let items = receipt_rows(&db).await;
        assert!(
            items
                .iter()
                .filter(|row| row["table_kind"].as_deref() == Some(schema::RECORDS))
                .any(|row| row["bindings"].as_deref() != Some("[]"))
        );
        migration::rollback(&db, &applied.id, &plan).await.unwrap();
        assert_eq!(
            migration::snapshot::read(&db, &plan.schema).await.unwrap(),
            plan.before
        );
        assert_eq!(server.received_requests().await.unwrap().len(), calls);
        db.close().await.unwrap();
        directory.close().unwrap();
    }
}

#[tokio::test]
async fn applied_jobs_keep_exact_original_authority_and_gain_only_operational_point_projection() {
    let fixture::Fixture {
        directory,
        server,
        db,
        config: _,
        auth: _,
        binding: _,
        declaration,
    } = fixture::fixture(fixture::Phase::Applied).await;
    let declaration = complete(&db, declaration).await;
    let plan = migration::inventory(&db, &declaration).await.unwrap();
    let before = native::capture(&db).await.unwrap();
    let calls = server.received_requests().await.unwrap().len();
    let jobs = crate::store::entities::pin_job::Entity::find()
        .all(&db)
        .await
        .unwrap();
    assert!(!jobs.is_empty());
    for job in &jobs {
        assert!(
            declaration.namespaces[0]
                .bindings
                .iter()
                .any(|binding| binding.table == "pin_jobs"
                    && binding.key.get("id") == Some(&job.id)
                    && binding.historical_route.is_some())
        );
        assert!(
            job_authority::check_before_call(&db, job)
                .await
                .unwrap()
                .is_some()
        );
    }
    let applied = migration::apply(&db, &declaration, &plan).await.unwrap();
    let current = crate::store::entities::pin_job::Entity::find()
        .all(&db)
        .await
        .unwrap();
    for job in &current {
        let original = jobs.iter().find(|original| original.id == job.id).unwrap();
        let point = migration::resolve_legacy_binding(
            &db,
            "pin_jobs",
            &[("id".into(), job.id.clone())].into(),
            &original.provider,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(point.operational_key, job.provider);
        let authority = job_authority::lookup(&db, job).await.unwrap().unwrap();
        assert_eq!(
            authority.provider, original.provider,
            "stored authority is not rewritten"
        );
        assert_eq!(point.historical_route.as_ref(), Some(&authority.route));
        assert!(
            !migration::job_io_allowed(&db, job).await.unwrap(),
            "R26 does not reissue old submit/unpin authority"
        );
    }
    let after = native::capture(&db).await.unwrap();
    for table in [schema::RECORDS, schema::RETIREMENTS] {
        assert!(before.table(table) == after.table(table));
        for row in &plan
            .before
            .iter()
            .find(|image| image.table == table)
            .unwrap()
            .rows
        {
            let shape = before.table(table).unwrap();
            let key = shape
                .primary_key
                .iter()
                .map(|column| row[column].as_deref().unwrap())
                .collect::<Vec<_>>();
            for column in &shape.columns {
                assert_eq!(
                    before.original_value(table, &key, column),
                    after.original_value(table, &key, column)
                );
            }
        }
    }
    assert_eq!(
        migration::apply(&db, &declaration, &plan).await.unwrap(),
        applied
    );
    assert_eq!(server.received_requests().await.unwrap().len(), calls);
    db.close().await.unwrap();
    directory.close().unwrap();
}

#[tokio::test]
async fn typed_record_original_authentication_and_complete_locator_bindings_are_not_optional() {
    let fixture::Fixture {
        directory,
        server: _,
        db,
        config: _,
        auth: _,
        binding: _,
        declaration,
    } = fixture::fixture(fixture::Phase::Staged).await;
    let declaration = complete(&db, declaration).await;
    let plan = migration::inventory(&db, &declaration).await.unwrap();
    let mut missing = declaration.clone();
    let index = missing.namespaces[0]
        .bindings
        .iter()
        .position(|binding| binding.table == schema::RECORDS && binding.pointer.is_some())
        .unwrap();
    missing.namespaces[0].bindings.remove(index);
    assert!(migration::inventory(&db, &missing).await.is_err());
    let key = native::NativeKey {
        table: schema::RECORDS.into(),
        values: vec![
            plan.before
                .iter()
                .find(|table| table.table == schema::RECORDS)
                .unwrap()
                .rows[0]["database_scope"]
                .clone()
                .unwrap(),
            "typed-plan".into(),
            "object".into(),
            "action".into(),
        ],
    };
    let mut wrong = provider_points::original_record(&db, &key)
        .await
        .unwrap()
        .unwrap();
    wrong.insert("binding".into(), Some("{}".into()));
    let ns = &declaration.namespaces[0];
    assert!(
        provider_points::inspect(&db, &key, &wrong, &change(ns))
            .await
            .is_err()
    );
    db.execute_unprepared(
        "UPDATE pin_management_records SET principal='foreign-principal' WHERE kind='action'",
    )
    .await
    .unwrap();
    let changed = migration::snapshot::read(&db, &plan.schema).await.unwrap();
    assert!(migration::inventory(&db, &declaration).await.is_err());
    assert!(migration::apply(&db, &declaration, &plan).await.is_err());
    assert_eq!(
        migration::snapshot::read(&db, &plan.schema).await.unwrap(),
        changed
    );
    assert!(
        migration::snapshot::frontier(&db, &plan.schema)
            .await
            .unwrap()
            .is_empty()
    );
    db.close().await.unwrap();
    directory.close().unwrap();
}

#[tokio::test]
async fn unrelated_valid_retirement_is_preserved_but_same_canonical_target_cannot_bypass_it() {
    let retired_fixture::Fixture {
        directory,
        db,
        server: _server,
        declaration,
    } = retired_fixture::fixture().await;
    let mut unknown = declaration.namespaces[0].target_route.clone();
    unknown.provider_id = "legacy:old".into();
    unknown.scope = "legacy:old".into();
    unknown.storage_domain = "legacy:old".into();
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO remote_pin_ledger(provider,cid,route,ownership,effect) VALUES ('old','historical',$1,'unknown','unknown')",
        [serde_json::to_string(&unknown).unwrap().into()])).await.unwrap();
    let declaration = complete(&db, declaration).await;
    let open = migration::inventory(&db, &declaration).await.unwrap();
    let retired = open
        .before
        .iter()
        .find(|table| table.table == schema::RETIREMENTS)
        .unwrap()
        .rows[0]["allocation_domain"]
        .clone()
        .unwrap();
    let routes = open
        .before
        .iter()
        .find(|table| table.table == "pin_provider_routes")
        .unwrap();
    let original: crate::pinning::identity::ProviderRouteSnapshot = serde_json::from_str(
        routes
            .rows
            .iter()
            .find(|row| row["provider"].as_deref() == Some(&retired))
            .unwrap()["snapshot"]
            .as_deref()
            .unwrap(),
    )
    .unwrap();
    let mut closed = declaration.clone();
    closed.namespaces[0].target = retired.clone();
    closed.namespaces[0].scope = original.scope.clone();
    closed.namespaces[0].backend = original.backend.clone();
    closed.namespaces[0].resource_type = original.resource_type();
    closed.namespaces[0].target_route = original.clone();
    assert!(migration::inventory(&db, &closed).await.is_err());
    let tx = db.begin().await.unwrap();
    migration::fence::lock_before_routes(&tx, &open.schema)
        .await
        .unwrap();
    migration::fence::lock_from_routes(&tx, &open.schema)
        .await
        .unwrap();
    migration::fence::final_authority(&tx).await.unwrap();
    let management =
        migration::management_closure::collect(&tx, &open.before, &closed, &open.schema)
            .await
            .unwrap();
    let projected = migration::projection::project(&open.before, &closed, &management).unwrap();
    assert!(
        crate::store::pinning::management::admission::check_r26_open(&tx, &projected.admission)
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    let mut other_type = original.clone();
    other_type.api_profile = "rpc".into();
    crate::store::pinning::management::admission::check_open(&db, &[other_type])
        .await
        .unwrap();
    assert_eq!(
        migration::snapshot::read(&db, &open.schema).await.unwrap(),
        open.before
    );
    assert!(
        migration::snapshot::frontier(&db, &open.schema)
            .await
            .unwrap()
            .is_empty()
    );
    db.close().await.unwrap();
    directory.close().unwrap();
}

#[tokio::test]
async fn unrelated_valid_records_do_not_narrow_the_existing_r26_legacy_source_bound() {
    let retired_fixture::Fixture {
        directory,
        db,
        server: _server,
        mut declaration,
    } = retired_fixture::fixture().await;
    let source = "a".repeat(300);
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "UPDATE pin_provider_usage SET provider=$1 WHERE provider='old'",
        [source.clone().into()],
    ))
    .await
    .unwrap();
    let ns = &mut declaration.namespaces[0];
    ns.source = source.clone();
    ns.bindings[0].provider = source.clone();
    ns.bindings[0].key.insert("provider".into(), source);
    migration::validate_declaration(&declaration).unwrap();
    // B's typed service must accept the original R26 bound, not truncate/hash
    // it or skip authentication merely because the records are unrelated.
    let plan = migration::inventory(&db, &declaration).await.unwrap();
    assert_eq!(plan.declaration.namespaces[0].bindings.len(), 1);
    let applied = migration::apply(&db, &declaration, &plan).await.unwrap();
    migration::rollback(&db, &applied.id, &plan).await.unwrap();
    assert_eq!(
        migration::snapshot::read(&db, &plan.schema).await.unwrap(),
        plan.before
    );
    db.close().await.unwrap();
    directory.close().unwrap();
}
