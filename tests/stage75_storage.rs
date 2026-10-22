use ipfs_s3_gateway::store::{
    self,
    pinning::management::{
        native::meter::Budget,
        native::scope::{self, ScopeSpec, Selector},
    },
};
use sea_orm::{ConnectionTrait, Database, DatabaseBackend, Statement, TransactionTrait};

#[tokio::test]
async fn partial_database_is_rejected_without_migration_writes() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared("CREATE TABLE buckets (name TEXT PRIMARY KEY)")
        .await
        .unwrap();
    assert!(store::run_migrations(&db).await.is_err());
    let rows = db
        .query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name",
        ))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn exact_membership_detects_phantom_but_not_unrelated_routes() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let spec = ScopeSpec::new(vec![Selector::Route {
        provider: "selected".into(),
    }])
    .unwrap();
    let tx = db.begin().await.unwrap();
    let before = scope::collect_scope(&tx, &spec, &mut Budget::default())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db.execute_unprepared("INSERT INTO pin_provider_routes VALUES ('unrelated','{}','private',0)")
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    scope::revalidate_scope(&tx, &before, &mut Budget::default())
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    db.execute_unprepared(
        "INSERT INTO pin_provider_routes VALUES ('selected','{  \"secret\": null }','private',0)",
    )
    .await
    .unwrap();
    let tx = db.begin().await.unwrap();
    assert!(
        scope::revalidate_scope(&tx, &before, &mut Budget::default())
            .await
            .is_err()
    );
    let after = scope::collect_scope(&tx, &spec, &mut Budget::default())
        .await
        .unwrap();
    assert_eq!(after.rows().len(), 1);
    assert_eq!(
        after.rows().values().next().unwrap()["snapshot"].as_deref(),
        Some("{  \"secret\": null }")
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn locator_deletion_cannot_hide_a_restrictive_candidate() {
    use store::pinning::management::{
        native::NativeKey,
        records::locator::{self, AuthoritySelector},
    };
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let route = stage75_route("route-provider");
    let domain = stage75_domain(&route);
    for (id, retired) in [("permit", false), ("restrict", true)] {
        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "INSERT INTO pin_provider_routes VALUES ($1,$2,$1,$3)",
            [
                id.into(),
                serde_json::to_string(&route).unwrap().into(),
                retired.into(),
            ],
        ))
        .await
        .unwrap();
    }
    let tx = db.begin().await.unwrap();
    assert!(
        locator::load_authority(
            &tx,
            &[AuthoritySelector::Job("ordinary".into())],
            &mut Budget::default()
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    let keys = ["permit", "restrict"].map(|id| NativeKey {
        table: "pin_provider_routes".into(),
        values: vec![id.into()],
    });
    for key in &keys {
        let tx = db.begin().await.unwrap();
        locator::rebuild_source(&tx, key, &mut Budget::default())
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    let tx = db.begin().await.unwrap();
    assert_eq!(
        locator::load_authority(
            &tx,
            &[AuthoritySelector::Domain(domain.clone())],
            &mut Budget::default()
        )
        .await
        .unwrap()
        .originals
        .len(),
        2
    );
    assert!(
        locator::load_authority(
            &tx,
            &[AuthoritySelector::Job("ordinary".into())],
            &mut Budget::default()
        )
        .await
        .unwrap()
        .originals
        .is_empty()
    );
    tx.rollback().await.unwrap();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "DELETE FROM pin_management_locator WHERE source_key=$1 AND selector_kind='domain'",
        [locator::source_key(&keys[1]).unwrap().into()],
    ))
    .await
    .unwrap();
    let tx = db.begin().await.unwrap();
    assert!(
        locator::load_authority(
            &tx,
            &[AuthoritySelector::Domain(domain.clone())],
            &mut Budget::default()
        )
        .await
        .is_err()
    );
    assert!(
        locator::load_authority(
            &tx,
            &[AuthoritySelector::Job("ordinary".into())],
            &mut Budget::default()
        )
        .await
        .unwrap()
        .originals
        .is_empty()
    );
    tx.rollback().await.unwrap();
    let tx = db.begin().await.unwrap();
    locator::rebuild_source(&tx, &keys[1], &mut Budget::default())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let tx = db.begin().await.unwrap();
    assert_eq!(
        locator::load_authority(
            &tx,
            &[AuthoritySelector::Domain(domain)],
            &mut Budget::default()
        )
        .await
        .unwrap()
        .originals
        .len(),
        2
    );
    tx.rollback().await.unwrap();
}

fn stage75_route(provider: &str) -> ipfs_s3_gateway::pinning::identity::ProviderRouteSnapshot {
    use ipfs_s3_gateway::pinning::identity::{CleanupMode, ProviderRouteSnapshot};
    ProviderRouteSnapshot {
        provider_id: provider.into(),
        backend: "noop".into(),
        scope: "isolated".into(),
        storage_domain: "remote".into(),
        credential_revision: 1,
        endpoint_revision: 1,
        secret_ref: Some("env:PRIVATE".into()),
        api_profile: "noop".into(),
        strategy: "cid".into(),
        cleanup: CleanupMode::Retain,
        confirmation: None,
    }
}
fn stage75_domain(route: &ipfs_s3_gateway::pinning::identity::ProviderRouteSnapshot) -> String {
    format!(
        "domain:{}",
        serde_json::to_string(&(&route.backend, &route.scope, route.resource_type())).unwrap()
    )
}

#[tokio::test]
async fn segmented_archive_preserves_private_bytes_and_null_and_is_immutable() {
    use store::pinning::management::records::segmented::{self, ArchiveHeader};
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let header = ArchiveHeader {
        database_scope: "original".into(),
        database_identity: "foreign-private".into(),
        principal: "operator".into(),
        catalog: vec!["original".into()],
        request: "{  \"private\":null }".into(),
        configuration: "{\"secret\":\"keep\"}".into(),
        acknowledgements: vec!["item".into()],
        created_at: "2026-01-01 01:02:03.123456".into(),
        expires_at: None,
    };
    let bytes = format!("[null,\"\",\"{}\"]", "私密".repeat(40000)).into_bytes();
    let tx = db.begin().await.unwrap();
    let reference = segmented::write_archive(
        &tx,
        "archive",
        &header,
        &mut bytes.as_slice(),
        &mut Budget::default(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let mut restored = Vec::new();
    let restored_header =
        segmented::read_archive(&db, &reference, &mut restored, &mut Budget::default())
            .await
            .unwrap();
    assert!(restored_header == header);
    assert_eq!(restored, bytes);
    assert!(
        db.execute_unprepared(
            "UPDATE pin_management_archive_chunks SET payload=X'00' WHERE ordinal=0"
        )
        .await
        .is_err()
    );
    assert!(
        db.execute_unprepared("DELETE FROM pin_management_archive_chunks WHERE ordinal=0")
            .await
            .is_err()
    );
    assert!(
        db.execute_unprepared(
            "INSERT INTO pin_management_archive_chunks VALUES ('archive',999,X'00')"
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn scan_drift_and_rollback_revision_are_observable() {
    use store::pinning::management::native::scan;
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let spec = ScopeSpec::new(vec![Selector::Route {
        provider: "p".into(),
    }])
    .unwrap();
    let tx = db.begin().await.unwrap();
    let checkpoint = scan::start_scan(&tx, spec, &mut Budget::default())
        .await
        .unwrap();
    let page = scan::scan_page(&tx, &checkpoint, &mut Budget::default())
        .await
        .unwrap();
    assert!(page.complete);
    tx.commit().await.unwrap();
    let tx = db.begin().await.unwrap();
    scope::writer_boundary(&tx).await.unwrap();
    tx.execute_unprepared("INSERT INTO pin_provider_routes VALUES ('p','{}','p',0)")
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let tx = db.begin().await.unwrap();
    scan::seal_scan(&tx, &page.checkpoint, &mut Budget::default())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db.execute_unprepared("INSERT INTO pin_provider_routes VALUES ('p','{}','p',0)")
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    assert!(
        scan::seal_scan(&tx, &page.checkpoint, &mut Budget::default())
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn missing_trigger_is_not_a_ready_schema() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    db.execute_unprepared("DROP TRIGGER s75_locator_delete")
        .await
        .unwrap();
    assert!(store::initialization::validate_existing(&db).await.is_err());
}

#[tokio::test]
async fn writer_fence_holds_real_file_lock_without_revision_or_pending_changes() {
    let directory = tempfile::Builder::new()
        .prefix("stage75-storage-")
        .tempdir_in(env!("CARGO_MANIFEST_DIR"))
        .unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("writer.db")
            .display()
            .to_string()
            .replace('\\', "/")
    );
    let mut options = sea_orm::ConnectOptions::new(url.clone());
    options.max_connections(1).map_sqlx_sqlite_opts(|o| {
        o.pragma("temp_store", "MEMORY")
            .busy_timeout(std::time::Duration::from_millis(50))
    });
    let a = Database::connect(options).await.unwrap();
    store::run_migrations(&a).await.unwrap();
    let mut options = sea_orm::ConnectOptions::new(url);
    options.max_connections(1).map_sqlx_sqlite_opts(|o| {
        o.pragma("temp_store", "MEMORY")
            .busy_timeout(std::time::Duration::from_millis(50))
    });
    let b = Database::connect(options).await.unwrap();
    for rollback in [false, true] {
        let before=a.query_one(Statement::from_string(DatabaseBackend::Sqlite,"SELECT revision FROM pin_management_revision WHERE source_table='pin_provider_routes'")).await.unwrap().unwrap().try_get::<i64>("","revision").unwrap();
        let tx = a.begin().await.unwrap();
        scope::writer_boundary(&tx).await.unwrap();
        let after=tx.query_one(Statement::from_string(DatabaseBackend::Sqlite,"SELECT revision FROM pin_management_revision WHERE source_table='pin_provider_routes'")).await.unwrap().unwrap().try_get::<i64>("","revision").unwrap();
        assert_eq!(before, after);
        assert!(
            b.execute_unprepared(
                "INSERT INTO pin_provider_routes VALUES ('blocked','{}','blocked',0)"
            )
            .await
            .is_err()
        );
        if rollback {
            tx.rollback().await.unwrap();
        } else {
            tx.commit().await.unwrap();
        }
        b.execute_unprepared("INSERT INTO pin_provider_routes VALUES ('blocked','{}','blocked',0)")
            .await
            .unwrap();
        b.execute_unprepared("DELETE FROM pin_provider_routes WHERE provider='blocked'")
            .await
            .unwrap();
    }
    b.close().await.unwrap();
    a.close().await.unwrap();
    directory.close().unwrap();
}

#[tokio::test]
async fn long_native_cell_streams_without_materializing_a_row() {
    use store::pinning::management::native::{NativeKey, cells};
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let text = "私密\0exact".repeat(200000);
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_provider_routes VALUES ('large',$1,'',0)",
        [text.clone().into()],
    ))
    .await
    .unwrap();
    let tx = db.begin().await.unwrap();
    let key = NativeKey {
        table: "pin_provider_routes".into(),
        values: vec!["large".into()],
    };
    let mut output = Vec::new();
    let mut budget = Budget::default();
    assert_eq!(
        cells::copy_cell(&tx, &key, "snapshot", &mut output, &mut budget)
            .await
            .unwrap(),
        Some(text.len() as u64)
    );
    assert_eq!(output, text.as_bytes());
    assert!(budget.peak_resident() <= 128 * 1024);
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn resource_family_reads_both_expected_cid_observation_memberships() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    db.execute_unprepared("INSERT INTO pin_submit_observations(id,job_id,submit_call,provider,expected_cid,route,remote_epoch,claim_until,effect,resources,outcome,needs_attention,started_at) VALUES ('submit','job',1,'p','cid','{}',1,'2026-01-01','unknown','[]','in_flight',1,'2026-01-01')").await.unwrap();
    db.execute_unprepared("INSERT INTO pin_replica_observations(sequence,job_id,provider,expected_cid,route,remote_epoch,claim_until,phase,resources,outcome,projectable,observed_at) VALUES (1,'job','p','cid','{}',1,'2026-01-01','submit','[]','unknown',0,'2026-01-01')").await.unwrap();
    let selector = Selector::ResourceFamily {
        provider: "p".into(),
        cid: "cid".into(),
    };
    let tx = db.begin().await.unwrap();
    let read = scope::collect_scope(
        &tx,
        &ScopeSpec::new(vec![selector.clone()]).unwrap(),
        &mut Budget::default(),
    )
    .await
    .unwrap();
    let members = &read.memberships()[&selector];
    assert!(
        members
            .iter()
            .any(|k| k.table == "pin_submit_observations" && k.values == ["submit"])
    );
    assert!(
        members
            .iter()
            .any(|k| k.table == "pin_replica_observations" && k.values == ["1"])
    );
    assert_eq!(members.len(), 2);
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn scoped_frame_retains_private_originals_null_and_empty_membership() {
    use store::pinning::management::native::NativeKey;
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let (scope_id, _) = scope::binding(&db).await.unwrap();
    let key = NativeKey {
        table: "pin_management_records".into(),
        values: vec![
            scope_id,
            "schema".into(),
            "identity".into(),
            "database_scope".into(),
        ],
    };
    let original = " { \"private\" : [null,\"\",\"秘密\"] } ";
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "UPDATE pin_management_records SET native_before=$1,binding='' WHERE kind='database_scope'",
        [original.into()],
    ))
    .await
    .unwrap();
    let empty = Selector::Route {
        provider: "absent".into(),
    };
    let tx = db.begin().await.unwrap();
    let mut budget = Budget::default();
    let read = scope::collect_scope(
        &tx,
        &ScopeSpec::new(vec![Selector::Exact(key.clone()), empty.clone()]).unwrap(),
        &mut budget,
    )
    .await
    .unwrap();
    let frame = read.into_native_frame(&mut budget).unwrap();
    let refs = key.values.iter().map(String::as_str).collect::<Vec<_>>();
    assert_eq!(frame.row(&key.table, &refs).unwrap()["native_before"], None);
    assert_eq!(
        frame.original_value(&key.table, &refs, "native_before"),
        Some(original)
    );
    assert_eq!(frame.original_value(&key.table, &refs, "binding"), Some(""));
    assert_eq!(frame.original_value(&key.table, &refs, "result"), None);
    assert!(
        frame
            .scope_evidence()
            .unwrap()
            .memberships
            .iter()
            .any(|m| m.selector == empty && m.keys.is_empty())
    );
    assert!(!serde_json::to_string(&frame).unwrap().contains("秘密"));
    scope::revalidate_frame_scope(&tx, &frame, &mut Budget::default())
        .await
        .unwrap();
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn canonical_domain_includes_current_and_legacy_operational_keys_without_route_scan() {
    use std::collections::BTreeMap;
    use store::pinning::{
        legacy_migration as legacy,
        management::{
            native::NativeKey,
            records::locator::{self, AuthoritySelector},
        },
    };
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let (database_scope, identity) = scope::binding(&db).await.unwrap();
    let route = stage75_route("route-provider");
    let domain = stage75_domain(&route);
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_provider_routes VALUES ('current-key',$1,'current',0)",
        [serde_json::to_string(&route).unwrap().into()],
    ))
    .await
    .unwrap();
    let binding = legacy::Binding {
        table: "remote_pins".into(),
        key: BTreeMap::from([
            ("provider".into(), "old-key".into()),
            ("cid".into(), "cid".into()),
        ]),
        provider: "old-key".into(),
        pointer: None,
        historical_route: Some(route.clone()),
    };
    let declaration = legacy::Declaration {
        version: 1,
        database_scope: database_scope.clone(),
        maintenance: legacy::Maintenance {
            all_writers_stopped: true,
            all_workers_drained: true,
        },
        ttl_seconds: 60,
        confirmation_file: None,
        rollback_receipt: None,
        namespaces: vec![legacy::Namespace {
            source: "old-key".into(),
            target: "current-key".into(),
            backend: route.backend.clone(),
            scope: route.scope.clone(),
            resource_type: route.resource_type(),
            binding_only: false,
            target_route: route.clone(),
            bindings: vec![binding.clone()],
        }],
    };
    let original: BTreeMap<String, Option<String>> = [
        ("provider", Some("old-key")),
        ("cid", Some("cid")),
        ("request_id", None),
        ("cid_size", Some("1")),
        ("status", Some("pinned")),
        ("epoch", Some("1")),
        ("failure_attempts", Some("0")),
        ("next_retry_at", None),
        ("last_failed_request_id", None),
        ("last_touched_at", Some("2026-01-01 00:00:00")),
        ("last_error_class", None),
        ("last_error_text", None),
    ]
    .into_iter()
    .map(|(k, v)| (k.into(), v.map(str::to_owned)))
    .collect();
    let mut after = original.clone();
    after.insert("provider".into(), Some("current-key".into()));
    let image = |row| legacy::TableImage {
        table: "remote_pins".into(),
        columns: original.keys().cloned().collect(),
        primary_key: vec!["provider".into(), "cid".into()],
        rows: vec![row],
    };
    let confirmation = legacy::Confirmation {
        version: 1,
        operation_id: "legacy-op".into(),
        rollback_of: None,
        database_scope: database_scope.clone(),
        database_identity: identity,
        schema: store::pinning::management::schema::validate(&db)
            .await
            .unwrap(),
        db_clock: "2026-01-01T00:00:00Z".into(),
        expires_at: "2026-01-01T00:01:00Z".into(),
        declaration: declaration.clone(),
        receipt_frontier: vec![],
        before: vec![image(original.clone())],
        after: vec![image(after.clone())],
    };
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO pin_legacy_migrations VALUES ('legacy-op','apply',NULL,$1,'2026-01-01T00:00:30Z',$2,$3,$4,1)",[database_scope.into(),legacy::SCHEMA.into(),serde_json::to_string(&declaration).unwrap().into(),serde_json::to_string(&confirmation).unwrap().into()])).await.unwrap();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_legacy_migration_items VALUES ('legacy-op',0,'remote_pins',$1,$2,$3,$4)",
        [
            serde_json::to_string(&binding.key).unwrap().into(),
            serde_json::to_string(&Some(original)).unwrap().into(),
            serde_json::to_string(&Some(after)).unwrap().into(),
            serde_json::to_string(&vec![binding]).unwrap().into(),
        ],
    ))
    .await
    .unwrap();
    let keys = [
        NativeKey {
            table: "pin_provider_routes".into(),
            values: vec!["current-key".into()],
        },
        NativeKey {
            table: "pin_legacy_migrations".into(),
            values: vec!["legacy-op".into()],
        },
        NativeKey {
            table: "pin_legacy_migration_items".into(),
            values: vec!["legacy-op".into(), "0".into()],
        },
    ];
    let tx = db.begin().await.unwrap();
    for key in &keys {
        locator::rebuild_source(&tx, key, &mut Budget::default())
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    let tx = db.begin().await.unwrap();
    let read = scope::collect_scope(
        &tx,
        &ScopeSpec::new(vec![Selector::RouteDomain {
            allocation_domain: domain.clone(),
        }])
        .unwrap(),
        &mut Budget::default(),
    )
    .await
    .unwrap();
    let operational = read.operational_keys(&domain).unwrap();
    assert!(
        operational.contains("old-key")
            && operational.contains("current-key")
            && operational.contains("route-provider")
            && operational.contains(&domain)
    );
    let authority = locator::discover_authority(
        &tx,
        &[
            AuthoritySelector::LegacyPoint(
                "remote_pins".into(),
                vec!["old-key".into(), "cid".into()],
                "old-key".into(),
            ),
            AuthoritySelector::Job("unmatched".into()),
        ],
        &mut Budget::default(),
    )
    .await
    .unwrap();
    assert!(
        authority
            .proof
            .memberships
            .iter()
            .any(|m| m.selector == AuthoritySelector::Job("unmatched".into())
                && m.sources.is_empty())
    );
    assert!(
        authority
            .proof
            .seals
            .iter()
            .all(|s| s.source_generation == s.sealed_source_generation
                && s.locator_generation == s.sealed_locator_generation)
    );
    assert!(
        authority.originals.contains_key(&keys[1]) && authority.originals.contains_key(&keys[2])
    );
    tx.rollback().await.unwrap();
    // Discovery is legal on a read-only connection and never acquires a writer.
    db.execute_unprepared("PRAGMA query_only=ON").await.unwrap();
    let tx = db.begin().await.unwrap();
    locator::discover_authority(
        &tx,
        &[AuthoritySelector::Domain(domain)],
        &mut Budget::default(),
    )
    .await
    .unwrap();
    assert!(locator::fence_authority(&tx).await.is_err());
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn preparation_version_locator_is_complete_and_keeps_original_expiry() {
    use store::pinning::management::{
        native::NativeKey,
        records::locator::{self, AuthoritySelector},
    };
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let (scope_id, identity) = scope::binding(&db).await.unwrap();
    let cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    let object = serde_json::json!({"id":"object","bucket":"bucket","key":"key","cid":cid,"metadata":" { \"private\":null } "});
    let version = serde_json::json!({"id":"version","object_id":"object","bucket":"bucket","key":"key","version_id":null,"kind":"object"});
    let result=serde_json::json!([{
        "identity":{"bucket":"bucket","key":"key","public_version":"null","private_version_id":"version","object_id":"object","cid":cid},
        "object_tuple":object,"version_tuple":version,
        "source":{"node_identity":"node","local_reads_only":true,"idle_timeout_millis":1000},
        "stored_bytes":1,"proof":"file_clean_eof","observed_at":"2026-01-01T00:00:00Z","expires_at":"2026-01-01T00:01:00Z"
    },"http://private-node/"]).to_string();
    let before = serde_json::json!([object, version]).to_string();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO pin_management_records VALUES ($1,'prepare','version','evidence_preparation','operator',$2,'{}',$3,$4,'[]','[]','prepared','2026-01-01T00:00:00Z','2026-01-01T00:01:00Z','2026-01-01T00:00:00Z')",[scope_id.clone().into(),identity.into(),before.clone().into(),result.clone().into()])).await.unwrap();
    let key = NativeKey {
        table: "pin_management_records".into(),
        values: vec![
            scope_id,
            "prepare".into(),
            "version".into(),
            "evidence_preparation".into(),
        ],
    };
    let tx = db.begin().await.unwrap();
    locator::rebuild_source(&tx, &key, &mut Budget::default())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let tx = db.begin().await.unwrap();
    let read = locator::load_authority(
        &tx,
        &[
            AuthoritySelector::ExactVersion("version".into()),
            AuthoritySelector::ExactVersion("missing".into()),
        ],
        &mut Budget::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        read.originals[&key]["native_before"].as_deref(),
        Some(before.as_str())
    );
    assert_eq!(
        read.originals[&key]["result"].as_deref(),
        Some(result.as_str())
    );
    assert_eq!(
        read.originals[&key]["expires_at"].as_deref(),
        Some("2026-01-01T00:01:00Z")
    );
    assert!(read.proof.memberships.iter().any(|m| m.selector
        == AuthoritySelector::ExactVersion("missing".into())
        && m.sources.is_empty()));
    let frame = store::pinning::management::native::NativeFrame::from_scoped_reads(
        &[],
        std::slice::from_ref(&read),
        &mut Budget::default(),
    )
    .unwrap();
    scope::revalidate_frame_scope(&tx, &frame, &mut Budget::default())
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "DELETE FROM pin_management_locator WHERE source_key=$1 AND selector_kind='version'",
        [locator::source_key(&key).unwrap().into()],
    ))
    .await
    .unwrap();
    let tx = db.begin().await.unwrap();
    assert!(
        locator::load_authority(
            &tx,
            &[AuthoritySelector::ExactVersion("version".into())],
            &mut Budget::default()
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn source_sealing_requires_all_original_same_plan_members() {
    use ipfs_s3_gateway::pinning::management::{
        Action, ItemRequest, PlanRequest, RouteIdentity, Snapshot, Subject, Summary,
    };
    use sha2::{Digest, Sha256};
    use store::pinning::management::{
        native::{self, NativeKey},
        records::locator::{self, AuthoritySelector},
    };
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let (scope_id, identity) = scope::binding(&db).await.unwrap();
    let versions = store::pinning::management::schema::validate(&db)
        .await
        .unwrap();
    let items = ["a", "b"]
        .into_iter()
        .map(|id| ItemRequest {
            item_id: id.into(),
            action: Action::Retire,
            subject: Subject::Provider {
                route: RouteIdentity::from_original(stage75_route("provider")),
            },
        })
        .collect::<Vec<_>>();
    let request = PlanRequest {
        plan_id: "same-plan".into(),
        items: items.clone(),
    };
    let entries=items.iter().map(|item|serde_json::json!({"id":format!("management-v1:same-plan:{}",item.item_id),"request":item,"status":"candidate","capture":null,"step":null})).collect::<Vec<_>>();
    let mut rest = serde_json::to_value(Snapshot::default()).unwrap();
    rest.as_object_mut().unwrap().remove("native_frame");
    let manifest = serde_json::to_string(&serde_json::json!([
        versions,
        native::registry()
            .iter()
            .map(|table| serde_json::json!([table.table, []]))
            .collect::<Vec<_>>()
    ]))
    .unwrap();
    let hash = |domain: &[u8], raw: &str| {
        let mut h = Sha256::new();
        h.update(domain);
        h.update(raw.as_bytes());
        hex::encode(h.finalize())
    };
    let manifest_hash = hash(b"stage7/manifest/1\0", &manifest);
    let core = serde_json::to_string(&serde_json::json!([
        request,
        entries,
        Summary::default(),
        rest,
        "2026-01-01T00:00:00Z",
        "2026-01-01T00:01:00Z",
        [],
        manifest_hash
    ]))
    .unwrap();
    let core_hash = hash(b"stage7/core/1\0", &core);
    let anchor = serde_json::json!([
        "pin_management_records",
        [scope_id, "same-plan", "a", "action"]
    ]);
    let head = serde_json::to_string(&serde_json::json!([
        scope_id,
        identity,
        "operator",
        anchor,
        versions,
        core_hash,
        manifest_hash,
        []
    ]))
    .unwrap();
    let before = format!(
        "S7F1\n{:08x}{:08x}{:08x}{:08x}\n\n{head}{core}{manifest}",
        head.len(),
        core.len(),
        manifest.len(),
        0
    );
    let binding = format!("S7B1\n{}", serde_json::json!([0, anchor, core_hash]));
    let pointer = format!(
        "S7L1\n{}",
        serde_json::json!([anchor, core_hash, manifest_hash])
    );
    let keys = ["a", "b"].map(|id| NativeKey {
        table: "pin_management_records".into(),
        values: vec![
            scope_id.clone(),
            "same-plan".into(),
            id.into(),
            "action".into(),
        ],
    });
    for (index, raw) in [before, pointer].into_iter().enumerate() {
        db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO pin_management_records VALUES ($1,'same-plan',$2,'action','operator',$3,$4,$5,NULL,'[]','[]','prepared','2026-01-01T00:00:00Z','2026-01-01T00:01:00Z',NULL)",[scope_id.clone().into(),keys[index].values[2].clone().into(),identity.clone().into(),binding.clone().into(),raw.into()])).await.unwrap();
        if index == 0 {
            let tx = db.begin().await.unwrap();
            assert!(
                locator::rebuild_source(&tx, &keys[0], &mut Budget::default())
                    .await
                    .is_err()
            );
            tx.rollback().await.unwrap();
        }
    }
    let tx = db.begin().await.unwrap();
    for key in &keys {
        locator::rebuild_source(&tx, key, &mut Budget::default())
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    let tx = db.begin().await.unwrap();
    let view = locator::load_authority(
        &tx,
        &[AuthoritySelector::RecordMembers(
            scope_id.clone(),
            "same-plan".into(),
        )],
        &mut Budget::default(),
    )
    .await
    .unwrap();
    assert_eq!(view.classifications[&keys[0]].ordered_plan_members, keys);
    assert!(
        view.classifications[&keys[1]]
            .dependencies
            .contains_key(&keys[0])
    );
    tx.rollback().await.unwrap();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "DELETE FROM pin_management_locator WHERE source_key=$1 AND selector_kind='record_members'",
        [locator::source_key(&keys[1]).unwrap().into()],
    ))
    .await
    .unwrap();
    let tx = db.begin().await.unwrap();
    assert!(
        locator::load_authority(
            &tx,
            &[AuthoritySelector::RecordMembers(
                scope_id,
                "same-plan".into()
            )],
            &mut Budget::default()
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
}
