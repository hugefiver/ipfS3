//! A committed Stage 7 Retain original, using its historical storage protocol.
use crate::legacy_import;
#[path = "stage75_source_catalog.rs"]
mod source_catalog;
use ipfs_s3_gateway::{
    pinning::management as model,
    store::{
        initialization::{self, history::legacy},
        pinning::management::{native, records, schema},
    },
};
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait, Value,
};
use sha2::{Digest, Sha256};

fn digest(domain: &[u8], raw: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(raw.as_bytes());
    hex::encode(hash.finalize())
}

pub async fn install(
    db: &DatabaseConnection,
    config: &model::local_config::ManagementConfig,
) -> model::PlanRequest {
    let backend = db.get_database_backend();
    for (name, table, _, _) in schema::storage::triggers(backend) {
        let sql = if backend == DatabaseBackend::Postgres {
            format!("DROP TRIGGER {name} ON \"{table}\"")
        } else {
            format!("DROP TRIGGER {name}")
        };
        db.execute_unprepared(&sql).await.unwrap();
        if backend == DatabaseBackend::Postgres {
            db.execute_unprepared(&format!("DROP FUNCTION {name}()"))
                .await
                .unwrap();
        }
    }
    for name in ["s75_applied_no_delete", "s75_applied_no_update"] {
        let sql = if backend == DatabaseBackend::Postgres {
            format!("DROP TRIGGER {name} ON pin_management_records")
        } else {
            format!("DROP TRIGGER {name}")
        };
        db.execute_unprepared(&sql).await.unwrap();
    }
    if backend == DatabaseBackend::Postgres {
        for table in native::registry() {
            db.execute_unprepared(&format!(
                "DROP TRIGGER s75_{}_truncate ON \"{}\"",
                table.table, table.table,
            ))
            .await
            .unwrap();
        }
    }
    for (name, _) in schema::storage::indexes() {
        db.execute_unprepared(&format!("DROP INDEX {name}"))
            .await
            .unwrap();
    }
    for (name, _) in schema::storage::tables(backend).into_iter().rev() {
        db.execute_unprepared(&format!("DROP TABLE {name}"))
            .await
            .unwrap();
    }
    if backend == DatabaseBackend::Postgres {
        for name in [
            "s75_no_truncate",
            "s75_immutable",
            "s75_archive_guard",
            "s75_chunk_guard",
            "s75_archive_id_claim",
        ] {
            db.execute_unprepared(&format!("DROP FUNCTION {name}()"))
                .await
                .unwrap();
        }
    }
    legacy_import::restore(db).await;
    if backend == DatabaseBackend::Sqlite {
        let tx = db.begin().await.unwrap();
        ipfs_s3_gateway::store::migrations::replace_sqlite_layout(
            &tx,
            ipfs_s3_gateway::store::migrations::history::M13,
        )
        .await;
        tx.commit().await.unwrap();
    } else {
        db.execute_unprepared("DELETE FROM seaql_migrations")
            .await
            .unwrap();
        for version in ipfs_s3_gateway::store::migrations::history::prefix(
            "m20261013_000001_zip_root_logical_release",
        )
        .unwrap()
        {
            db.execute(Statement::from_sql_and_values(
                backend,
                "INSERT INTO seaql_migrations(version,applied_at) VALUES($1,0)",
                [(*version).into()],
            ))
            .await
            .unwrap();
        }
    }
    let (scope, identity) = native::scope::binding(db).await.unwrap();
    let plan = "recorded-provider";
    let owner = (
        schema::RECORDS.to_owned(),
        vec![scope.clone(), plan.into(), "retire".into(), "action".into()],
    );
    let catalog = db
        .query_all(Statement::from_string(
            backend,
            "SELECT version FROM seaql_migrations ORDER BY version",
        ))
        .await
        .unwrap()
        .iter()
        .map(|row| row.try_get::<String>("", "version").unwrap())
        .collect::<Vec<_>>();
    let mut leaves = Vec::new();
    let mut tables = Vec::new();
    let mut frontier = Vec::new();
    for (ordinal, table) in source_catalog::tables(&catalog).await.iter().enumerate() {
        let projection = table
            .columns
            .iter()
            .map(|c| format!("CAST(\"{c}\" AS TEXT) AS \"{c}\""))
            .collect::<Vec<_>>()
            .join(",");
        let order = table
            .primary_key
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(",");
        let rows = db
            .query_all(Statement::from_string(
                backend,
                format!(
                    "SELECT {projection} FROM \"{}\" ORDER BY {order}",
                    table.table
                ),
            ))
            .await
            .unwrap();
        if ["pin_leases", "pin_lease_targets", "remote_pins"].contains(&table.table.as_str()) {
            assert!(rows.is_empty());
        }
        let mut versions = Vec::new();
        for row in rows {
            let key = table
                .primary_key
                .iter()
                .map(|c| row.try_get::<String>("", c).unwrap())
                .collect::<Vec<_>>();
            let cells = table
                .columns
                .iter()
                .map(|c| row.try_get::<Option<String>>("", c).unwrap())
                .collect::<Vec<_>>();
            if table.table == schema::RECORDS {
                assert_ne!(row.try_get::<String>("", "kind").unwrap(), "action");
            }
            let leaf = serde_json::to_string(&(ordinal, &key, cells)).unwrap();
            versions.push((
                key.clone(),
                owner.clone(),
                leaves.len(),
                leaf.len(),
                digest(b"stage7/leaf/1\0", &leaf),
            ));
            frontier.push(native::NativeKey {
                table: table.table.clone(),
                values: key,
            });
            leaves.push(leaf);
        }
        tables.push((table.table.clone(), versions));
    }
    frontier.sort();
    let manifest = serde_json::to_string(&(&catalog, tables)).unwrap();
    let manifest_hash = digest(b"stage7/manifest/1\0", &manifest);
    let original = config.pinning.providers[0].route_snapshot();
    let domain: String = db
        .query_one(Statement::from_sql_and_values(
            backend,
            "SELECT provider FROM pin_provider_routes WHERE snapshot=$1",
            [serde_json::to_string(&original).unwrap().into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "provider")
        .unwrap();
    let route = model::RouteSnapshot {
        identity: model::RouteIdentity::from_original(original.clone()),
        original: Some(original.clone()),
        available: true,
        allocation_allowed: true,
        cleanup: model::CleanupMode::Retain,
        exclusive_management: false,
    };
    let inventory = model::ProviderInventory {
        provider_id: original.provider_id.clone(),
        root_reference_ids: vec![],
        frontier,
        complete: true,
        attribution_complete: true,
        allocation_stopped: false,
        leases: vec![],
    };
    let request = model::PlanRequest {
        plan_id: plan.into(),
        items: vec![model::ItemRequest {
            item_id: "retire".into(),
            action: model::Action::Retire,
            subject: model::Subject::Provider {
                route: route.identity.clone(),
            },
        }],
    };
    let entry = legacy::PlanEntry {
        id: serde_json::from_value(serde_json::json!(format!("management-v1:{plan}:retire")))
            .unwrap(),
        request: request.items[0].clone(),
        status: model::PlanStatus::Ready,
        capture: Some(legacy::Capture::Provider(Box::new(
            legacy::RetirementCapture {
                inventory: inventory.clone(),
                routes: vec![route.clone()],
                targets: vec![],
                remotes: vec![],
                historical_remotes: vec![],
                root_references: vec![],
            },
        ))),
        step: Some(model::PlannedStep::Retire(model::RetirementStep {
            stop_new_allocations: true,
            needs_attention: false,
            leases: 0,
            retained: 0,
            unknown: 0,
            inflight: 0,
            cleanup_pending: 0,
            root_references: 0,
        })),
    };
    let now = chrono::Utc::now() - chrono::Duration::days(1);
    let now = if backend == DatabaseBackend::Postgres {
        chrono::DateTime::from_timestamp_micros(now.timestamp_micros()).unwrap()
    } else {
        now
    };
    let expires = now + chrono::Duration::minutes(5);
    let database_time = |time: chrono::DateTime<chrono::Utc>| -> Value {
        if backend == DatabaseBackend::Postgres {
            time.into()
        } else {
            time.to_rfc3339().into()
        }
    };
    let snapshot = legacy::Snapshot {
        observed_at: now.timestamp(),
        database_now: Some(now),
        configuration: Some(model::ConfigurationBinding {
            routes: config
                .pinning
                .providers
                .iter()
                .map(|p| p.route_snapshot())
                .collect(),
            policies: config.pinning.policies.clone(),
            limits: config
                .pinning
                .provider_limits
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            zip_rules: Some(config.zip_rules.clone()),
            source_node: config.source_node().map(str::to_owned),
        }),
        objects: vec![],
        routes: vec![route],
        targets: vec![],
        remotes: vec![],
        historical_remotes: vec![],
        unknown_resources: vec![],
        ledger_domains: vec![],
        inventories: vec![inventory],
        root_references: vec![],
        applied: vec![],
    };
    let core = serde_json::to_string(&(
        &request,
        vec![entry.clone()],
        model::Summary {
            items: 1,
            ..Default::default()
        },
        snapshot,
        now,
        expires,
        Vec::<records::MigrationPoint>::new(),
        &manifest_hash,
    ))
    .unwrap();
    let core_hash = digest(b"stage7/core/1\0", &core);
    let head = serde_json::to_string(&(
        &scope,
        &identity,
        "local_configuration",
        &owner,
        &catalog,
        &core_hash,
        &manifest_hash,
        leaves
            .iter()
            .map(|leaf| digest(b"stage7/leaf/1\0", leaf))
            .collect::<Vec<_>>(),
    ))
    .unwrap();
    let directory = leaves
        .iter()
        .map(|leaf| format!("{:08x}", leaf.len()))
        .collect::<String>();
    let before = format!(
        "S7F1\n{:08x}{:08x}{:08x}{:08x}\n{directory}\n{head}{core}{manifest}{}",
        head.len(),
        core.len(),
        manifest.len(),
        leaves.len(),
        leaves.concat()
    );
    let binding = format!(
        "S7B1\n{}",
        serde_json::to_string(&(1u8, &owner, &core_hash, now, Vec::<String>::new())).unwrap()
    );
    db.execute(Statement::from_sql_and_values(backend, "INSERT INTO pin_management_records(database_scope,plan_id,item_id,kind,native_identity,principal,binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at) VALUES($1,$2,'retire','action',$3,'local_configuration',$4,$5,NULL,'[]','[]','prepared',$6,$7,NULL)", [scope.clone().into(),plan.into(),identity.clone().into(),binding.clone().into(),before.into(),database_time(now),database_time(expires)])).await.unwrap();
    let tx = db.begin().await.unwrap();
    let retired = if backend == DatabaseBackend::Postgres {
        "TRUE"
    } else {
        "1"
    };
    tx.execute(Statement::from_sql_and_values(
        backend,
        format!("UPDATE pin_provider_routes SET retired={retired} WHERE provider=$1"),
        [domain.clone().into()],
    ))
    .await
    .unwrap();
    let inventory = format!(
        "S7I1\n{}",
        serde_json::to_string(&(&owner, &manifest_hash)).unwrap()
    );
    tx.execute(Statement::from_sql_and_values(backend, "INSERT INTO pin_management_retirements(allocation_domain,database_scope,plan_id,item_id,principal,native_identity,binding,inventory,disposition,unresolved,state,created_at,record_kind) VALUES($1,$2,$3,'retire','local_configuration',$4,$5,$6,'retain','[]','retired',$7,'action')", [domain.clone().into(),scope.clone().into(),plan.into(),identity.clone().into(),binding.into(),inventory.into(),database_time(now)])).await.unwrap();
    let result = legacy::RecordedAction {
        receipt: legacy::Receipt {
            binding: entry,
            intent_ids: vec![],
            job_ids: vec![],
        },
        state: records::ActionState::Accepted,
        targets: vec![],
        root_releases: vec![],
        jobs: vec![],
        migration_points: vec![],
        decision: None,
    };
    let mut outputs = Vec::new();
    for (slot, name) in [schema::RETIREMENTS, "pin_provider_routes"]
        .iter()
        .enumerate()
    {
        let table = native::registry()
            .into_iter()
            .find(|t| &t.table == name)
            .unwrap();
        let projection = table
            .columns
            .iter()
            .map(|c| format!("CAST(\"{c}\" AS TEXT) AS \"{c}\""))
            .collect::<Vec<_>>()
            .join(",");
        let row = tx
            .query_one(Statement::from_sql_and_values(
                backend,
                format!(
                    "SELECT {projection} FROM \"{name}\" WHERE \"{}\"=$1",
                    table.primary_key[0]
                ),
                [domain.clone().into()],
            ))
            .await
            .unwrap()
            .unwrap();
        let cells = table
            .columns
            .iter()
            .map(|c| row.try_get::<Option<String>>("", c).unwrap())
            .collect::<Vec<_>>();
        outputs.push(serde_json::json!([(name, [&domain]), slot, cells]));
    }
    let facts = serde_json::json!([
        0,
        [
            [[schema::RETIREMENTS, [&domain]], [0]],
            [["pin_provider_routes", [&domain]], [1]]
        ],
        outputs,
        [],
        [],
        ["retire", [1, 0], []],
        []
    ]);
    #[derive(serde::Serialize)]
    struct WithoutState<'a> {
        receipt: &'a legacy::Receipt,
        targets: &'a [String],
        jobs: &'a [records::JobAuthority],
        migration_points: &'a [records::MigrationPoint],
        decision: &'a Option<records::OperatorDecision>,
    }
    let fields = WithoutState {
        receipt: &result.receipt,
        targets: &result.targets,
        jobs: &result.jobs,
        migration_points: &result.migration_points,
        decision: &result.decision,
    };
    let raw = serde_json::to_string(&(
        &scope,
        &identity,
        "local_configuration",
        plan,
        "retire",
        fields,
        &facts,
    ))
    .unwrap();
    let encoded = format!(
        "S7R1\n{}",
        serde_json::to_string(&(&result, &facts, digest(b"stage7/facts/1\0", &raw))).unwrap()
    );
    tx.execute(Statement::from_sql_and_values(backend, "UPDATE pin_management_records SET state='applied',result=$1,completed_at=$2 WHERE database_scope=$3 AND plan_id=$4 AND item_id='retire' AND kind='action'", [encoded.into(),database_time(now),scope.clone().into(),plan.into()])).await.unwrap();
    tx.commit().await.unwrap();
    initialization::upgrade_existing(
        db,
        initialization::UpgradeRequest {
            database_scope: &scope,
            native_identity: &identity,
            source_version: "m20261013_000001_zip_root_logical_release",
            target_version: schema::VERSION,
        },
    )
    .await
    .unwrap();
    request
}
