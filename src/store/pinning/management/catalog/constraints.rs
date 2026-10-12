//! Constraints used to authorize exact owner/version/lease/resource selections.
use super::*;
use std::collections::BTreeMap;
use crate::store::migrations::m20261007_000001_legacy_pinning_migration::schema as r26;

type ForeignKey=(&'static str,&'static [&'static str],&'static [&'static str],&'static str);
fn foreign_keys(table:&str)->Option<Vec<ForeignKey>> {
    Some(match table {
        "objects"|"multipart_uploads"=>vec![("buckets",&["bucket"],&["name"],"CASCADE")],
        "multipart_parts"=>vec![("multipart_uploads",&["upload_id"],&["upload_id"],"CASCADE")],
        "pin_leases"=>vec![("objects",&["owner_object_id"],&["id"],"CASCADE")],
        "pin_lease_targets"=>vec![("pin_leases",&["lease_id"],&["id"],"CASCADE")],
        "object_versions"=>vec![("buckets",&["bucket"],&["name"],"CASCADE"),("objects",&["object_id"],&["id"],"CASCADE")],
        "pin_extension_decisions"=>vec![("objects",&["object_id"],&["id"],"CASCADE"),("object_versions",&["version_row_id","object_id"],&["id","object_id"],"CASCADE")],
        "zip_manifest_entries"|"zip_root_builds"=>vec![("zip_batches",&["batch_id"],&["id"],"RESTRICT")],
        "zip_root_references"=>vec![("zip_root_builds",&["batch_id","revision","epoch"],&["batch_id","revision","epoch"],"RESTRICT")],
        "zip_v2_manifest"=>vec![("zip_v2_executions",&["batch_id"],&["id"],"RESTRICT")],
        "zip_v2_targets"=>vec![("zip_v2_executions",&["batch_id"],&["id"],"RESTRICT"),("zip_v2_manifest",&["batch_id","object_key"],&["batch_id","object_key"],"RESTRICT")],
        "zip_v2_import_requests"=>vec![("zip_v2_executions",&["batch_id"],&["id"],"RESTRICT")],
        "zip_v2_mpu_intakes"=>vec![("multipart_uploads",&["active_upload_id"],&["upload_id"],"SET NULL"),("zip_v2_executions",&["execution_id"],&["id"],"RESTRICT")],
        "zip_v2_mpu_completions"=>vec![("zip_v2_mpu_intakes",&["upload_id"],&["original_upload_id"],"RESTRICT")],
        "pin_legacy_migrations"=>vec![("pin_legacy_migrations",&["reverses"],&["id"],"RESTRICT")],
        "pin_legacy_migration_items"=>vec![("pin_legacy_migrations",&["operation_id"],&["id"],"RESTRICT")],
        "pin_lease_requirements"|"pin_replica_observations"|"remote_pins"|"remote_pin_ledger"|"pin_resource_history"|"pin_invocation_routes"|"pin_provider_routes"|"pin_provider_usage"|"pin_jobs"|"pin_submit_history"|"pin_submit_observations"=>vec![],
        _=>return None,
    })
}
pub(super) async fn validate<C:ConnectionTrait>(db:&C)->Result<()> {
    for table in super::super::native::registry() {
        if let Some(expected)=foreign_keys(&table.table) {
            if db.get_database_backend()==DatabaseBackend::Sqlite {sqlite_foreign_keys(db,&table.table,&expected).await?;}else{pg_foreign_keys(db,&table.table,&expected).await?;}
        }
    }
    if db.get_database_backend()==DatabaseBackend::Sqlite {
        for (table,expected) in [(r26::OPERATIONS,r26::OPERATIONS_SQL),(r26::ITEMS,r26::ITEMS_SQL)] {
            let sql=sql(db,table).await?;
            if compact(&sql)!=compact(expected) {return Err(ManagementError::Schema);}
        }
        let row=db.query_one(Statement::from_string(DatabaseBackend::Sqlite,"SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='pin_legacy_item_bound'")).await.map_err(|_|ManagementError::Schema)?.ok_or(ManagementError::Schema)?;
        if compact(&row.try_get::<String>("","sql").map_err(|_|ManagementError::Schema)?)!=compact(r26::SQLITE_BOUND) {return Err(ManagementError::Schema);}
        for (table,required) in [
            ("pin_leases",vec!["CHECK(state IN ('active','expired','cancelled','evicted'))"]),
            ("pin_lease_targets",vec!["CHECK(state IN ('waiting','submitted','pinned','degraded','quota_waiting','quota_blocked','evicted','released'))"]),
            ("remote_pins",vec!["CHECK(status IN ('reserved','queued','pinning','pinned','failed','absent'))","CHECK(epoch >= 1)","CHECK(failure_attempts >= 0)"]),
            ("pin_jobs",vec!["CHECK(state IN ('pending','running','done'))","CHECK(((operation IN ('submit','poll') AND lease_id IS NOT NULL AND target_id IS NOT NULL AND expected_generation IS NOT NULL AND expected_remote_epoch IS NULL) OR (operation IN ('unpin','reconcile') AND lease_id IS NULL AND target_id IS NULL AND expected_generation IS NULL AND expected_remote_epoch IS NOT NULL)))","CHECK(((operation='submit' AND submit_phase IS NOT NULL AND submit_phase IN ('ready','calling','recovering','recovery_backoff')) OR (operation<>'submit' AND submit_phase IS NULL)))"]),
        ] {
            let actual=compact(&sql(db,table).await?);
            if required.iter().any(|required|!actual.contains(&compact(required))) {return Err(ManagementError::Schema);}
        }
        for (table,name,definition) in [
            ("pin_leases","uq_pin_leases_owner_object_source","CREATE UNIQUE INDEX uq_pin_leases_owner_object_source ON pin_leases(owner_object_id,source)"),
            ("pin_lease_targets","uq_pin_lease_targets_lease_cid_provider","CREATE UNIQUE INDEX uq_pin_lease_targets_lease_cid_provider ON pin_lease_targets(lease_id,cid,provider)"),
            ("object_versions","uq_object_versions_null_slot","CREATE UNIQUE INDEX uq_object_versions_null_slot ON object_versions(bucket,key) WHERE version_id IS NULL"),
            ("object_versions","uq_object_versions_public_id","CREATE UNIQUE INDEX uq_object_versions_public_id ON object_versions(bucket,key,version_id) WHERE version_id IS NOT NULL"),
        ] {
            let row=db.query_one(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"SELECT sql FROM sqlite_schema WHERE type='index' AND name=$1 AND tbl_name=$2",[name.into(),table.into()])).await.map_err(|_|ManagementError::Schema)?.ok_or(ManagementError::Schema)?;
            let actual=row.try_get::<String>("","sql").map_err(|_|ManagementError::Schema)?.replace("IF NOT EXISTS ","");
            if compact(&actual)!=compact(definition) {return Err(ManagementError::Schema);}
        }
    }else{
        pg_checks(db).await?;
        let row=db.query_one(Statement::from_string(DatabaseBackend::Postgres,"SELECT t.tgtype::integer AS flags,t.tgenabled::text AS enabled,t.tgqual IS NULL AS unconditional,t.tgattr::text AS columns,encode(t.tgargs,'hex') AS args,p.prosrc AS body,p.prosecdef,p.proconfig IS NULL AS default_config,pn.nspname=current_schema() AS local_function FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid JOIN pg_catalog.pg_namespace pn ON pn.oid=p.pronamespace WHERE n.nspname=current_schema() AND c.relname='pin_legacy_migration_items' AND t.tgname='pin_legacy_item_bound' AND NOT t.tgisinternal")).await.map_err(|_|ManagementError::Schema)?.ok_or(ManagementError::Schema)?;
        if row.try_get::<i32>("","flags").map_err(|_|ManagementError::Schema)?!=7 || row.try_get::<String>("","enabled").map_err(|_|ManagementError::Schema)?!="O" || compact(&row.try_get::<String>("","body").map_err(|_|ManagementError::Schema)?)!=compact(r26::PG_BOUND_BODY) || !row.try_get::<String>("","columns").map_err(|_|ManagementError::Schema)?.is_empty() || !row.try_get::<String>("","args").map_err(|_|ManagementError::Schema)?.is_empty() || row.try_get::<bool>("","prosecdef").map_err(|_|ManagementError::Schema)? {return Err(ManagementError::Schema);}
        for flag in ["unconditional","default_config","local_function"] {if !row.try_get::<bool>("",flag).map_err(|_|ManagementError::Schema)? {return Err(ManagementError::Schema);}}
    }
    Ok(())
}
async fn sql<C:ConnectionTrait>(db:&C,table:&str)->Result<String> {
    db.query_one(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"SELECT sql FROM sqlite_schema WHERE type='table' AND name=$1",[table.into()])).await.map_err(|_|ManagementError::Schema)?.ok_or(ManagementError::Schema)?.try_get("","sql").map_err(|_|ManagementError::Schema)
}
async fn sqlite_foreign_keys<C:ConnectionTrait>(db:&C,table:&str,expected:&[ForeignKey])->Result<()> {
    let rows=db.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"SELECT id,seq,\"table\" AS target,\"from\" AS source,\"to\" AS destination,on_update,on_delete,\"match\" AS matching FROM pragma_foreign_key_list($1) ORDER BY id,seq",[table.into()])).await.map_err(|_|ManagementError::Schema)?;
    let mut groups:BTreeMap<i32,(String,Vec<String>,Vec<String>,String)>=BTreeMap::new();
    for row in rows {
        let get=|column|row.try_get::<String>("",column).map_err(|_|ManagementError::Schema);
        if get("on_update")?!="NO ACTION" || get("matching")?!="NONE" {return Err(ManagementError::Schema);}
        let group=groups.entry(row.try_get("","id").map_err(|_|ManagementError::Schema)?).or_insert((get("target")?,vec![],vec![],get("on_delete")?));
        group.1.push(get("source")?);group.2.push(get("destination")?);
    }
    let mut actual=groups.into_values().collect::<Vec<_>>();actual.sort();
    let mut expected=expected.iter().map(|(table,from,to,delete)|(table.to_string(),from.iter().map(|v|v.to_string()).collect(),to.iter().map(|v|v.to_string()).collect(),delete.to_string())).collect::<Vec<_>>();expected.sort();
    if actual!=expected {return Err(ManagementError::Schema);}Ok(())
}
async fn pg_foreign_keys<C:ConnectionTrait>(db:&C,table:&str,expected:&[ForeignKey])->Result<()> {
    let rows=db.query_all(Statement::from_sql_and_values(DatabaseBackend::Postgres,"SELECT pg_get_constraintdef(k.oid,false) AS definition,k.convalidated,k.condeferrable,k.condeferred,rn.nspname=current_schema() AS local_reference FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_class rc ON rc.oid=k.confrelid JOIN pg_namespace rn ON rn.oid=rc.relnamespace WHERE n.nspname=current_schema() AND c.relname=$1 AND k.contype='f'",[table.into()])).await.map_err(|_|ManagementError::Schema)?;
    let mut actual=Vec::new();
    for row in rows {
        if !row.try_get::<bool>("","convalidated").map_err(|_|ManagementError::Schema)? || row.try_get::<bool>("","condeferrable").map_err(|_|ManagementError::Schema)? || row.try_get::<bool>("","condeferred").map_err(|_|ManagementError::Schema)? || !row.try_get::<bool>("","local_reference").map_err(|_|ManagementError::Schema)? {return Err(ManagementError::Schema);}
        actual.push(compact(&row.try_get::<String>("","definition").map_err(|_|ManagementError::Schema)?));
    }
    let mut expected=expected.iter().map(|(table,from,to,delete)|compact(&format!("FOREIGN KEY ({}) REFERENCES {}({}) ON DELETE {}",from.join(","),table,to.join(","),delete))).collect::<Vec<_>>();actual.sort();expected.sort();
    if actual!=expected {return Err(ManagementError::Schema);}Ok(())
}
async fn pg_checks<C:ConnectionTrait>(db:&C)->Result<()> {
    for (table,expected) in [
        ("pin_leases",vec!["CHECK ((state = ANY (ARRAY['active'::text, 'expired'::text, 'cancelled'::text, 'evicted'::text])))"]),
        ("pin_lease_targets",vec!["CHECK ((state = ANY (ARRAY['waiting'::text, 'submitted'::text, 'pinned'::text, 'degraded'::text, 'quota_waiting'::text, 'quota_blocked'::text, 'evicted'::text, 'released'::text])))"]),
        ("remote_pins",vec!["CHECK ((status = ANY (ARRAY['reserved'::text, 'queued'::text, 'pinning'::text, 'pinned'::text, 'failed'::text, 'absent'::text])))","CHECK ((epoch >= 1))","CHECK ((failure_attempts >= 0))"]),
        ("pin_jobs",vec!["CHECK ((state = ANY (ARRAY['pending'::text, 'running'::text, 'done'::text])))","CHECK ((((operation = ANY (ARRAY['submit'::text, 'poll'::text])) AND (lease_id IS NOT NULL) AND (target_id IS NOT NULL) AND (expected_generation IS NOT NULL) AND (expected_remote_epoch IS NULL)) OR ((operation = ANY (ARRAY['unpin'::text, 'reconcile'::text])) AND (lease_id IS NULL) AND (target_id IS NULL) AND (expected_generation IS NULL) AND (expected_remote_epoch IS NOT NULL))))","CHECK ((((operation = 'submit'::text) AND (submit_phase IS NOT NULL) AND (submit_phase = ANY (ARRAY['ready'::text, 'calling'::text, 'recovering'::text, 'recovery_backoff'::text]))) OR ((operation <> 'submit'::text) AND (submit_phase IS NULL))))"]),
        (r26::OPERATIONS,vec!["CHECK ((direction = ANY (ARRAY['apply'::text, 'rollback'::text])))","CHECK (((item_count > 0) AND (item_count <= 10100)))","CHECK ((((direction = 'apply'::text) AND (reverses IS NULL)) OR ((direction = 'rollback'::text) AND (reverses IS NOT NULL))))"]),
        (r26::ITEMS,vec!["CHECK ((ordinal >= 0))"]),
    ] {
        let rows=db.query_all(Statement::from_sql_and_values(DatabaseBackend::Postgres,"SELECT pg_get_constraintdef(k.oid,false) AS definition,k.convalidated,k.condeferrable FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname=$1 AND k.contype='c'",[table.into()])).await.map_err(|_|ManagementError::Schema)?;
        let mut actual=Vec::new();
        for row in rows {
            if !row.try_get::<bool>("","convalidated").map_err(|_|ManagementError::Schema)? || row.try_get::<bool>("","condeferrable").map_err(|_|ManagementError::Schema)? {return Err(ManagementError::Schema);}
            actual.push(compact(&row.try_get::<String>("","definition").map_err(|_|ManagementError::Schema)?));
        }
        let mut expected=expected.into_iter().map(compact).collect::<Vec<_>>();expected.sort();actual.sort();
        if actual!=expected {return Err(ManagementError::Schema);}
    }
    Ok(())
}
