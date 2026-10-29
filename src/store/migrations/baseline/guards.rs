use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr};

pub(super) async fn install<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    let columns = "action_id,action_kind,bucket,object_key,config_revision,rule_id,target_version_row_id,target_public_version_id,target_object_id,target_sequence,source_tier,destination_tier,source_cid,destination_cid,source_residency_revision,expected_source_node_identity,expected_destination_node_identity,ownership_generation";
    let settled = "(((OLD.settlement_kind = 'cancelled' AND EXISTS (SELECT 1 FROM lifecycle_actions WHERE id = OLD.action_id AND state = 'cancelled')) OR (OLD.settlement_kind = 'cleanup_complete' AND EXISTS (SELECT 1 FROM lifecycle_actions WHERE id = OLD.action_id AND state = 'succeeded'))) AND NOT EXISTS (SELECT 1 FROM residency_references WHERE owner_kind = 'transition' AND owner_id IN (OLD.id, OLD.action_id)))";
    if db.get_database_backend() == DatabaseBackend::Sqlite {
        db.execute_unprepared(&format!("CREATE TRIGGER ck_lifecycle_transitions_immutable_snapshot BEFORE UPDATE OF {columns} ON lifecycle_transitions BEGIN SELECT RAISE(ABORT, 'lifecycle transition snapshot is immutable'); END")).await?;
        db.execute_unprepared(&format!("CREATE TRIGGER ck_lifecycle_transitions_guarded_delete BEFORE DELETE ON lifecycle_transitions WHEN {settled} IS NOT TRUE BEGIN SELECT RAISE(ABORT, 'lifecycle transition is not safely settled'); END")).await?;
        db.execute_unprepared("CREATE TRIGGER zip_v2_import_legacy_conflict BEFORE INSERT ON zip_v2_import_requests WHEN EXISTS (SELECT 1 FROM import_jobs WHERE bucket=NEW.bucket AND key=NEW.source_key AND client_token=NEW.client_token) BEGIN SELECT RAISE(ABORT, 'zip_v2_import_token_conflict'); END").await?;
        for (name, event) in [
            ("zip_v2_legacy_import_conflict", "INSERT"),
            (
                "zip_v2_legacy_import_update_conflict",
                "UPDATE OF bucket,key,client_token",
            ),
        ] {
            db.execute_unprepared(&format!("CREATE TRIGGER {name} BEFORE {event} ON import_jobs WHEN NEW.client_token IS NOT NULL AND EXISTS (SELECT 1 FROM zip_v2_import_requests WHERE bucket=NEW.bucket AND source_key=NEW.key AND client_token=NEW.client_token) BEGIN SELECT RAISE(ABORT, 'zip_v2_import_token_conflict'); END")).await?;
        }
        for table in ["pin_lease_requirements", "pin_replica_observations"] {
            for event in ["UPDATE", "DELETE"] {
                db.execute_unprepared(&format!("CREATE TRIGGER {table}_no_{} BEFORE {event} ON {table} BEGIN SELECT RAISE(ABORT, 'immutable pin history'); END", event.to_lowercase())).await?;
            }
        }
    } else {
        let old = columns
            .split(',')
            .map(|c| format!("OLD.{c}"))
            .collect::<Vec<_>>()
            .join(",");
        let new = columns
            .split(',')
            .map(|c| format!("NEW.{c}"))
            .collect::<Vec<_>>()
            .join(",");
        db.execute_unprepared(&format!("CREATE FUNCTION ipfs_s3_reject_lifecycle_transition_snapshot_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF ROW({old}) IS DISTINCT FROM ROW({new}) THEN RAISE EXCEPTION 'lifecycle transition snapshot is immutable' USING ERRCODE='23514'; END IF; RETURN NEW; END; $$")).await?;
        db.execute_unprepared("CREATE TRIGGER ck_lifecycle_transitions_immutable_snapshot BEFORE UPDATE ON lifecycle_transitions FOR EACH ROW EXECUTE FUNCTION ipfs_s3_reject_lifecycle_transition_snapshot_update()").await?;
        db.execute_unprepared(&format!("CREATE FUNCTION ipfs_s3_guard_lifecycle_transition_delete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF {settled} IS DISTINCT FROM TRUE THEN RAISE EXCEPTION 'lifecycle transition is not safely settled' USING ERRCODE='23514'; END IF; RETURN OLD; END; $$")).await?;
        db.execute_unprepared("CREATE TRIGGER ck_lifecycle_transitions_guarded_delete BEFORE DELETE ON lifecycle_transitions FOR EACH ROW EXECUTE FUNCTION ipfs_s3_guard_lifecycle_transition_delete()").await?;
        db.execute_unprepared("CREATE FUNCTION zip_v2_import_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF TG_TABLE_NAME='import_jobs' THEN IF NEW.client_token IS NOT NULL AND EXISTS (SELECT 1 FROM zip_v2_import_requests WHERE bucket=NEW.bucket AND source_key=NEW.key AND client_token=NEW.client_token) THEN RAISE EXCEPTION 'zip_v2_import_token_conflict'; END IF; ELSE IF EXISTS (SELECT 1 FROM import_jobs WHERE bucket=NEW.bucket AND key=NEW.source_key AND client_token=NEW.client_token) THEN RAISE EXCEPTION 'zip_v2_import_token_conflict'; END IF; END IF; RETURN NEW; END $$").await?;
        db.execute_unprepared("CREATE TRIGGER zip_v2_import_legacy_conflict BEFORE INSERT ON zip_v2_import_requests FOR EACH ROW EXECUTE FUNCTION zip_v2_import_guard()").await?;
        db.execute_unprepared("CREATE TRIGGER zip_v2_legacy_import_conflict BEFORE INSERT OR UPDATE OF bucket,key,client_token ON import_jobs FOR EACH ROW EXECUTE FUNCTION zip_v2_import_guard()").await?;
        db.execute_unprepared("CREATE FUNCTION reject_pin_history_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'immutable pin history'; END $$").await?;
        for table in ["pin_lease_requirements", "pin_replica_observations"] {
            db.execute_unprepared(&format!("CREATE TRIGGER {table}_immutable BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION reject_pin_history_mutation()")).await?;
        }
    }
    Ok(())
}
