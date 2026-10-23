//! One database-enforced key space for both archive representations and staging.
use super::*;

pub(super) const TABLE: &str = "pin_management_archive_identity";
pub(super) const PG_BODY: &str = " BEGIN INSERT INTO pin_management_archive_identity(archive_id) VALUES(NEW.archive_id); RETURN NEW; END ";

pub(super) fn sqlite_guards() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "s75_archive_id_segmented",
            "CREATE TRIGGER s75_archive_id_segmented BEFORE INSERT ON pin_management_archive BEGIN SELECT CASE WHEN EXISTS(SELECT 1 FROM pin_management_inline_archive WHERE archive_id=NEW.archive_id) THEN RAISE(ABORT,'archive identity conflict') END; INSERT INTO pin_management_archive_identity(archive_id) VALUES(NEW.archive_id); END",
        ),
        (
            "s75_archive_id_inline",
            "CREATE TRIGGER s75_archive_id_inline BEFORE INSERT ON pin_management_inline_archive BEGIN SELECT CASE WHEN EXISTS(SELECT 1 FROM pin_management_archive WHERE archive_id=NEW.archive_id) THEN RAISE(ABORT,'archive identity conflict') END; INSERT INTO pin_management_archive_identity(archive_id) VALUES(NEW.archive_id); END",
        ),
        (
            "s75_archive_id_no_update",
            "CREATE TRIGGER s75_archive_id_no_update BEFORE UPDATE ON pin_management_archive_identity BEGIN SELECT RAISE(ABORT,'immutable archive identity'); END",
        ),
        (
            "s75_archive_id_no_delete",
            "CREATE TRIGGER s75_archive_id_no_delete BEFORE DELETE ON pin_management_archive_identity BEGIN SELECT RAISE(ABORT,'immutable archive identity'); END",
        ),
    ]
}

pub(super) async fn install<C: ConnectionTrait>(db: &C) -> std::result::Result<(), sea_orm::DbErr> {
    if db.get_database_backend() == DatabaseBackend::Postgres {
        db.execute_unprepared("LOCK TABLE pin_management_archive IN ACCESS EXCLUSIVE MODE NOWAIT")
            .await?;
    }
    // Index construction copies only identities inside the database, not bodies
    // or a materialized application-side inventory. Old root bytes stay intact.
    db.execute_unprepared("INSERT INTO pin_management_archive_identity(archive_id) SELECT archive_id FROM pin_management_archive").await?;
    if db.get_database_backend() == DatabaseBackend::Sqlite {
        for (_, ddl) in sqlite_guards() {
            db.execute_unprepared(ddl).await?;
        }
    } else {
        db.execute_unprepared(&format!("CREATE FUNCTION s75_archive_id_claim() RETURNS trigger LANGUAGE plpgsql AS $${PG_BODY}$$")).await?;
        for (name, table) in [
            ("s75_archive_id_segmented", "pin_management_archive"),
            ("s75_archive_id_inline", "pin_management_inline_archive"),
        ] {
            db.execute_unprepared(&format!("CREATE TRIGGER {name} BEFORE INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION s75_archive_id_claim()")).await?;
        }
        db.execute_unprepared("CREATE TRIGGER s75_archive_id_immutable BEFORE UPDATE OR DELETE ON pin_management_archive_identity FOR EACH ROW EXECUTE FUNCTION s75_immutable()").await?;
        db.execute_unprepared("CREATE TRIGGER s75_archive_id_truncate BEFORE TRUNCATE ON pin_management_archive_identity FOR EACH STATEMENT EXECUTE FUNCTION s75_no_truncate()").await?;
    }
    Ok(())
}
