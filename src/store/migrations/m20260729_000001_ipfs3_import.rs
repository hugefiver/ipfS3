use sea_orm_migration::prelude::*;

const IMPORT_JOB_RESULT_OUTCOME_CHECK: &str = "((cid IS NOT NULL AND size IS NOT NULL AND error_code IS NULL AND error_message IS NULL) OR (cid IS NULL AND size IS NULL AND error_code IS NOT NULL AND error_message IS NOT NULL))";

#[derive(DeriveMigrationName)]
pub struct Migration;

fn required_text(name: &str) -> ColumnDef {
    let mut column = ColumnDef::new(Alias::new(name));
    column.text().not_null();
    column
}

fn nullable_text(name: &str) -> ColumnDef {
    let mut column = ColumnDef::new(Alias::new(name));
    column.text();
    column
}

fn required_timestamp(name: &str) -> ColumnDef {
    let mut column = ColumnDef::new(Alias::new(name));
    column.timestamp_with_time_zone().not_null();
    column
}

fn nullable_timestamp(name: &str) -> ColumnDef {
    let mut column = ColumnDef::new(Alias::new(name));
    column.timestamp_with_time_zone();
    column
}

fn required_big_integer(name: &str) -> ColumnDef {
    let mut column = ColumnDef::new(Alias::new(name));
    column.big_integer().not_null();
    column
}

fn nullable_big_integer(name: &str) -> ColumnDef {
    let mut column = ColumnDef::new(Alias::new(name));
    column.big_integer();
    column
}

fn required_integer(name: &str) -> ColumnDef {
    let mut column = ColumnDef::new(Alias::new(name));
    column.integer().not_null();
    column
}

fn create_import_jobs_table() -> TableCreateStatement {
    let mut id = required_text("id");
    id.primary_key();
    let mut table = Table::create();
    table
        .table(Alias::new("import_jobs"))
        .if_not_exists()
        .col(id)
        .col(required_text("bucket"))
        .col(required_text("key"))
        .col(required_text("source_type"))
        .col(required_text("source_value"))
        .col(required_text("request_fingerprint"))
        .col(nullable_text("client_token"))
        .col(nullable_text("object_content_type"))
        .col(required_text("metadata_json"))
        .col(required_text("tags_json"))
        .col(nullable_text("decompress_prefix"))
        .col(required_text("state"))
        .col(required_text("phase"))
        .col(required_integer("attempts"))
        .col(required_timestamp("next_attempt_at"))
        .col(nullable_text("locked_by"))
        .col(nullable_timestamp("locked_until"))
        .col({
            let mut column = required_big_integer("claim_epoch");
            column.default(0);
            column
        })
        .col(required_big_integer("providers_observed"))
        .col(required_big_integer("pin_nodes_processed"))
        .col(required_big_integer("pin_bytes_processed"))
        .col(required_big_integer("downloaded_bytes"))
        .col(nullable_big_integer("download_total"))
        .col(required_big_integer("ipfs_add_bytes"))
        .col(nullable_big_integer("logical_size"))
        .col(required_big_integer("entries_processed"))
        .col(required_big_integer("entries_succeeded"))
        .col(required_big_integer("entries_failed"))
        .col(required_big_integer("decompressed_bytes"))
        .col(nullable_text("final_cid"))
        .col(nullable_text("failure_code"))
        .col(nullable_text("failure_message"))
        .col(required_timestamp("created_at"))
        .col(required_timestamp("updated_at"))
        .col(nullable_timestamp("completed_at"))
        .check(Expr::col(Alias::new("source_type")).is_in(["cid", "url"]))
        .check(Expr::col(Alias::new("state")).is_in([
            "queued",
            "running",
            "completed",
            "failed",
            "superseded",
        ]))
        .check(Expr::col(Alias::new("phase")).is_in([
            "queued",
            "discovering_providers",
            "pinning_local",
            "downloading",
            "adding_to_ipfs",
            "inspecting",
            "decompressing",
            "publishing",
        ]))
        .check(Expr::col(Alias::new("attempts")).gte(0))
        .check(Expr::col(Alias::new("claim_epoch")).gte(0))
        .check(Expr::col(Alias::new("providers_observed")).gte(0))
        .check(Expr::col(Alias::new("pin_nodes_processed")).gte(0))
        .check(Expr::col(Alias::new("pin_bytes_processed")).gte(0))
        .check(Expr::col(Alias::new("downloaded_bytes")).gte(0))
        .check(Expr::col(Alias::new("download_total")).gte(0))
        .check(Expr::col(Alias::new("ipfs_add_bytes")).gte(0))
        .check(Expr::col(Alias::new("logical_size")).gte(0))
        .check(Expr::col(Alias::new("entries_processed")).gte(0))
        .check(Expr::col(Alias::new("entries_succeeded")).gte(0))
        .check(Expr::col(Alias::new("entries_failed")).gte(0))
        .check(Expr::col(Alias::new("decompressed_bytes")).gte(0));
    table.to_owned()
}

fn create_import_destinations_table() -> TableCreateStatement {
    let mut bucket_fk = ForeignKey::create();
    bucket_fk
        .name("fk_import_destinations_bucket")
        .from(Alias::new("import_destinations"), Alias::new("bucket"))
        .to(Alias::new("buckets"), Alias::new("name"))
        .on_delete(ForeignKeyAction::Cascade);
    let mut owner_job_fk = ForeignKey::create();
    owner_job_fk
        .name("fk_import_destinations_owner_job_id")
        .from(
            Alias::new("import_destinations"),
            Alias::new("owner_job_id"),
        )
        .to(Alias::new("import_jobs"), Alias::new("id"))
        .on_delete(ForeignKeyAction::SetNull);

    let mut table = Table::create();
    table
        .table(Alias::new("import_destinations"))
        .if_not_exists()
        .col(required_text("bucket"))
        .col(required_text("key"))
        .col(required_big_integer("generation"))
        .col(nullable_text("owner_job_id"))
        .col(required_timestamp("updated_at"))
        .primary_key(
            Index::create()
                .name("pk_import_destinations")
                .col(Alias::new("bucket"))
                .col(Alias::new("key")),
        )
        .foreign_key(&mut bucket_fk)
        .foreign_key(&mut owner_job_fk)
        .check(Expr::col(Alias::new("generation")).gte(1));
    table.to_owned()
}

fn create_import_prefix_claims_table() -> TableCreateStatement {
    let mut job_fk = ForeignKey::create();
    job_fk
        .name("fk_import_prefix_claims_job_id")
        .from(Alias::new("import_prefix_claims"), Alias::new("job_id"))
        .to(Alias::new("import_jobs"), Alias::new("id"))
        .on_delete(ForeignKeyAction::Cascade);
    let mut bucket_fk = ForeignKey::create();
    bucket_fk
        .name("fk_import_prefix_claims_bucket")
        .from(Alias::new("import_prefix_claims"), Alias::new("bucket"))
        .to(Alias::new("buckets"), Alias::new("name"))
        .on_delete(ForeignKeyAction::Cascade);

    let mut table = Table::create();
    table
        .table(Alias::new("import_prefix_claims"))
        .if_not_exists()
        .col(required_text("job_id"))
        .col(required_text("bucket"))
        .col(required_text("prefix"))
        .col(required_big_integer("claim_order"))
        .primary_key(
            Index::create()
                .name("pk_import_prefix_claims")
                .col(Alias::new("job_id"))
                .col(Alias::new("bucket"))
                .col(Alias::new("prefix")),
        )
        .foreign_key(&mut job_fk)
        .foreign_key(&mut bucket_fk)
        .check(Expr::col(Alias::new("claim_order")).gte(0));
    table.to_owned()
}

fn create_import_job_targets_table() -> TableCreateStatement {
    let mut job_fk = ForeignKey::create();
    job_fk
        .name("fk_import_job_targets_job_id")
        .from(Alias::new("import_job_targets"), Alias::new("job_id"))
        .to(Alias::new("import_jobs"), Alias::new("id"))
        .on_delete(ForeignKeyAction::Cascade);
    let mut bucket_fk = ForeignKey::create();
    bucket_fk
        .name("fk_import_job_targets_bucket")
        .from(Alias::new("import_job_targets"), Alias::new("bucket"))
        .to(Alias::new("buckets"), Alias::new("name"))
        .on_delete(ForeignKeyAction::Cascade);

    let mut table = Table::create();
    table
        .table(Alias::new("import_job_targets"))
        .if_not_exists()
        .col(required_text("job_id"))
        .col(required_text("bucket"))
        .col(required_text("key"))
        .col(required_big_integer("expected_generation"))
        .col(required_text("kind"))
        .primary_key(
            Index::create()
                .name("pk_import_job_targets")
                .col(Alias::new("job_id"))
                .col(Alias::new("bucket"))
                .col(Alias::new("key")),
        )
        .foreign_key(&mut job_fk)
        .foreign_key(&mut bucket_fk)
        .check(Expr::col(Alias::new("expected_generation")).gte(1))
        .check(Expr::col(Alias::new("kind")).is_in(["archive", "extracted"]));
    table.to_owned()
}

fn create_import_job_results_table() -> TableCreateStatement {
    let mut job_fk = ForeignKey::create();
    job_fk
        .name("fk_import_job_results_job_id")
        .from(Alias::new("import_job_results"), Alias::new("job_id"))
        .to(Alias::new("import_jobs"), Alias::new("id"))
        .on_delete(ForeignKeyAction::Cascade);

    let mut table = Table::create();
    table
        .table(Alias::new("import_job_results"))
        .if_not_exists()
        .col(required_text("job_id"))
        .col(required_big_integer("sequence"))
        .col(required_text("key"))
        .col(nullable_text("cid"))
        .col(nullable_big_integer("size"))
        .col(nullable_text("error_code"))
        .col(nullable_text("error_message"))
        .primary_key(
            Index::create()
                .name("pk_import_job_results")
                .col(Alias::new("job_id"))
                .col(Alias::new("sequence")),
        )
        .foreign_key(&mut job_fk)
        .check(Expr::col(Alias::new("sequence")).gte(0))
        .check(Expr::col(Alias::new("size")).gte(0))
        .check(Expr::cust(IMPORT_JOB_RESULT_OUTCOME_CHECK));
    table.to_owned()
}

fn create_tables() -> [TableCreateStatement; 5] {
    [
        create_import_jobs_table(),
        create_import_destinations_table(),
        create_import_prefix_claims_table(),
        create_import_job_targets_table(),
        create_import_job_results_table(),
    ]
}

fn create_indexes() -> [IndexCreateStatement; 7] {
    [
        Index::create()
            .if_not_exists()
            .name("uq_import_jobs_bucket_key_client_token")
            .table(Alias::new("import_jobs"))
            .col(Alias::new("bucket"))
            .col(Alias::new("key"))
            .col(Alias::new("client_token"))
            .unique()
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_import_jobs_state_next_attempt_locked")
            .table(Alias::new("import_jobs"))
            .col(Alias::new("state"))
            .col(Alias::new("next_attempt_at"))
            .col(Alias::new("locked_until"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_import_jobs_state_completed_at")
            .table(Alias::new("import_jobs"))
            .col(Alias::new("state"))
            .col(Alias::new("completed_at"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_import_destinations_owner_job")
            .table(Alias::new("import_destinations"))
            .col(Alias::new("owner_job_id"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_import_prefix_claims_bucket_prefix")
            .table(Alias::new("import_prefix_claims"))
            .col(Alias::new("bucket"))
            .col(Alias::new("prefix"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_import_job_targets_bucket_key")
            .table(Alias::new("import_job_targets"))
            .col(Alias::new("bucket"))
            .col(Alias::new("key"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_import_job_results_job_sequence")
            .table(Alias::new("import_job_results"))
            .col(Alias::new("job_id"))
            .col(Alias::new("sequence"))
            .to_owned(),
    ]
}

fn drop_indexes() -> [IndexDropStatement; 7] {
    [
        Index::drop()
            .if_exists()
            .name("idx_import_job_results_job_sequence")
            .to_owned(),
        Index::drop()
            .if_exists()
            .name("idx_import_job_targets_bucket_key")
            .to_owned(),
        Index::drop()
            .if_exists()
            .name("idx_import_prefix_claims_bucket_prefix")
            .to_owned(),
        Index::drop()
            .if_exists()
            .name("idx_import_destinations_owner_job")
            .to_owned(),
        Index::drop()
            .if_exists()
            .name("idx_import_jobs_state_completed_at")
            .to_owned(),
        Index::drop()
            .if_exists()
            .name("idx_import_jobs_state_next_attempt_locked")
            .to_owned(),
        Index::drop()
            .if_exists()
            .name("uq_import_jobs_bucket_key_client_token")
            .to_owned(),
    ]
}

fn drop_tables() -> [TableDropStatement; 5] {
    [
        Table::drop()
            .table(Alias::new("import_job_results"))
            .if_exists()
            .to_owned(),
        Table::drop()
            .table(Alias::new("import_job_targets"))
            .if_exists()
            .to_owned(),
        Table::drop()
            .table(Alias::new("import_prefix_claims"))
            .if_exists()
            .to_owned(),
        Table::drop()
            .table(Alias::new("import_destinations"))
            .if_exists()
            .to_owned(),
        Table::drop()
            .table(Alias::new("import_jobs"))
            .if_exists()
            .to_owned(),
    ]
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for table in create_tables() {
            manager.create_table(table).await?;
        }
        for index in create_indexes() {
            manager.create_index(index).await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for index in drop_indexes() {
            manager.drop_index(index).await?;
        }
        for table in drop_tables() {
            manager.drop_table(table).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_rendering_has_import_constraints_and_indexes() {
        let creates: Vec<String> = create_tables()
            .iter()
            .map(|statement| statement.to_string(PostgresQueryBuilder))
            .collect();
        for table in [
            "import_jobs",
            "import_destinations",
            "import_prefix_claims",
            "import_job_targets",
            "import_job_results",
        ] {
            assert!(
                creates.iter().any(|statement| statement
                    .starts_with(&format!("CREATE TABLE IF NOT EXISTS \"{table}\""))),
                "missing PostgreSQL create statement for {table}: {creates:#?}"
            );
        }

        let jobs = creates
            .iter()
            .find(|statement| statement.contains("\"import_jobs\""))
            .unwrap();
        for required in [
            "\"claim_epoch\" bigint NOT NULL DEFAULT 0",
            "CHECK (\"source_type\" IN ('cid', 'url'))",
            "CHECK (\"state\" IN ('queued', 'running', 'completed', 'failed', 'superseded'))",
            "CHECK (\"phase\" IN ('queued', 'discovering_providers', 'pinning_local', 'downloading', 'adding_to_ipfs', 'inspecting', 'decompressing', 'publishing'))",
            "CHECK (\"attempts\" >= 0)",
            "CHECK (\"claim_epoch\" >= 0)",
        ] {
            assert!(jobs.contains(required), "missing {required} in {jobs}");
        }
        assert!(
            !jobs.contains("FOREIGN KEY (\"bucket\") REFERENCES \"buckets\" (\"name\")"),
            "import jobs must survive bucket deletion for terminal retention: {jobs}"
        );

        let destinations = creates
            .iter()
            .find(|statement| statement.contains("\"import_destinations\""))
            .unwrap();
        assert!(destinations.contains("PRIMARY KEY (\"bucket\", \"key\")"));
        assert!(destinations.contains("CHECK (\"generation\" >= 1)"));
        assert!(destinations.contains(
            "FOREIGN KEY (\"bucket\") REFERENCES \"buckets\" (\"name\") ON DELETE CASCADE"
        ));
        assert!(destinations.contains(
            "FOREIGN KEY (\"owner_job_id\") REFERENCES \"import_jobs\" (\"id\") ON DELETE SET NULL"
        ));

        let prefix_claims = creates
            .iter()
            .find(|statement| statement.contains("\"import_prefix_claims\""))
            .unwrap();
        assert!(prefix_claims.contains(
            "FOREIGN KEY (\"bucket\") REFERENCES \"buckets\" (\"name\") ON DELETE CASCADE"
        ));

        let targets = creates
            .iter()
            .find(|statement| statement.contains("\"import_job_targets\""))
            .unwrap();
        assert!(targets.contains("PRIMARY KEY (\"job_id\", \"bucket\", \"key\")"));
        assert!(targets.contains("CHECK (\"expected_generation\" >= 1)"));
        assert!(targets.contains("CHECK (\"kind\" IN ('archive', 'extracted'))"));
        assert!(targets.contains(
            "FOREIGN KEY (\"job_id\") REFERENCES \"import_jobs\" (\"id\") ON DELETE CASCADE"
        ));
        assert!(targets.contains(
            "FOREIGN KEY (\"bucket\") REFERENCES \"buckets\" (\"name\") ON DELETE CASCADE"
        ));

        let results = creates
            .iter()
            .find(|statement| statement.contains("\"import_job_results\""))
            .unwrap();
        assert!(results.contains("PRIMARY KEY (\"job_id\", \"sequence\")"));
        assert!(results.contains("CHECK (\"sequence\" >= 0)"));
        assert!(results.contains("CHECK (\"size\" >= 0)"));
        assert!(results.contains(IMPORT_JOB_RESULT_OUTCOME_CHECK));

        let indexes: Vec<String> = create_indexes()
            .iter()
            .map(|statement| statement.to_string(PostgresQueryBuilder))
            .collect();
        for required in [
            "CREATE UNIQUE INDEX IF NOT EXISTS \"uq_import_jobs_bucket_key_client_token\" ON \"import_jobs\" (\"bucket\", \"key\", \"client_token\")",
            "CREATE INDEX IF NOT EXISTS \"idx_import_jobs_state_next_attempt_locked\" ON \"import_jobs\" (\"state\", \"next_attempt_at\", \"locked_until\")",
            "CREATE INDEX IF NOT EXISTS \"idx_import_jobs_state_completed_at\" ON \"import_jobs\" (\"state\", \"completed_at\")",
            "CREATE INDEX IF NOT EXISTS \"idx_import_destinations_owner_job\" ON \"import_destinations\" (\"owner_job_id\")",
            "CREATE INDEX IF NOT EXISTS \"idx_import_prefix_claims_bucket_prefix\" ON \"import_prefix_claims\" (\"bucket\", \"prefix\")",
            "CREATE INDEX IF NOT EXISTS \"idx_import_job_targets_bucket_key\" ON \"import_job_targets\" (\"bucket\", \"key\")",
            "CREATE INDEX IF NOT EXISTS \"idx_import_job_results_job_sequence\" ON \"import_job_results\" (\"job_id\", \"sequence\")",
        ] {
            assert!(
                indexes.iter().any(|statement| statement == required),
                "missing PostgreSQL index SQL: {required}; actual: {indexes:#?}"
            );
        }
    }
}
