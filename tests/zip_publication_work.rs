//! Database work stays batch-sized through the authenticated ZIP publication path.
#[allow(dead_code)]
mod support {
    pub mod cors;
    pub mod decompress;
    pub mod sigv4;
}
#[allow(dead_code)]
#[path = "support/stage9_zip_http.rs"]
mod fixture;
#[allow(dead_code)]
#[path = "support/stage9_zip_http_zip.rs"]
mod zip_fixture;

use ipfs_s3_gateway::store::entities::{object, pin_lease_target, pin_resource_history};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Set};
use std::sync::{Arc, Mutex};
use support::decompress::S3TestEndpoint;
use tracing_subscriber::{layer::SubscriberExt, registry::LookupSpan};

#[derive(Default, Clone, Debug)]
struct Queries {
    statements: usize,
    mutations: usize,
    affected_rows: u64,
}

#[derive(Default)]
struct QueryFields {
    statement: String,
    summary: String,
    affected_rows: u64,
}

impl tracing::field::Visit for QueryFields {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "db.statement" => self.statement = value.to_owned(),
            "summary" => self.summary = value.to_owned(),
            _ => {}
        }
    }
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == "rows_affected" {
            self.affected_rows = value;
        }
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.record_str(field, &format!("{value:?}"));
    }
}

#[derive(Clone)]
struct PublicationQueries(Arc<Mutex<Queries>>);

impl<S> tracing_subscriber::Layer<S> for PublicationQueries
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() != "sqlx::query"
            || !context.event_scope(event).is_some_and(|mut scope| {
                scope.any(|span| span.metadata().name() == "zip_publication")
            })
        {
            return;
        }
        let mut fields = QueryFields::default();
        event.record(&mut fields);
        let sql = if fields.statement.is_empty() {
            &fields.summary
        } else {
            &fields.statement
        };
        let sql = sql.trim().to_ascii_uppercase();
        let mutation = ["INSERT", "UPDATE", "DELETE"].iter().any(|verb| {
            sql.starts_with(verb)
                || (sql.starts_with("WITH ") && sql.contains(&format!(" {verb} ")))
        });
        let mut counts = self.0.lock().unwrap();
        counts.statements += 1;
        counts.mutations += usize::from(mutation);
        if mutation {
            counts.affected_rows += fields.affected_rows;
        }
    }
}

async fn signed_small_zip_publication_ignores_unrelated_released_history() {
    for entry in fixture::ENTRY_POINTS {
        let archive = zip_fixture::distinct_outputs_zip(2);
        let replies = zip_fixture::distinct_output_cids(2)
            .into_iter()
            .map(support::decompress::AddReply::Ok)
            .collect();
        let fixture = fixture::Fixture::new(entry, true, &archive, replies).await;
        let db = fixture.state.store.db();
        for start in (0..20_000).step_by(100) {
            pin_resource_history::Entity::insert_many((start..start + 100).map(|n| {
                pin_resource_history::ActiveModel {
                    provider: Set(fixture.provider.clone()),
                    cid: Set(format!("released-history-{n}")),
                    epoch: Set(1),
                    ledger: Set("{}".into()),
                }
            }))
            .exec(db)
            .await
            .unwrap();
        }
        fixture.publish(entry, archive, true, false).await;
        for index in 0usize..2 {
            let key = format!("expanded/entry-{index:02}.txt");
            let row = object::Entity::find()
                .filter(object::Column::Bucket.eq(fixture::BUCKET))
                .filter(object::Column::Key.eq(&key))
                .one(db)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.cid, zip_fixture::distinct_output_cid(index));
            let target = pin_lease_target::Entity::find()
                .filter(pin_lease_target::Column::Provider.eq(&fixture.provider))
                .filter(pin_lease_target::Column::Cid.eq(&row.cid))
                .one(db)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(target.state, "waiting");
            let response = support::sigv4::send_sigv4(
                reqwest::Method::GET,
                fixture.endpoint(),
                fixture::BUCKET,
                &key,
                &[],
                Vec::new(),
                http::HeaderMap::new(),
                "test",
            )
            .await;
            assert_eq!(response.status(), http::StatusCode::OK);
            assert_eq!(
                response.bytes().await.unwrap().as_ref(),
                (index as u64).to_le_bytes()
            );
        }
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn signed_zip_publication_batches_database_work() {
    let counts = Arc::new(Mutex::new(Queries::default()));
    let subscriber = tracing_subscriber::registry().with(PublicationQueries(counts.clone()));
    tracing::subscriber::set_global_default(subscriber).unwrap();
    for outputs in [256, 1649] {
        let archive = zip_fixture::distinct_outputs_zip(outputs);
        let replies = zip_fixture::distinct_output_cids(outputs)
            .into_iter()
            .map(support::decompress::AddReply::Ok)
            .collect();
        let fixture =
            fixture::Fixture::new(fixture::EntryPoint::CompleteMpu, true, &archive, replies).await;
        *counts.lock().unwrap() = Queries::default();
        let started = std::time::Instant::now();
        fixture
            .publish(fixture::EntryPoint::CompleteMpu, archive, true, false)
            .await;
        let measured = counts.lock().unwrap().clone();
        fixture.shutdown().await;
        eprintln!(
            "outputs={outputs} publication_queries={measured:?} elapsed={:?}",
            started.elapsed()
        );
        assert!(
            measured.statements > 0,
            "SQL tracing did not observe publication"
        );
        assert!(measured.mutations > 0, "SQL tracing did not observe writes");
        assert!(
            measured.mutations < outputs,
            "publication still writes intermediate state per output: {measured:?}"
        );
        assert!(
            measured.statements < outputs * 4,
            "publication still repeats database work per target: {measured:?}"
        );
    }
    signed_small_zip_publication_ignores_unrelated_released_history().await;
}
