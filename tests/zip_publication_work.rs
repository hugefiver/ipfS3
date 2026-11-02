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

use std::sync::{Arc, Mutex};
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
}
