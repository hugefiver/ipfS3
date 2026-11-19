#[path = "support/stage6_cluster_fixture.rs"]
mod fixture;
use fixture::Fixture;
use http::{Method, StatusCode};
use sea_orm::{ConnectionTrait, Statement};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::time::{sleep, timeout};
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{method, path, query_param},
};

const WAIT: Duration = Duration::from_secs(12);

async fn references(fixture: &Fixture, key: &str) -> Vec<Value> {
    let response = fixture
        .s3(Method::GET, key, &[("ipfs3-pin-status", "")], vec![])
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    response.json::<Value>().await.unwrap()["references"]
        .as_array()
        .unwrap()
        .clone()
}

async fn wait_for_status(fixture: &Fixture, expected: &str) {
    timeout(WAIT, async {
        while resource(fixture)
            .await
            .as_ref()
            .map(|(status, _)| status.as_str())
            != Some(expected)
        {
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Cluster resource did not reach {expected}"));
}

async fn resource(fixture: &Fixture) -> Option<(String, Option<String>)> {
    let db = fixture.state.store.db();
    db.query_one(Statement::from_string(
        db.get_database_backend(),
        "SELECT status, handle FROM pin_resources",
    ))
    .await
    .unwrap()
    .map(|row| {
        (
            row.try_get("", "status").unwrap(),
            row.try_get("", "handle").unwrap(),
        )
    })
}

async fn wait_for_release(fixture: &Fixture) {
    wait_for_status(fixture, "cleaned").await;
    assert_eq!(resource(fixture).await.unwrap().1, None);
}

#[tokio::test]
async fn stage6_late_accepted_cluster_submit_releases_only_after_native_delete_ack() {
    use axum::{Json, Router};
    use http::Uri;
    use tokio::sync::{mpsc, oneshot};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (calls, mut received) = mpsc::unbounded_channel();
    let app = Router::new().fallback(move |method: Method, uri: Uri| {
        let calls = calls.clone();
        async move {
            let (reply, gate) = oneshot::channel::<()>();
            calls.send((method, uri, reply)).unwrap();
            gate.await.unwrap();
            Json(json!({"cid": fixture::CID}))
        }
    });
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut fixture = Fixture::configured("rest", false, "sqlite::memory:", |config| {
        config.pinning.providers[0]
            .cluster
            .as_mut()
            .unwrap()
            .rest_endpoint = endpoint;
    })
    .await;
    fixture.put("in-flight").await;
    fixture.start();
    let (method, uri, accept) = timeout(WAIT, received.recv())
        .await
        .expect("Cluster POST")
        .unwrap();
    assert_eq!(method, Method::POST);
    assert_eq!(uri.path(), format!("/pins/{}", fixture::canonical()));
    assert_eq!(
        references(&fixture, "in-flight").await[0]["status"],
        "pending"
    );
    let response = fixture.s3(Method::DELETE, "in-flight", &[], vec![]).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(resource(&fixture).await, Some(("pending".into(), None)));
    accept.send(()).unwrap();

    let (method, uri, ack) = timeout(WAIT, received.recv())
        .await
        .expect("native Cluster DELETE")
        .unwrap();
    assert_eq!(method, Method::DELETE);
    assert_eq!(uri.path(), format!("/pins/{}", fixture::CID));
    assert_eq!(
        resource(&fixture).await,
        Some(("pending".into(), Some(fixture::CID.into())))
    );
    ack.send(()).unwrap();
    wait_for_release(&fixture).await;
    fixture.stop().await;
    assert!(
        received.try_recv().is_err(),
        "unexpected Cluster request after the delete ACK"
    );
    server.abort();
}

async fn signed_shared_expiry(protocol: &str, database: &str) {
    let mut fixture = Fixture::new(protocol, true, database).await;
    let target = if protocol == "proxy" {
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .and(query_param("arg", fixture::canonical()))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"Pins": [fixture::CID]})))
            .expect(1)
            .mount(&fixture.proxy)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/rm"))
            .and(query_param("arg", fixture::CID))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"Pins": [fixture::CID]})))
            .expect(1)
            .mount(&fixture.proxy)
            .await;
        &fixture.proxy
    } else {
        Mock::given(method("POST"))
            .and(path(format!("/pins/{}", fixture::canonical())))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"cid": fixture::CID})))
            .expect(1)
            .mount(&fixture.rest)
            .await;
        Mock::given(method("DELETE"))
            .and(path(format!("/pins/{}", fixture::CID)))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&fixture.rest)
            .await;
        &fixture.rest
    };
    fixture.put("first").await;
    fixture.put("second").await;
    assert!(target.received_requests().await.unwrap().is_empty());
    let before = references(&fixture, "first").await;
    assert_eq!(before.len(), 1);
    assert_eq!(before[0]["cid"], fixture::canonical());
    assert_eq!(before[0]["source"], "automatic");
    assert_eq!(before[0]["status"], "waiting");

    fixture.start();
    wait_for_status(&fixture, "accepted").await;
    let first = references(&fixture, "first").await;
    let second = references(&fixture, "second").await;
    assert_eq!(second.len(), 1);
    assert_eq!(second[0]["provider"], first[0]["provider"]);
    assert_eq!(second[0]["status"], "accepted");
    assert_eq!(
        resource(&fixture).await,
        Some(("accepted".into(), Some(fixture::CID.into())))
    );
    let tags = fixture
        .s3(Method::GET, "first", &[("tagging", "")], vec![])
        .await;
    assert_eq!(tags.status(), StatusCode::OK);
    assert!(
        tags.text()
            .await
            .unwrap()
            .contains("<Key>audit</Key><Value>cluster</Value>")
    );
    let replacement =
        b"<Tagging><TagSet><Tag><Key>audit</Key><Value>changed</Value></Tag></TagSet></Tagging>";
    let response = fixture
        .s3(
            Method::PUT,
            "first",
            &[("tagging", "")],
            replacement.to_vec(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let tags = fixture
        .s3(Method::GET, "first", &[("tagging", "")], vec![])
        .await;
    assert_eq!(tags.status(), StatusCode::OK);
    assert!(
        tags.text()
            .await
            .unwrap()
            .contains("<Key>audit</Key><Value>changed</Value>")
    );
    assert_eq!(references(&fixture, "first").await, first);

    let db = fixture.state.store.db();
    db.execute_unprepared("UPDATE pin_refs SET expires_at=0 WHERE owner_object_id=(SELECT id FROM objects WHERE key='first')").await.unwrap();
    timeout(WAIT, async {
        loop {
            let row = db
                .query_one(Statement::from_string(
                    db.get_database_backend(),
                    "SELECT COUNT(*) AS count FROM pin_refs",
                ))
                .await
                .unwrap()
                .unwrap();
            if row.try_get::<i64>("", "count").unwrap() == 1 {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("worker removes the first expired reference");
    assert!(references(&fixture, "first").await.is_empty());
    assert_eq!(
        references(&fixture, "second").await[0]["status"],
        "accepted"
    );
    let target = if protocol == "proxy" {
        &fixture.proxy
    } else {
        &fixture.rest
    };
    assert_eq!(target.received_requests().await.unwrap().len(), 1);

    db.execute_unprepared("UPDATE pin_refs SET expires_at=0")
        .await
        .unwrap();
    wait_for_release(&fixture).await;
    assert!(references(&fixture, "second").await.is_empty());
    fixture.stop().await;
    let (target, unused) = if protocol == "proxy" {
        (&fixture.proxy, &fixture.rest)
    } else {
        (&fixture.rest, &fixture.proxy)
    };
    assert_eq!(target.received_requests().await.unwrap().len(), 2);
    assert!(unused.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn stage6_signed_cluster_protocols_share_one_resource_until_last_expiry() {
    for protocol in ["proxy", "rest"] {
        signed_shared_expiry(protocol, "sqlite::memory:").await;
    }
}

#[tokio::test]
async fn stage6_signed_one_policy_selects_the_available_cluster() {
    for unavailable in ["disabled", "retired", "capacity"] {
        let mut fixture = Fixture::configured("rest", true, "sqlite::memory:", |config| {
            config.pinning.policies[0].provider_mode = "one".into();
            config.pinning_identity.providers[1].scope = "pinset-b".into();
            config.pinning_identity.providers[1].storage_domain = "cluster-b".into();
            config.pinning.providers[1].priority = 2;
            match unavailable {
                "disabled" => config.pinning.providers[0].enabled = false,
                "retired" => config.pinning_identity.providers[0].retired = true,
                _ => config.pinning.providers[0].max_bytes = 1,
            }
        })
        .await;
        Mock::given(method("POST"))
            .and(path(format!("/pins/{}", fixture::canonical())))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"cid": fixture::CID})))
            .expect(1)
            .mount(&fixture.rest)
            .await;
        let selected = &fixture.state.pinning.effective_config().providers[1].name;
        fixture.put("object").await;
        let before = references(&fixture, "object").await;
        assert_eq!(before.len(), 1);
        assert_eq!(before[0]["provider"], selected.as_str(), "{unavailable}");
        fixture.start();
        wait_for_status(&fixture, "accepted").await;
        assert_eq!(
            references(&fixture, "object").await[0]["status"],
            "accepted"
        );
        fixture.stop().await;
        assert_eq!(fixture.rest.received_requests().await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn stage6_failed_cluster_writes_do_not_claim_a_handle_or_schedule_cleanup() {
    for (reply, status) in [
        (
            ResponseTemplate::new(401).set_body_string("redact-this-upstream-body"),
            "failed",
        ),
        (
            ResponseTemplate::new(200).set_body_json(json!({"Pins":[fixture::other_cid()]})),
            "unknown",
        ),
        (
            ResponseTemplate::new(200)
                .set_body_json(json!({"Pins":[fixture::CID, fixture::other_cid()]})),
            "unknown",
        ),
        (
            ResponseTemplate::new(200).set_body_json(json!({"Pins":[]})),
            "unknown",
        ),
        (ResponseTemplate::new(503), "unknown"),
    ] {
        let mut fixture = Fixture::new("proxy", false, "sqlite::memory:").await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(reply)
            .expect(1)
            .mount(&fixture.proxy)
            .await;
        fixture.put("object").await;
        fixture.start();
        wait_for_status(&fixture, status).await;
        let refs = references(&fixture, "object").await;
        assert_eq!(refs[0]["status"], status);
        assert_eq!(resource(&fixture).await.unwrap().1, None);
        assert_eq!(
            fixture
                .s3(Method::DELETE, "object", &[], vec![])
                .await
                .status(),
            StatusCode::NO_CONTENT
        );
        if status == "unknown" {
            assert_eq!(resource(&fixture).await, Some(("unknown".into(), None)));
        } else {
            assert!(resource(&fixture).await.is_none());
        }
        fixture.stop().await;
        assert_eq!(fixture.proxy.received_requests().await.unwrap().len(), 1);
        assert!(fixture.rest.received_requests().await.unwrap().is_empty());
    }
}

#[tokio::test]
#[ignore = "requires explicit fresh isolated PostgreSQL database"]
async fn stage6_cluster_postgres_signed_lifecycle() {
    let database = std::env::var("IPFS_S3_STAGE6_TEST_DATABASE_URL")
        .expect("explicit isolated PostgreSQL URL required");
    assert!(database.starts_with("postgres://") || database.starts_with("postgresql://"));
    let db = ipfs_s3_gateway::store::connect_database(&database)
        .await
        .expect("connect to selected isolated PostgreSQL");
    let schema = sea_orm_migration::SchemaManager::new(&db);
    assert!(
        !schema.has_table("buckets").await.unwrap(),
        "requires a fresh isolated database"
    );
    db.close().await.unwrap();
    signed_shared_expiry("rest", &database).await;
}
