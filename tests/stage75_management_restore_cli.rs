#[path = "support/stage7_management_cli.rs"]
#[allow(dead_code, unused_imports)]
mod support;

use ipfs_s3_gateway::pinning::management::wire::v3::*;

#[tokio::test]
async fn original_stream_restores_and_committed_replay_survives_expiry_and_config_drift() {
    let fixture = support::Fixture::new(true).await;
    fixture.prepare().await;
    let original = fixture.file("original.v3");
    support::report(&fixture.confirm(&original, "10", true).await);
    let calls = fixture.remote_calls().await;
    let before = fixture.frame().await;
    let status = fixture.import("--status", &original, false).await;
    assert_eq!(support::report(&status)["items"][0]["state"], "not_applied");
    assert!(fixture.frame().await == before);
    let applied = support::report(&fixture.import("--apply", &original, false).await);
    assert_eq!(applied["replayed"], false);
    let before = fixture.frame().await;

    for field in [
        "scope",
        "identity",
        "principal",
        "created",
        "expires",
        "catalog",
        "configuration",
        "item_step",
        "summary",
        "acks",
        "native",
        "claims",
    ] {
        let changed = fixture.file(&format!("changed-{field}.v3"));
        let mut edited = false;
        support::rewrite(
            &original,
            &changed,
            |event, _| {
                match event {
                    Event::Frame(Frame::Header {
                        database_scope,
                        database_identity,
                        principal,
                        created_at,
                        expires_at,
                        catalog,
                        ..
                    }) => match field {
                        "scope" => database_scope.push_str("-forged"),
                        "identity" => database_identity.push_str("-forged"),
                        "principal" => principal.push_str("-forged"),
                        "created" => *created_at = "2000-01-01T00:00:00Z".into(),
                        "expires" => *expires_at = Some("2099-01-01T00:00:00Z".into()),
                        "catalog" => catalog.push("forged-catalog".into()),
                        _ => {}
                    },
                    Event::Frame(Frame::Acknowledgements { item_ids }) if field == "acks" => {
                        *item_ids = None
                    }
                    Event::Frame(Frame::Configuration {
                        configuration: Some(configuration),
                    }) if field == "configuration" => configuration.source_node = None,
                    Event::Frame(Frame::Item { entry }) if field == "item_step" => {
                        entry.step = None
                    }
                    Event::Frame(Frame::Summary { summary }) if field == "summary" => {
                        summary.logical_intent_bytes += 1
                    }
                    Event::Chunk(bytes) if field == "native" && !edited => {
                        // Public native UTF-8 bytes; retain a valid terminal digest.
                        bytes.push(b'x');
                        edited = true;
                    }
                    _ => {}
                }
            },
            |writer| {
                if field == "claims" {
                    writer
                        .frame(&Frame::Row {
                            section: Section::AppliedClaim,
                            key: ipfs_s3_gateway::pinning::management::NativeKey {
                                table: "management_claim".into(),
                                values: vec!["0".into()],
                            },
                            cells: 0,
                        })
                        .unwrap();
                    writer.frame(&Frame::RowEnd).unwrap();
                }
            },
        );
        assert!(
            !fixture
                .import("--status", &changed, false)
                .await
                .status
                .success(),
            "accepted changed {field}"
        );
        assert!(fixture.frame().await == before);
    }
    fixture.wait_for_confirmation_expiry().await;
    let mut config: toml::Value =
        toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    let max_pins = config["pinning"]["providers"][0]["max_pins"]
        .as_integer()
        .unwrap();
    config["pinning"]["providers"][0]["max_pins"] = (max_pins + 1).into();
    std::fs::write(&fixture.config_path, toml::to_string(&config).unwrap()).unwrap();
    let after = fixture.frame().await;
    let replay = support::report(&fixture.import("--apply", &original, false).await);
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["items"], applied["items"]);
    assert!(fixture.frame().await == after);
    assert_eq!(fixture.remote_calls().await, calls);
}
