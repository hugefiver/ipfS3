use super::support;
use ipfs_s3_gateway::store::entities::pin_job;
use sea_orm::EntityTrait;

#[tokio::test]
async fn opt_in_run_uses_only_applied_receipt_jobs_not_the_old_queue() {
    let fixture = support::Fixture::new(false).await;
    let unrelated = fixture.unrelated_queue().await;
    assert!(!unrelated.is_empty());
    fixture.prepare().await;
    let confirmed = fixture.file("confirmed.json");
    support::report(&fixture.confirm(&confirmed, "300", false).await);
    let applied = fixture.import("--apply", &confirmed, false).await;
    let no_run = support::report(&applied);
    assert_eq!(no_run["run_requested"], false);
    for job in &unrelated {
        assert_eq!(
            pin_job::Entity::find_by_id(job.id.clone())
                .one(&fixture.db)
                .await
                .unwrap()
                .unwrap(),
            *job
        );
    }
    let result = fixture.import("--apply", &confirmed, true).await;
    let run = support::report(&result);
    assert_eq!(run["replayed"], true);
    assert_eq!(run["run"]["passes"], 1);
    assert!(run["run"]["submit_observations"].is_array());
    assert!(matches!(
        run["items"][0]["state"].as_str().unwrap(),
        "queued" | "waiting" | "remote_complete" | "needs_attention"
    ));
    for job in &unrelated {
        assert_eq!(
            pin_job::Entity::find_by_id(job.id.clone())
                .one(&fixture.db)
                .await
                .unwrap()
                .unwrap(),
            *job
        );
    }
    fixture.assert_redacted(&result);
    let status_before = fixture.frame().await;
    let result = fixture.import("--status", &confirmed, false).await;
    support::report(&result);
    assert!(fixture.frame().await == status_before);
}

#[cfg(unix)]
#[tokio::test]
async fn signal_cancels_only_the_cli_owned_offline_read_and_leaves_no_preparation_record() {
    use std::{process::Stdio, time::Duration};
    use wiremock::{
        Mock, ResponseTemplate,
        matchers::{method, path},
    };
    let fixture = support::Fixture::new(false).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(b"0123456789")
                .set_delay(Duration::from_secs(20)),
        )
        .with_priority(1)
        .mount(&fixture.server)
        .await;
    let input = fixture.prepare_file(false);
    let before = fixture.frame().await;
    let mut command = fixture.command(&["--pinning-manage", "--prepare", input.to_str().unwrap()]);
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let child = command.spawn().unwrap();
    let pid = child.id().unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if fixture
                .server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .any(|request| request.url.path() == "/api/v0/cat")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("owned CLI never reached its explicit local source read");
    let sent = tokio::process::Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .await
        .unwrap();
    assert!(sent.success());
    let result = tokio::time::timeout(Duration::from_secs(7), child.wait_with_output())
        .await
        .expect("management CLI did not drain its own cancelled work")
        .unwrap();
    assert!(!result.status.success());
    assert!(fixture.frame().await == before);
    fixture.assert_redacted(&result);
}
