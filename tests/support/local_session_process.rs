use super::{reserve_loopback_address, spawn_gateway, wait_until_listening};
use s3::{Bucket, Region, creds::Credentials};
use std::{
    fs,
    process::{Command, Stdio},
};

#[tokio::test]
async fn issued_credentials_work_across_gateways_with_the_same_local_policy() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("config.toml");
    fs::write(
        &config,
        format!(
            r#"
[auth]
session_signing_key = "{}"
[[auth.credentials]]
access_key = "test"
secret_key = "test"
[[auth.policy]]
user = "test"
bucket = "permitted-bucket"
effect = "allow"
actions = ["CreateBucket", "GetObject"]
"#,
            "32".repeat(32)
        ),
    )
    .unwrap();
    let output = tempfile::tempfile().unwrap();
    let stdout = output.try_clone().unwrap();
    let status = Command::new(env!("CARGO_BIN_EXE_ipfs-s3-gateway"))
        .args(["--issue-session", "test", "900"])
        .env("IPFS_S3_CONFIG", &config)
        .env("IPFS_S3_ACCESS_KEY_ID", "test")
        .env("IPFS_S3_SECRET_ACCESS_KEY", "test")
        .env_remove("IPFS_S3_SESSION_SIGNING_KEY")
        .stdout(Stdio::from(stdout))
        .status()
        .unwrap();
    assert!(status.success());
    use std::io::Seek;
    let mut output = output;
    output.rewind().unwrap();
    let document: serde_json::Value = serde_json::from_reader(output).unwrap();
    assert_eq!(document["Version"], 1);
    let credentials = Credentials::new(
        document["AccessKeyId"].as_str(),
        document["SecretAccessKey"].as_str(),
        None,
        Some(document["SessionToken"].as_str().unwrap()),
        None,
    )
    .unwrap();
    let client = reqwest::Client::new();
    for _ in 0..2 {
        let address = reserve_loopback_address();
        let stdout = temp.path().join("stdout.log");
        let stderr = temp.path().join("stderr.log");
        let mut gateway =
            spawn_gateway(address, &temp, "warn", "sqlite::memory:", &stdout, &stderr);
        wait_until_listening(address, &mut gateway.0, &stdout, &stderr);
        let region = Region::Custom {
            region: "us-east-1".to_owned(),
            endpoint: format!("http://{address}"),
        };
        let created = Bucket::create_with_path_style(
            "permitted-bucket",
            region.clone(),
            credentials.clone(),
            Default::default(),
        )
        .await
        .unwrap();
        assert_eq!(created.response_code, 200);
        for (name, expected) in [("permitted-bucket", 404), ("other-bucket", 403)] {
            let bucket = Bucket::new(name, region.clone(), credentials.clone())
                .unwrap()
                .with_path_style();
            let url = bucket.presign_get("missing", 60, None).await.unwrap();
            let response = client.get(url).send().await.unwrap();
            assert_eq!(response.status().as_u16(), expected);
        }
    }
}
