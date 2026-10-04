//! Stage5: real leaf-provider transport + exact DAG/stored-byte acceptance only.
//! Not configuration/worker/account/production-main or S3 SSE SDK acceptance.
//! Run with tests/run-ipfs-rpc-real.ps1; ordinary cargo test ignores the real case.
#[path = "support/ipfs_rpc_real_car.rs"]
mod car;
#[path = "support/ipfs_rpc_real_fixture.rs"]
mod fixture;
#[path = "support/ipfs_rpc_real_import.rs"]
mod import;

use anyhow::{Context, Result, ensure};
use fixture::{Fixture, Rpc};
use ipfs_s3_gateway::{
    crypto::{aes_gcm::encrypt_chunk, key::ObjectKey},
    kubo::KuboClient,
    pinning::{
        identity::Ownership,
        ipfs_rpc::{
            IpfsRpcProvider, RpcAuth, RpcPinKind, RpcPinObservation, RpcProfile, RpcResourceStatus,
            RpcStrategy, RpcSubmitEffect, RpcTimeouts,
        },
        provider::{RemotePinStatus, SubmitPin},
    },
};
use std::{collections::BTreeMap, time::Duration};

struct Case {
    name: &'static str,
    cid: String,
    car: Vec<u8>,
    files: Vec<(String, Vec<u8>)>,
}

fn request(cid: &str) -> SubmitPin {
    SubmitPin {
        cid: cid.to_owned(),
        name: "real-rpc-fixture".into(),
        metadata: BTreeMap::new(),
    }
}

fn provider(
    source: &Rpc,
    target: &Rpc,
    strategy: RpcStrategy,
    auth: Option<RpcAuth>,
) -> Result<IpfsRpcProvider> {
    Ok(IpfsRpcProvider::new_with_timeouts(
        "real-kubo".into(),
        target.base.clone(),
        KuboClient::new(source.base.clone()),
        RpcProfile::Kubo,
        strategy,
        auth,
        RpcTimeouts {
            connect: Duration::from_secs(5),
            control: Duration::from_secs(60),
            idle: Duration::from_secs(30),
        },
    )?)
}

async fn submit(provider: &IpfsRpcProvider, cid: &str) -> Result<()> {
    let observation = provider.submit_observed(request(cid)).await;
    ensure!(
        observation.effect == RpcSubmitEffect::Observed,
        "submit evidence: {observation:?}"
    );
    ensure!(
        observation.resources.len() == 1,
        "not exact single-root evidence: {observation:?}"
    );
    let resource = &observation.resources[0];
    ensure!(
        resource.cid == car::canonical(cid)?
            && resource.ownership == Ownership::Unknown
            && resource.status == RpcResourceStatus::RecursiveVerified,
        "invalid evidence: {observation:?}"
    );
    let pin = observation.result?;
    ensure!(
        pin.cid == car::canonical(cid)? && pin.status == RemotePinStatus::Pinned,
        "provider accepted a noncanonical/wrong root: {pin:?}"
    );
    println!(
        "provider_submit cid={} recursive_verified=true ownership=Unknown",
        pin.cid
    );
    Ok(())
}

async fn file(source: &Rpc, name: &'static str, bytes: Vec<u8>, chunker: &str) -> Result<Case> {
    let cid = source.add(&bytes, chunker).await?;
    let car = source.export(&cid).await?;
    let blocks = car::blocks(&car, &cid)?;
    if bytes.len() <= 262144 && chunker == "size-262144" {
        ensure!(
            car::canonical(&cid)? == car::raw_cid(&bytes)?,
            "raw/empty CID is not SHA-256 of all bytes"
        );
        ensure!(blocks.len() == 1, "small raw file is not one block");
    } else {
        ensure!(
            blocks.len() > 1,
            "multi-block fixture collapsed to one block"
        );
    }
    println!(
        "input name={name} cid={} bytes={} blocks={} chunker={chunker}",
        car::canonical(&cid)?,
        bytes.len(),
        blocks.len()
    );
    Ok(Case {
        name,
        cid,
        car,
        files: vec![(String::new(), bytes)],
    })
}

async fn verify(target: &Rpc, provider: &IpfsRpcProvider, case: &Case) -> Result<()> {
    ensure!(
        provider.observe_pin(&case.cid).await? == RpcPinObservation::Present(RpcPinKind::Recursive),
        "target root is not recursively pinned"
    );
    let path = format!("/ipfs/{}", case.cid);
    let stat = target
        .json(
            "files/stat",
            &[("arg", &path), ("with-local", "true"), ("offline", "true")],
        )
        .await?;
    ensure!(
        stat["WithLocality"] == true
            && stat["Local"] == true
            && car::canonical(stat["Hash"].as_str().context("stat CID")?)?
                == car::canonical(&case.cid)?,
        "incomplete offline local DAG: {stat}"
    );
    let target_car = target.export(&case.cid).await?;
    ensure!(
        car::blocks(&target_car, &case.cid)? == car::blocks(&case.car, &case.cid)?,
        "source and target complete single-root DAG block sets differ"
    );
    for (suffix, expected) in &case.files {
        let path = format!("{}{}", case.cid, suffix);
        let actual = target
            .bytes("cat", &[("arg", &path), ("offline", "true")])
            .await?;
        ensure!(
            actual == *expected,
            "{}: all stored bytes differ at {suffix}",
            case.name
        );
    }
    println!(
        "target_verify name={} cid={} recursive_pin=true single_root_car=true complete_local=true all_bytes=true offline=true",
        case.name,
        car::canonical(&case.cid)?
    );
    Ok(())
}

/// Narrow diagnostic: toggles only pin-roots on the exact same complete CAR.
/// It deliberately reproduces the legacy fixture assertion, not provider QA.
#[tokio::test]
#[ignore = "requires owned offline Kubo nodes; diagnostic only, not full acceptance"]
async fn rpc_preseed_import_contract_probe() -> Result<()> {
    let source_url = std::env::var("IPFS_S3_REAL_RPC_SOURCE_URL")?;
    let target_url = std::env::var("IPFS_S3_REAL_RPC_TARGET_URL")?;
    let owned = Fixture::load()?;
    owned.verify(&source_url, &target_url)?;
    let source = Rpc::new(source_url)?;
    let target = Rpc::new(target_url)?;
    for rpc in [&source, &target] {
        let version = rpc.json("version", &[]).await?;
        ensure!(version["Version"] == "0.43.0", "wrong probe version");
        println!(
            "probe_kubo_version={} node={}",
            version["Version"], rpc.base
        );
    }
    let case = file(
        &source,
        "cid-explicit-preseed",
        (0..590_123).map(|i| ((i * 37) % 251) as u8).collect(),
        "size-65536",
    )
    .await?;
    let blocks = car::blocks(&case.car, &case.cid)?;
    let block_bytes: usize = blocks.values().map(Vec::len).sum();
    println!(
        "probe_source_car version=1 header_hex={} roots=[{}] complete_blocks={} block_bytes={block_bytes} car_bytes={}",
        hex::encode(car::header(&case.car, &case.cid)?),
        car::canonical(&case.cid)?,
        blocks.len(),
        case.car.len()
    );
    let observer = provider(&source, &target, RpcStrategy::Cid, None)?;
    ensure!(observer.observe_pin(&case.cid).await? == RpcPinObservation::Absent);
    for pin in [false, true] {
        let raw = import::exchange(&target.base, &case.car, pin).await?;
        let records: Vec<serde_json::Value> = std::str::from_utf8(&raw)?
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<_, _>>()?;
        let roots = records.iter().filter(|r| r.get("Root").is_some()).count();
        println!(
            "probe pin_roots={pin} root_records={roots} legacy_fixture_expected=1 legacy_would_fail={}",
            roots != 1
        );
        ensure!(roots == usize::from(pin), "unexpected raw Root emission");
        let stats: Vec<_> = records.iter().filter_map(|r| r.get("Stats")).collect();
        ensure!(stats.len() == 1, "not exact stats record");
        ensure!(stats[0]["BlockCount"] == blocks.len());
        ensure!(stats[0]["BlockBytesCount"] == block_bytes);
        ensure!(car::blocks(&target.export(&case.cid).await?, &case.cid)? == blocks);
        for (_, expected) in &case.files {
            ensure!(
                target
                    .bytes("cat", &[("arg", &case.cid), ("offline", "true")])
                    .await?
                    == *expected
            );
        }
        let expected = if pin {
            RpcPinObservation::Present(RpcPinKind::Recursive)
        } else {
            RpcPinObservation::Absent
        };
        ensure!(observer.observe_pin(&case.cid).await? == expected);
        println!(
            "probe pin_roots={pin} complete_header_blocks=true all_bytes=true offline=true pin_observation={expected:?}"
        );
    }
    println!("preseed_probe=PASS diagnostic_only=true no_provider_submit=true no_pin_rm=true");
    Ok(())
}

#[tokio::test]
#[ignore = "requires an owned two-node Kubo v0.43.0 fixture; missing URLs are errors"]
async fn rpc_provider_real_transports_and_local_dags() -> Result<()> {
    // Explicit --ignored without configuration MUST fail, not skip/pass.
    let source_url = std::env::var("IPFS_S3_REAL_RPC_SOURCE_URL").context(
        "IPFS_S3_REAL_RPC_SOURCE_URL is required for explicitly requested real RPC acceptance",
    )?;
    let target_url = std::env::var("IPFS_S3_REAL_RPC_TARGET_URL").context(
        "IPFS_S3_REAL_RPC_TARGET_URL is required for explicitly requested real RPC acceptance",
    )?;
    for key in [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ] {
        ensure!(
            std::env::var(key).unwrap_or_default().is_empty(),
            "real own-node acceptance forbids proxy environment: {key}"
        );
    }
    let owned = Fixture::load()?;
    owned.verify(&source_url, &target_url)?;
    let source = Rpc::new(source_url)?;
    let target = Rpc::new(target_url)?;
    for rpc in [&source, &target] {
        let version = rpc.json("version", &[]).await?;
        ensure!(
            version["Version"] == "0.43.0",
            "unexpected Kubo version: {version}"
        );
        println!("kubo_version={} node={}", version["Version"], rpc.base);
    }
    let source_identity = source.json("id", &[]).await?["ID"]
        .as_str()
        .context("source identity")?
        .to_owned();
    let target_identity = target.json("id", &[]).await?["ID"]
        .as_str()
        .context("target identity")?
        .to_owned();
    ensure!(
        !source_identity.is_empty()
            && !target_identity.is_empty()
            && source_identity != target_identity,
        "source/target identities must be independent"
    );
    println!("source_identity={source_identity} target_identity={target_identity}");

    let upload = provider(&source, &target, RpcStrategy::Upload, None)?;
    let car_provider = provider(&source, &target, RpcStrategy::Car, None)?;
    let cid_provider = IpfsRpcProvider::new(
        "real-cid".into(),
        target.base.clone(),
        KuboClient::new(source.base.clone()),
        RpcProfile::Kubo,
        RpcStrategy::Cid,
        None,
    )?;
    let mut cases = Vec::new();
    for (name, bytes) in [
        ("empty-raw", Vec::new()),
        ("small-raw", b"real RPC stored bytes\0\xff".to_vec()),
        (
            "multi-block",
            (0..800_123)
                .map(|i| ((i * 73 + i / 251) % 256) as u8)
                .collect(),
        ),
    ] {
        let case = file(&source, name, bytes, "size-262144").await?;
        ensure!(
            upload.observe_pin(&case.cid).await? == RpcPinObservation::Absent,
            "target not initially independent"
        );
        // The empty raw CAR really reaches an initially empty target, including
        // its legal zero-block-byte stats; Upload then repeats the exact root.
        if name == "empty-raw" {
            submit(&car_provider, &case.cid).await?;
            verify(&target, &car_provider, &case).await?;
        }
        if let Err(error) = submit(&upload, &case.cid).await {
            let response = target.diagnose_upload(&source, &case.cid).await;
            eprintln!(
                "diagnostic_direct_streamed_add pin=false not_provider_acceptance cid={} response={response:?}",
                case.cid
            );
            return Err(error);
        }
        verify(&target, &upload, &case).await?;
        cases.push(case);
    }

    let cid_case = file(
        &source,
        "cid-explicit-preseed",
        (0..590_123).map(|i| ((i * 37) % 251) as u8).collect(),
        "size-65536",
    )
    .await?;
    ensure!(
        cid_provider.observe_pin(&cid_case.cid).await? == RpcPinObservation::Absent,
        "CID fixture root was pinned before unpinned preseed"
    );
    target.preseed(&cid_case.car, &cid_case.cid, false).await?;
    ensure!(
        cid_provider.observe_pin(&cid_case.cid).await? == RpcPinObservation::Absent,
        "CID preseed unexpectedly pinned"
    );
    println!("cid_preseed pin_roots=false pin_observation=Absent before_provider_submit=true");
    submit(&cid_provider, &cid_case.cid).await?;
    verify(&target, &cid_provider, &cid_case).await?;
    cases.push(cid_case);

    let arbitrary = file(
        &source,
        "arbitrary-chunker-car",
        (0..700_019)
            .map(|i| ((i * 13 + i / 71) % 256) as u8)
            .collect(),
        "size-65536",
    )
    .await?;
    let mismatch = upload.submit_observed(request(&arbitrary.cid)).await;
    ensure!(
        mismatch.result.is_err()
            && !mismatch.resources.is_empty()
            && mismatch
                .resources
                .iter()
                .all(|r| r.cid != car::canonical(&arbitrary.cid).unwrap()
                    && r.ownership == Ownership::Unknown),
        "byte upload must not accept a changed UnixFS root: {mismatch:?}"
    );
    ensure!(
        upload.observe_pin(&arbitrary.cid).await? == RpcPinObservation::Absent,
        "wrong original root was pinned"
    );
    submit(&car_provider, &arbitrary.cid).await?;
    verify(&target, &car_provider, &arbitrary).await?;
    cases.push(arbitrary);

    let plaintext: Vec<u8> = (0..600_017).map(|i| ((i * 19) % 253) as u8).collect();
    let ciphertext =
        encrypt_chunk(&ObjectKey { bytes: [0x63; 32] }, &[0x17; 12], &plaintext)?.to_vec();
    ensure!(
        ciphertext != plaintext,
        "ciphertext fixture equals plaintext"
    );
    let encrypted = file(
        &source,
        "gateway-crypto-stored-ciphertext-not-sse-sdk",
        ciphertext,
        "size-262144",
    )
    .await?;
    submit(&upload, &encrypted.cid).await?;
    verify(&target, &upload, &encrypted).await?;
    cases.push(encrypted);

    source
        .bytes(
            "files/mkdir",
            &[("arg", "/rpc-real/nested"), ("parents", "true")],
        )
        .await?;
    source
        .bytes(
            "files/cp",
            &[
                ("arg", &format!("/ipfs/{}", cases[1].cid)),
                ("arg", "/rpc-real/a.bin"),
            ],
        )
        .await?;
    source
        .bytes(
            "files/cp",
            &[
                ("arg", &format!("/ipfs/{}", cases[4].cid)),
                ("arg", "/rpc-real/nested/b.bin"),
            ],
        )
        .await?;
    let directory_cid = source.json("files/stat", &[("arg", "/rpc-real")]).await?["Hash"]
        .as_str()
        .context("directory CID")?
        .to_owned();
    let directory = Case {
        name: "directory-complete-car",
        car: source.export(&directory_cid).await?,
        cid: directory_cid,
        files: vec![
            ("/a.bin".into(), cases[1].files[0].1.clone()),
            ("/nested/b.bin".into(), cases[4].files[0].1.clone()),
        ],
    };
    println!(
        "input name={} cid={} blocks={} kind=directory",
        directory.name,
        car::canonical(&directory.cid)?,
        car::blocks(&directory.car, &directory.cid)?.len()
    );
    let rejected = upload.submit_observed(request(&directory.cid)).await;
    ensure!(
        rejected.result.is_err()
            && rejected.resources.is_empty()
            && rejected.effect == RpcSubmitEffect::NotSubmitted,
        "directory byte upload was not rejected before mutation: {rejected:?}"
    );
    submit(&car_provider, &directory.cid).await?;
    verify(&target, &car_provider, &directory).await?;
    cases.push(directory);

    let shared = file(
        &source,
        "preexisting-shared-pin",
        b"pin owned by someone else in the isolated fixture".to_vec(),
        "size-262144",
    )
    .await?;
    ensure!(cid_provider.observe_pin(&shared.cid).await? == RpcPinObservation::Absent);
    target.preseed(&shared.car, &shared.cid, true).await?;
    ensure!(
        cid_provider.observe_pin(&shared.cid).await?
            == RpcPinObservation::Present(RpcPinKind::Recursive)
    );
    println!("shared_preexisting_pin=true recursive_verified=true before_provider_submit=true");
    // Anonymous Kubo exercises actual None/Basic/Bearer header transport only;
    // this is NOT a claim of authorization enforcement (adapter mocks own it).
    for (strategy, auth) in [
        (RpcStrategy::Cid, None),
        (
            RpcStrategy::Upload,
            Some(RpcAuth::Basic {
                username: "fixture-only".into(),
                password: owned.run_id.clone(),
            }),
        ),
        (
            RpcStrategy::Car,
            Some(RpcAuth::Bearer(format!("fixture-only-{}", owned.run_id))),
        ),
    ] {
        let existing = provider(&source, &target, strategy, auth)?;
        submit(&existing, &shared.cid).await?;
        verify(&target, &existing, &shared).await?;
    }
    cases.push(shared);

    owned.stop_source()?;
    ensure!(
        source.json("id", &[]).await.is_err(),
        "source RPC still reachable after isolation"
    );
    for case in &cases {
        verify(&target, &cid_provider, case).await?;
    }
    ensure!(
        target.json("id", &[]).await?["ID"] == target_identity,
        "target identity changed"
    );
    println!(
        "real_rpc_acceptance=PASS source_stopped=true no_external_swarm=true cid_transport=explicit_preseed no_pin_rm=true scope=leaf_provider_dag_stored_bytes_only"
    );
    Ok(())
}
