//! Independent-source provider acceptance. Every content write is source-only until
//! ClusterProvider dispatches the real Cluster add; no target preload or removal.
use crate::{
    car, cluster, cluster_pinning_real_fixture::RealClusterFixture, pinning, stored_crypto,
};
use anyhow::{Context, Result, ensure};
use cluster::provider::{
    ClusterFailureCode, ClusterProtocol, ClusterProvider, ClusterProviderOptions,
    ClusterResourceStatus, ClusterSource, ClusterStrategy, ClusterWriteEffect,
};
use cluster::{ClusterPinset, ReplicaOutcome};
use futures_util::StreamExt;
use pinning::{identity::Ownership, provider::SubmitPin};
use reqwest::{Client, Method};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

const LIMIT: usize = 16 * 1024 * 1024;

struct Leaf {
    path: String,
    bytes: Vec<u8>,
    plaintext: Option<Vec<u8>>,
}
struct Dag {
    name: String,
    cid: String,
    blocks: BTreeMap<String, Vec<u8>>,
    leaves: Vec<Leaf>,
    protocol: ClusterProtocol,
    strategy: ClusterStrategy,
}

struct Fixture {
    owned: RealClusterFixture,
    client: Client,
}

fn payload(seed: &str, len: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(len);
    for counter in 0_u64.. {
        let mut hash = Sha256::new();
        hash.update(seed.as_bytes());
        hash.update(counter.to_le_bytes());
        bytes.extend_from_slice(&hash.finalize());
        if bytes.len() >= len {
            bytes.truncate(len);
            break;
        }
    }
    bytes
}

impl Fixture {
    async fn request(
        &self,
        origin: &str,
        path: &str,
        form: Option<reqwest::multipart::Form>,
    ) -> Result<Vec<u8>> {
        let mut request = self
            .client
            .post(format!("{origin}{path}"))
            .header("Cache-Control", "no-cache, no-store");
        if let Some(form) = form {
            request = request.multipart(form);
        }
        let response = request.send().await?;
        let status = response.status();
        ensure!(response.content_length().unwrap_or(0) <= LIMIT as u64);
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            ensure!(
                body.len() + chunk.len() <= LIMIT,
                "fixture response exceeds budget"
            );
            body.extend_from_slice(&chunk);
        }
        self.owned.record(
            "bounded-fixture-http",
            &format!(
                "POST {origin}{path} status={status} bytes={} sha256={}",
                body.len(),
                hex::encode(Sha256::digest(&body))
            ),
        )?;
        ensure!(
            status.is_success(),
            "fixture POST {path} status={status}: {}",
            String::from_utf8_lossy(&body[..body.len().min(4096)])
        );
        Ok(body)
    }

    async fn source_add_with_chunker(
        &self,
        bytes: &[u8],
        name: &str,
        chunker: &str,
    ) -> Result<String> {
        // MFS nesting is explicit below. A slash in a multipart filename makes
        // Kubo add implicit directories and emits additional root receipts.
        let basename = name
            .rsplit('/')
            .next()
            .context("missing fixture basename")?;
        ensure!(!basename.is_empty());
        let form = reqwest::multipart::Form::new().part(
            "file",
            reqwest::multipart::Part::bytes(bytes.to_vec()).file_name(basename.to_owned()),
        );
        let raw = self.request(self.owned.source_rpc(), &format!("/api/v0/add?pin=false&cid-version=1&raw-leaves=true&chunker={chunker}&progress=false"), Some(form)).await?;
        self.owned
            .record("source-add-wire", std::str::from_utf8(&raw)?)?;
        let records: Vec<Value> = std::str::from_utf8(&raw)?
            .lines()
            .map(serde_json::from_str)
            .collect::<std::result::Result<_, _>>()?;
        ensure!(records.len() == 1, "source file add must have one root");
        car::canonical(
            records[0]["Hash"]
                .as_str()
                .context("source add missing CID")?,
        )
    }

    async fn source_filename_reproduction(&self) -> Result<()> {
        let bytes = payload(&format!("{}-filename-repro", self.owned.run_id()), 98_321);
        let name = "nested/receipt-probe.bin";
        let form = reqwest::multipart::Form::new().part(
            "file",
            reqwest::multipart::Part::bytes(bytes.clone()).file_name(name),
        );
        let raw = self.request(self.owned.source_rpc(), "/api/v0/add?pin=false&cid-version=1&raw-leaves=true&chunker=rabin-16384-32768-65536&progress=false", Some(form)).await?;
        self.owned.record(
            "source-filename-reproduction-wire",
            std::str::from_utf8(&raw)?,
        )?;
        let records: Vec<Value> = std::str::from_utf8(&raw)?
            .lines()
            .map(serde_json::from_str)
            .collect::<std::result::Result<_, _>>()?;
        ensure!(
            records.len() == 2,
            "original nested-filename regression not reproduced"
        );
        let leaf = records
            .iter()
            .find(|r| r["Name"] == name)
            .context("missing actual nested file receipt")?;
        let root = self.source_add(&bytes, name).await?;
        ensure!(
            root == car::canonical(leaf["Hash"].as_str().context("nested leaf CID")?)?,
            "basename normalization changed file DAG"
        );
        self.owned.record("source-filename-regression", "original slash filename=two actual receipts; basename filename=one actual receipt; identical file root; explicit MFS nesting retained")?;
        Ok(())
    }

    async fn source_add(&self, bytes: &[u8], name: &str) -> Result<String> {
        self.source_add_with_chunker(bytes, name, "rabin-16384-32768-65536")
            .await
    }

    async fn export(&self, rpc: &str, cid: &str) -> Result<BTreeMap<String, Vec<u8>>> {
        let raw = self
            .request(
                rpc,
                &format!("/api/v0/dag/export?arg={cid}&offline=true&progress=false"),
                None,
            )
            .await?;
        car::blocks(&raw, cid)
    }

    async fn source_fixtures(&self, protocol: ClusterProtocol) -> Result<Vec<Dag>> {
        let label = if protocol == ClusterProtocol::Proxy {
            "proxy"
        } else {
            "rest"
        };
        let bytes = payload(&format!("{}-{label}-file", self.owned.run_id()), 393_217);
        let cid = self.source_add(&bytes, "arbitrary-chunker.bin").await?;
        let blocks = self.export(self.owned.source_rpc(), &cid).await?;
        ensure!(blocks.len() >= 3, "file fixture is not multi-block");
        let file = Dag {
            name: format!("{label}-file"),
            cid,
            blocks,
            leaves: vec![Leaf {
                path: String::new(),
                bytes,
                plaintext: None,
            }],
            protocol,
            strategy: ClusterStrategy::Car,
        };

        let root = format!("/qa-{}-{label}", self.owned.run_id());
        self.request(
            self.owned.source_rpc(),
            &format!("/api/v0/files/mkdir?arg={root}/nested&parents=true"),
            None,
        )
        .await?;
        let mut leaves = Vec::new();
        for (path, len) in [("left.bin", 98_321), ("nested/right.bin", 147_457)] {
            let bytes = payload(&format!("{}-{label}-{path}", self.owned.run_id()), len);
            let leaf = self.source_add(&bytes, path).await?;
            // MFS mutation is allowed only on this fresh, isolated source fixture.
            self.request(
                self.owned.source_rpc(),
                &format!("/api/v0/files/cp?arg=/ipfs/{leaf}&arg={root}/{path}"),
                None,
            )
            .await?;
            leaves.push(Leaf {
                path: path.into(),
                bytes,
                plaintext: None,
            });
        }
        let stat = self
            .owned
            .json(
                Method::POST,
                self.owned.source_rpc(),
                &format!("/api/v0/files/stat?arg={root}"),
            )
            .await?;
        ensure!(stat["Type"] == "directory");
        let cid = car::canonical(
            stat["Hash"]
                .as_str()
                .context("source directory missing CID")?,
        )?;
        let blocks = self.export(self.owned.source_rpc(), &cid).await?;
        ensure!(blocks.len() >= 7, "nested two-leaf directory DAG too small");
        let mut dags = vec![
            file,
            Dag {
                name: format!("{label}-directory"),
                cid,
                blocks,
                leaves,
                protocol,
                strategy: ClusterStrategy::Car,
            },
        ];
        // Exact default UnixFS parameters permit upload to retain the root. Also
        // transfer genuine product AES-GCM stored bytes, not a named byte sentinel.
        for (strategy, encrypted) in [
            (ClusterStrategy::Upload, false),
            (ClusterStrategy::Upload, true),
            (ClusterStrategy::Car, true),
        ] {
            let name = format!(
                "{label}-{strategy:?}-{}",
                if encrypted { "stored-crypto" } else { "plain" }
            );
            let plaintext = payload(&format!("{}-{name}", self.owned.run_id()), 393_217);
            let bytes = if encrypted {
                stored_crypto::encrypt(&plaintext).await?
            } else {
                plaintext.clone()
            };
            if encrypted {
                ensure!(bytes != plaintext && stored_crypto::decrypt(&bytes).await? == plaintext);
            }
            let chunker = if strategy == ClusterStrategy::Car {
                "rabin-16384-32768-65536"
            } else {
                "size-262144"
            };
            let cid = self.source_add_with_chunker(&bytes, &name, chunker).await?;
            let blocks = self.export(self.owned.source_rpc(), &cid).await?;
            ensure!(blocks.len() >= 3);
            dags.push(Dag {
                name,
                cid,
                blocks,
                leaves: vec![Leaf {
                    path: String::new(),
                    bytes,
                    plaintext: encrypted.then_some(plaintext),
                }],
                protocol,
                strategy,
            });
        }
        Ok(dags)
    }

    async fn assert_absent(&self, dag: &Dag) -> Result<()> {
        for rpc in [self.owned.rpc_a(), self.owned.rpc_b()] {
            self.owned.assert_no_swarm(rpc).await?;
            let raw = self.request(rpc, "/api/v0/refs/local", None).await?;
            let mut local = BTreeSet::new();
            for line in std::str::from_utf8(&raw)?.lines().filter(|s| !s.is_empty()) {
                let record: Value = serde_json::from_str(line)?;
                ensure!(record["Err"].as_str().unwrap_or("").is_empty());
                local.insert(car::canonical(
                    record["Ref"].as_str().context("local inventory CID")?,
                )?);
            }
            ensure!(
                dag.blocks.keys().all(|cid| !local.contains(cid)),
                "target already has a fixture block"
            );
            self.owned.record(
                "target-full-dag-absent",
                &format!(
                    "{} {} origin={rpc} blocks={} all_absent=true",
                    dag.name,
                    dag.cid,
                    dag.blocks.len()
                ),
            )?;
        }
        Ok(())
    }

    fn provider(
        &self,
        protocol: ClusterProtocol,
        strategy: ClusterStrategy,
    ) -> Result<ClusterProvider> {
        let options = ClusterProviderOptions {
            protocol,
            strategy,
            source: Some(ClusterSource {
                endpoint: self.owned.source_rpc().into(),
                auth: None,
            }),
            proxy_endpoint: (protocol == ClusterProtocol::Proxy)
                .then(|| self.owned.proxy_a().into()),
            // Independent explicit auth slots remain None on isolated test nodes.
            proxy_auth: None,
            rest_auth: None,
            rest_endpoint: if protocol == ClusterProtocol::Proxy {
                self.owned.rest_a()
            } else {
                self.owned.rest_b()
            }
            .into(),
            pinset: ClusterPinset::new("cluster-real", self.owned.run_id())?,
            min_replicas: 2,
            allow_private_network: true,
            stream_channels: true,
            ..ClusterProviderOptions::default()
        };
        Ok(ClusterProvider::new(options)?)
    }

    async fn mismatch_no_fallback(&self, protocol: ClusterProtocol) -> Result<(Dag, Dag)> {
        let name = format!("{protocol:?}-upload-mismatch");
        let bytes = payload(&format!("{}-{name}", self.owned.run_id()), 393_217);
        let cid = self.source_add(&bytes, &name).await?;
        let dag = Dag {
            name,
            blocks: self.export(self.owned.source_rpc(), &cid).await?,
            cid,
            leaves: vec![Leaf {
                path: String::new(),
                bytes,
                plaintext: None,
            }],
            protocol,
            strategy: ClusterStrategy::Upload,
        };
        self.assert_absent(&dag).await?;
        // The explicit source only-hash preview independently determines the
        // target's default-import root, without writing/preloading either target.
        let form = reqwest::multipart::Form::new().part(
            "file",
            reqwest::multipart::Part::bytes(dag.leaves[0].bytes.clone()).file_name("content"),
        );
        let preview = self.request(self.owned.source_rpc(), "/api/v0/add?only-hash=true&pin=false&cid-version=1&raw-leaves=true&chunker=size-262144&progress=false", Some(form)).await?;
        let records: Vec<Value> = std::str::from_utf8(&preview)?
            .lines()
            .map(serde_json::from_str)
            .collect::<std::result::Result<_, _>>()?;
        ensure!(records.len() == 1);
        let expected_other = car::canonical(
            records[0]["Hash"]
                .as_str()
                .context("preview root missing")?,
        )?;
        ensure!(
            expected_other != dag.cid,
            "mismatch fixture did not produce a different DAG"
        );
        let out = self
            .provider(protocol, ClusterStrategy::Upload)?
            .submit_observed(submit(&dag.cid))
            .await;
        self.owned.record(
            "upload-mismatch-no-fallback",
            &format!(
                "case={} requested={} expected_other={expected_other} result={out:?}",
                dag.name, dag.cid
            ),
        )?;
        ensure!(out.result.as_ref().err().map(|e| e.code) == Some(ClusterFailureCode::CidMismatch));
        ensure!(out.effect == ClusterWriteEffect::Unknown && out.replicas.is_none());
        ensure!(
            out.resources
                .iter()
                .all(|r| r.ownership == Ownership::Unknown)
        );
        ensure!(out.resources.iter().any(|r| r.resource.cid == dag.cid));
        ensure!(
            out.resources
                .iter()
                .any(|r| r.resource.cid == expected_other
                    && r.status == ClusterResourceStatus::PinAccepted)
        );
        self.assert_original_root_absent(&dag).await?;
        // Audit the real unexpected DAG too, without making the requested job a
        // success. This source-only default import is AFTER the actual provider
        // dispatch; it neither supplies nor preloads a target.
        let cid = self
            .source_add_with_chunker(
                &dag.leaves[0].bytes,
                "unexpected-root-reference",
                "size-262144",
            )
            .await?;
        ensure!(cid == expected_other);
        let unexpected = Dag {
            name: format!("{}-unexpected", dag.name),
            blocks: self.export(self.owned.source_rpc(), &cid).await?,
            cid,
            leaves: vec![Leaf {
                path: String::new(),
                bytes: dag.leaves[0].bytes.clone(),
                plaintext: None,
            }],
            protocol,
            strategy: ClusterStrategy::Upload,
        };
        self.fresh_complete(
            &self.provider(protocol, ClusterStrategy::Cid)?,
            &unexpected,
            "unexpected-cid-audit-only",
        )
        .await?;
        Ok((dag, unexpected))
    }

    async fn assert_original_root_absent(&self, dag: &Dag) -> Result<()> {
        // Shared leaves can legitimately arrive from the byte upload. The
        // original root MUST NOT arrive: a CID/CAR fallback would expose it.
        for rpc in [self.owned.rpc_a(), self.owned.rpc_b()] {
            let raw = self.request(rpc, "/api/v0/refs/local", None).await?;
            for line in std::str::from_utf8(&raw)?.lines().filter(|s| !s.is_empty()) {
                let local: Value = serde_json::from_str(line)?;
                ensure!(local["Err"].as_str().unwrap_or("").is_empty());
                ensure!(
                    car::canonical(local["Ref"].as_str().context("local CID")?)? != dag.cid,
                    "original root arrived through forbidden fallback"
                );
            }
        }
        for rest in [self.owned.rest_a(), self.owned.rest_b()] {
            self.owned.assert_allocation_absent(rest, &dag.cid).await?;
        }
        Ok(())
    }

    async fn cid_source_only(&self, protocol: ClusterProtocol) -> Result<Dag> {
        let name = format!("{protocol:?}-cid-source-only");
        let bytes = payload(&format!("{}-{name}", self.owned.run_id()), 98_321);
        let cid = self.source_add(&bytes, &name).await?;
        let dag = Dag {
            name,
            blocks: self.export(self.owned.source_rpc(), &cid).await?,
            cid,
            leaves: vec![Leaf {
                path: String::new(),
                bytes,
                plaintext: None,
            }],
            protocol,
            strategy: ClusterStrategy::Cid,
        };
        self.assert_absent(&dag).await?;
        let provider = self.provider(protocol, ClusterStrategy::Cid)?;
        let out = provider.submit_observed(submit(&dag.cid)).await;
        self.owned.record(
            "cid-source-only-no-fallback",
            &format!("case={} result={out:?}", dag.name),
        )?;
        ensure!(
            out.result.is_ok() && out.effect == ClusterWriteEffect::Observed,
            "CID registration failed: {out:?}"
        );
        ensure!(out.replication_outcome() != ReplicaOutcome::Complete);
        ensure!(
            out.resources
                .iter()
                .all(|r| r.ownership == Ownership::Unknown && r.strategy == ClusterStrategy::Cid)
        );
        ensure!(provider.observe(&dag.cid).await.outcome() != ReplicaOutcome::Complete);
        self.assert_absent(&dag).await?;
        Ok(dag)
    }

    async fn fresh_complete(
        &self,
        provider: &ClusterProvider,
        dag: &Dag,
        phase: &str,
    ) -> Result<()> {
        self.owned
            .wait_for_pinned(self.owned.rest_a(), &dag.cid, 2)
            .await?;
        self.owned
            .wait_for_pinned(self.owned.rest_b(), &dag.cid, 2)
            .await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
        loop {
            let proof = provider.observe(&dag.cid).await;
            self.owned
                .record(phase, &format!("{} {} fresh={proof:?}", dag.name, dag.cid))?;
            if proof.outcome() == ReplicaOutcome::Complete {
                let evidence = proof.evidence().context("Complete without evidence")?;
                ensure!(
                    evidence.required_replicas == 2
                        && evidence.pinned_peers == self.owned.expected_peers()
                );
                ensure!(evidence.allocation.peers == self.owned.expected_peers());
                ensure!(
                    evidence.allocation.replication_factor_min == 2
                        && evidence.allocation.replication_factor_max == 2
                );
                println!(
                    "{phase} case={} cid={} REST=2/2 fresh=Complete allocated_peers={:?}",
                    dag.name, dag.cid, evidence.pinned_peers
                );
                return Ok(());
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "fresh provider proof never reached actual 2/2"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn verify_offline(&self, dag: &Dag) -> Result<()> {
        for (node, rpc) in [("A", self.owned.rpc_a()), ("B", self.owned.rpc_b())] {
            self.owned.assert_no_swarm(rpc).await?;
            let exported = self.export(rpc, &dag.cid).await?;
            ensure!(
                exported == dag.blocks,
                "target {node} full DAG differs from source"
            );
            // Every exported hash-verified block is also read locally, offline.
            for (cid, expected) in &dag.blocks {
                let bytes = self
                    .request(
                        rpc,
                        &format!("/api/v0/block/get?arg={cid}&offline=true"),
                        None,
                    )
                    .await?;
                ensure!(&bytes == expected, "target {node} offline block differs");
            }
            let mut byte_count = 0;
            for leaf in &dag.leaves {
                let path = if leaf.path.is_empty() {
                    dag.cid.clone()
                } else {
                    format!("{}/{}", dag.cid, leaf.path)
                };
                let bytes = self
                    .request(rpc, &format!("/api/v0/cat?arg={path}&offline=true"), None)
                    .await?;
                ensure!(
                    bytes == leaf.bytes,
                    "target {node} original leaf bytes differ"
                );
                if let Some(plaintext) = &leaf.plaintext {
                    ensure!(
                        &bytes != plaintext && stored_crypto::decrypt(&bytes).await? == *plaintext,
                        "stored crypto bytes were transformed or cannot decrypt"
                    );
                    self.owned.record("offline-stored-crypto", &format!("case={} node={node} ciphertext_bytes={} decrypted_original=true source_stopped=true", dag.name, bytes.len()))?;
                }
                byte_count += bytes.len();
                self.owned.record("offline-original-bytes", &json!({"case":dag.name,"node":node,"path":leaf.path,"bytes":bytes.len(),"sha256":hex::encode(Sha256::digest(&bytes)),"source_stopped":true}).to_string())?;
            }
            let pins = self
                .owned
                .json(
                    Method::POST,
                    rpc,
                    &format!("/api/v0/pin/ls?arg={}&type=recursive", dag.cid),
                )
                .await?;
            ensure!(pins["Keys"][&dag.cid]["Type"] == "recursive");
            self.owned.record("offline-full-dag", &json!({"case":dag.name,"cid":dag.cid,"node":node,"blocks":exported.len(),"block_cids":exported.keys().collect::<Vec<_>>(),"original_bytes":byte_count,"all_blocks_sha256_verified":true,"source_stopped":true}).to_string())?;
            println!(
                "offline_full_dag case={} target={node} cid={} hashverified_blocks={} original_bytes={byte_count} source_stopped=true",
                dag.name,
                dag.cid,
                exported.len()
            );
        }
        Ok(())
    }
}

fn submit(cid: &str) -> SubmitPin {
    SubmitPin {
        cid: cid.into(),
        name: "fixture-private-name".into(),
        metadata: BTreeMap::new(),
    }
}

pub async fn acceptance() -> Result<()> {
    let owned = RealClusterFixture::from_provider_environment().await?;
    let fixture = Fixture {
        owned,
        client: Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(30))
            .build()?,
    };
    fixture.owned.validate_nodes().await?;
    fixture.owned.assert_source_offline().await?;
    let source = fixture.owned.source_rpc();
    ensure!(
        !fixture
            .owned
            .kubo_ids()
            .contains(&fixture.owned.source_id()),
        "source reuses an allocated identity"
    );
    ensure!(
        fixture
            .owned
            .json(Method::POST, source, "/api/v0/id")
            .await?["ID"]
            == fixture.owned.source_id()
    );
    ensure!(
        fixture
            .owned
            .json(Method::POST, source, "/api/v0/version")
            .await?["Version"]
            == "0.43.0"
    );
    let config = fixture
        .owned
        .json(Method::POST, source, "/api/v0/config/show")
        .await?;
    ensure!(config["Addresses"]["Swarm"] == json!([]) && config["Bootstrap"] == json!([]));
    ensure!(
        config["AutoConf"]["Enabled"] == false && config["Discovery"]["MDNS"]["Enabled"] == false
    );
    ensure!(
        config["Routing"]["Type"] == "none"
            && config["Plugins"]["Plugins"]["telemetry"]["Disabled"] == true
    );
    fixture.owned.record(
        "independent-source",
        &format!(
            "source={} allocated={:?} source_mode=offline Kubo=0.43.0 Cluster=1.1.6 RF=2",
            fixture.owned.source_id(),
            fixture.owned.kubo_ids()
        ),
    )?;
    fixture.source_filename_reproduction().await?;
    let mut dags = fixture.source_fixtures(ClusterProtocol::Proxy).await?;
    dags.extend(fixture.source_fixtures(ClusterProtocol::Rest).await?);
    ensure!(dags.iter().map(|d| &d.cid).collect::<BTreeSet<_>>().len() == 10);
    // All original DAGs must be absent on both targets before ANY dispatch.
    for dag in &dags {
        fixture.assert_absent(dag).await?;
    }
    for dag in &dags {
        let provider = fixture.provider(dag.protocol, dag.strategy)?;
        let observation = provider.submit_observed(submit(&dag.cid)).await;
        fixture.owned.record(
            "provider-submit",
            &format!(
                "{} protocol={:?} strategy={:?} result={observation:?}",
                dag.name, dag.protocol, dag.strategy
            ),
        )?;
        ensure!(
            observation.result.is_ok(),
            "real provider write failed: {observation:?}"
        );
        ensure!(observation.effect == ClusterWriteEffect::Observed);
        ensure!(
            !observation.resources.is_empty()
                && observation
                    .resources
                    .iter()
                    .all(|r| r.ownership == Ownership::Unknown)
        );
        ensure!(
            observation.result.as_ref().unwrap().resource.cid == dag.cid,
            "original CID changed"
        );
        if observation.replication_outcome() == ReplicaOutcome::Complete {
            ensure!(
                observation
                    .replicas
                    .as_ref()
                    .context("missing submit replicas")?
                    .evidence()
                    .context("missing submit evidence")?
                    .pinned_peers
                    == fixture.owned.expected_peers()
            );
        }
        fixture
            .fresh_complete(&provider, dag, "after-upload")
            .await?;
    }
    let mut source_only = Vec::new();
    let mut mismatched_originals = Vec::new();
    for protocol in [ClusterProtocol::Proxy, ClusterProtocol::Rest] {
        let (original, unexpected) = fixture.mismatch_no_fallback(protocol).await?;
        mismatched_originals.push(original);
        dags.push(unexpected);
        source_only.push(fixture.cid_source_only(protocol).await?);
    }
    fixture.owned.stop_owned_source().await?;
    for dag in &mismatched_originals {
        fixture.assert_original_root_absent(dag).await?;
    }
    for dag in &source_only {
        fixture.assert_absent(dag).await?;
        ensure!(
            fixture
                .provider(dag.protocol, ClusterStrategy::Cid)?
                .observe(&dag.cid)
                .await
                .outcome()
                != ReplicaOutcome::Complete
        );
    }
    for dag in &dags {
        fixture.verify_offline(dag).await?;
        let provider = fixture.provider(dag.protocol, ClusterStrategy::Cid)?;
        // CID-only cannot copy source-only content with these intentionally empty
        // content swarms. Positive CID is therefore a re-pin of the DAG naturally
        // delivered by the provider (never target-preseeded), after source stop.
        let out = provider.submit_observed(submit(&dag.cid)).await;
        ensure!(
            out.result.is_ok() && out.effect == ClusterWriteEffect::Observed,
            "CID-only re-pin failed: {out:?}"
        );
        ensure!(
            out.resources
                .iter()
                .all(|r| r.ownership == Ownership::Unknown && r.strategy == ClusterStrategy::Cid)
        );
        fixture.owned.record(
            "cid-only-after-source-stop",
            &format!(
                "case={} protocol={:?} result={out:?}",
                dag.name, dag.protocol
            ),
        )?;
        fixture
            .fresh_complete(&provider, dag, "after-source-stop")
            .await?;
    }
    fixture.owned.record("acceptance-scope", "PASS: leaf Proxy+REST CAR/upload/stored-crypto/mismatch-no-fallback; cid-only re-pin after independent source stop; offline complete DAG/bytes and fresh REST 2/2; all ownership Unknown. NOT full Stage6/worker/S3/auth/account/continuous availability; no pin/rm, unpin or GC")?;
    Ok(())
}
