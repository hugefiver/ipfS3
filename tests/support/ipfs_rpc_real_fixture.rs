//! Own-container validation and bounded real RPC fixture I/O.
use anyhow::{Context, Result, ensure};
use reqwest::{Client, Response, Url, multipart};
use serde::Deserialize;
use serde_json::Value;
use std::{process::Command, time::Duration};

pub const LABEL: &str = "ipfs_s3.rpc_real.run";
pub const IMAGE: &str = "ipfs/kubo:v0.43.0";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fixture {
    pub run_id: String,
    pub network: String,
    pub source: String,
    pub target: String,
    pub source_volume: String,
    pub target_volume: String,
    pub host_forwarding: String,
    pub source_url: String,
    pub target_url: String,
}

pub fn docker(args: &[&str]) -> Result<String> {
    let output = Command::new("docker").args(args).output()?;
    ensure!(
        output.status.success(),
        "docker inventory/operation failed: {:?}: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

fn inspect(kind: &str, name: &str) -> Result<Value> {
    let output = docker(&[kind, "inspect", name])?;
    let mut items: Vec<Value> = serde_json::from_str(&output)?;
    ensure!(
        items.len() == 1,
        "inventory must identify exactly one resource"
    );
    Ok(items.remove(0))
}

impl Fixture {
    pub fn load() -> Result<Self> {
        let path = std::env::var("IPFS_S3_REAL_RPC_FIXTURE")
            .context("IPFS_S3_REAL_RPC_FIXTURE is required; use the own-resource runner")?;
        let fixture: Self = serde_json::from_slice(&std::fs::read(path)?)?;
        ensure!(
            fixture.run_id.len() == 32 && fixture.run_id.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid fixture run id"
        );
        let prefix = format!("ipfs-s3-rpc-{}", fixture.run_id);
        for (actual, suffix) in [
            (&fixture.network, "net"),
            (&fixture.source, "source"),
            (&fixture.target, "target"),
            (&fixture.source_volume, "source-data"),
            (&fixture.target_volume, "target-data"),
        ] {
            ensure!(
                *actual == format!("{prefix}-{suffix}"),
                "not an exact owned resource name"
            );
        }
        Ok(fixture)
    }

    pub fn verify(&self, source_url: &str, target_url: &str) -> Result<()> {
        ensure!(
            self.host_forwarding == "loopback-docker-exec-nc"
                && self.source_url == source_url
                && self.target_url == target_url,
            "URLs must match the owned loopback exec relay manifest"
        );
        let network = inspect("network", &self.network)?;
        ensure!(
            network["Internal"] == true && network["Labels"][LABEL] == self.run_id,
            "fixture network must be internal and labeled for this run"
        );
        let mut ids = Vec::new();
        for (name, volume, url) in [
            (&self.source, &self.source_volume, source_url),
            (&self.target, &self.target_volume, target_url),
        ] {
            let container = inspect("container", name)?;
            ensure!(
                container["Config"]["Labels"][LABEL] == self.run_id,
                "container label mismatch"
            );
            ensure!(
                container["Config"]["Image"] == IMAGE,
                "fixture image mismatch"
            );
            ensure!(
                container["State"]["Running"] == true,
                "fixture node is not running"
            );
            let networks = container["NetworkSettings"]["Networks"]
                .as_object()
                .context("networks")?;
            ensure!(
                networks.len() == 1 && networks.contains_key(&self.network),
                "unexpected node network"
            );
            let processes = docker(&["top", name, "-eo", "pid,args"])?;
            ensure!(
                processes
                    .lines()
                    .any(|line| line.split_whitespace().skip(1).eq([
                        "ipfs",
                        "daemon",
                        "--offline"
                    ])),
                "daemon must be explicitly offline"
            );
            let mounts = container["Mounts"].as_array().context("mount inventory")?;
            ensure!(
                mounts.iter().any(|m| m["Type"] == "volume"
                    && m["Name"] == *volume
                    && m["Destination"] == "/data/ipfs"),
                "wrong repository volume"
            );
            let ports = container["NetworkSettings"]["Ports"]
                .as_object()
                .context("RPC ports")?;
            ensure!(
                ports
                    .values()
                    .all(|value| value.is_null() || value.as_array().is_some_and(Vec::is_empty)),
                "internal nodes must have no effective published ports"
            );
            let parsed = Url::parse(url)?;
            ensure!(
                parsed.scheme() == "http"
                    && parsed.host_str() == Some("127.0.0.1")
                    && parsed.port().is_some_and(|port| port > 0)
                    && parsed.username().is_empty()
                    && parsed.password().is_none()
                    && parsed.query().is_none()
                    && parsed.fragment().is_none(),
                "exec RPC relay must be loopback-only with a legal OS-assigned port"
            );
            let volume = inspect("volume", volume)?;
            ensure!(
                volume["Labels"][LABEL] == self.run_id,
                "repository volume label mismatch"
            );
            ids.push(container["Id"].as_str().context("container id")?.to_owned());
        }
        ensure!(
            ids[0] != ids[1]
                && source_url != target_url
                && self.source_volume != self.target_volume,
            "source and target must have independent containers, URLs and repositories"
        );
        println!(
            "fixture run={} source={} target={} independent_volumes={}/{} internal_network={} host_forwarding=loopback-docker-exec-nc",
            self.run_id, ids[0], ids[1], self.source_volume, self.target_volume, self.network
        );
        Ok(())
    }

    pub fn stop_source(&self) -> Result<()> {
        // Recheck immediately before the only destructive node operation. No
        // external/preexisting environment is accepted by this fixture.
        let source = inspect("container", &self.source)?;
        ensure!(
            source["Config"]["Labels"][LABEL] == self.run_id,
            "source no longer owned"
        );
        docker(&["stop", "--time", "10", &self.source])?;
        let source = inspect("container", &self.source)?;
        ensure!(
            source["State"]["Running"] == false,
            "source was not isolated"
        );
        println!("source_isolated={} running=false", self.source);
        Ok(())
    }
}

pub struct Rpc {
    client: Client,
    pub base: String,
}

impl Rpc {
    pub fn new(base: String) -> Result<Self> {
        Ok(Self {
            base,
            client: Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(90))
                .build()?,
        })
    }

    fn url(&self, command: &str, query: &[(&str, &str)]) -> Result<Url> {
        let mut url = Url::parse(&format!("{}/api/v0/{command}", self.base))?;
        url.query_pairs_mut().extend_pairs(query.iter().copied());
        Ok(url)
    }

    pub async fn post(&self, command: &str, query: &[(&str, &str)]) -> Result<Response> {
        Ok(self
            .client
            .post(self.url(command, query)?)
            .send()
            .await?
            .error_for_status()?)
    }

    pub async fn json(&self, command: &str, query: &[(&str, &str)]) -> Result<Value> {
        Ok(self.post(command, query).await?.json().await?)
    }

    pub async fn bytes(&self, command: &str, query: &[(&str, &str)]) -> Result<Vec<u8>> {
        let mut response = self.post(command, query).await?;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(
                bytes.len() + chunk.len() <= 16 * 1024 * 1024,
                "fixture response exceeds budget"
            );
            bytes.extend_from_slice(&chunk);
        }
        ensure!(
            !response.headers().contains_key("X-Stream-Error"),
            "RPC stream error"
        );
        Ok(bytes)
    }

    pub async fn add(&self, bytes: &[u8], chunker: &str) -> Result<String> {
        let form = multipart::Form::new().part(
            "file",
            multipart::Part::bytes(bytes.to_vec()).file_name("fixture.bin"),
        );
        let text = self
            .client
            .post(self.url(
                "add",
                &[
                    ("cid-version", "1"),
                    ("raw-leaves", "true"),
                    ("hash", "sha2-256"),
                    ("chunker", chunker),
                    ("pin", "true"),
                    ("progress", "false"),
                    ("wrap-with-directory", "false"),
                ],
            )?)
            .multipart(form)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        let records: Vec<Value> = text
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<_, _>>()?;
        ensure!(records.len() == 1, "source add must produce one root");
        Ok(records[0]["Hash"]
            .as_str()
            .context("source CID")?
            .to_owned())
    }

    pub async fn export(&self, cid: &str) -> Result<Vec<u8>> {
        self.bytes(
            "dag/export",
            &[("arg", cid), ("offline", "true"), ("progress", "false")],
        )
        .await
    }

    /// Diagnostic replay only: same streamed bytes/add options, pin=false. It
    /// cannot turn a failed provider observation into an acceptance success.
    pub async fn diagnose_upload(&self, source: &Self, cid: &str) -> Result<String> {
        let response = source.post("cat", &[("arg", cid)]).await?;
        let body = reqwest::Body::wrap_stream(response.bytes_stream());
        let form =
            multipart::Form::new().part("file", multipart::Part::stream(body).file_name("object"));
        let mut response = self
            .client
            .post(self.url(
                "add",
                &[
                    ("cid-version", "1"),
                    ("wrap-with-directory", "false"),
                    ("raw-leaves", "true"),
                    ("chunker", "size-262144"),
                    ("hash", "sha2-256"),
                    ("pin", "false"),
                    ("progress", "true"),
                ],
            )?)
            .multipart(form)
            .send()
            .await?
            .error_for_status()?;
        let mut records = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(
                records.len() + chunk.len() <= 64 * 1024,
                "diagnostic add response exceeds budget"
            );
            records.extend_from_slice(&chunk);
        }
        Ok(String::from_utf8(records)?)
    }

    /// Explicit fixture preseed, never credited as a CID-strategy byte transfer.
    pub async fn preseed(&self, car: &[u8], root: &str, pin: bool) -> Result<()> {
        let blocks = super::car::blocks(car, root)?;
        let block_bytes = blocks.values().map(Vec::len).sum();
        let raw = super::import::exchange(&self.base, car, pin).await?;
        super::import::validate(&raw, root, pin, blocks.len(), block_bytes)?;
        // Stats are not root identity/completeness proof. Independently read the
        // complete target DAG locally and compare its header AND every block.
        let target_car = self.export(root).await?;
        ensure!(
            super::car::blocks(&target_car, root)? == blocks,
            "preseed target CAR differs from complete source CAR"
        );
        let path = format!("/ipfs/{root}");
        let stat = self
            .json(
                "files/stat",
                &[("arg", &path), ("with-local", "true"), ("offline", "true")],
            )
            .await?;
        ensure!(
            stat["Local"] == true
                && stat["WithLocality"] == true
                && super::car::canonical(stat["Hash"].as_str().context("preseed stat CID")?)?
                    == super::car::canonical(root)?,
            "preseed DAG is not complete local data: {stat}"
        );
        println!(
            "target_preseed=true pin_roots={pin} cid={} single_root_car=true complete_blocks={} block_bytes={block_bytes} complete_local=true offline=true not_provider_byte_transfer=true",
            super::car::canonical(root)?,
            blocks.len()
        );
        Ok(())
    }
}
