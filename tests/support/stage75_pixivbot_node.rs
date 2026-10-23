//! Bounded loopback Kubo model: CIDs derive from uploaded bytes and DAG links.
//! It models the protocol, not a deployed Kubo or a paid provider account.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use base64::{Engine, engine::general_purpose::STANDARD};
use sha2::{Digest, Sha256};
use wiremock::{Mock, MockServer, Request, ResponseTemplate, matchers::method};

#[derive(Default)]
struct Blocks {
    files: BTreeMap<String, Vec<u8>>,
    directories: BTreeMap<String, BTreeMap<String, String>>,
    sizes: BTreeMap<String, u64>,
    pins: BTreeSet<String>,
}

impl Blocks {
    fn resolve(&self, path: &str) -> String {
        let mut parts = path.strip_prefix("/ipfs/").unwrap_or(path).split('/');
        let mut cid = parts.next().unwrap().to_owned();
        for name in parts {
            cid = self.directories.get(&cid).expect("existing directory")[name].clone();
        }
        assert!(
            self.sizes.contains_key(&cid),
            "path must resolve to an uploaded block"
        );
        cid
    }

    fn directory(&mut self, payload: &[u8]) -> String {
        let json: serde_json::Value = serde_json::from_slice(payload).unwrap();
        let data = STANDARD
            .decode(json["Data"]["/"]["bytes"].as_str().unwrap())
            .unwrap();
        assert_eq!(data, [8, 1], "small UnixFS directory, not HAMT/file");
        let mut block = Vec::new();
        let mut links = BTreeMap::new();
        let mut cumulative = 0;
        // Canonical DAG-PB encodes sorted Links before Data.
        for link in json["Links"].as_array().unwrap() {
            let name = link["Name"].as_str().unwrap();
            let cid = link["Hash"]["/"].as_str().unwrap();
            let size = link["Tsize"].as_u64().unwrap();
            assert_eq!(self.sizes[cid], size, "link refers to prior stored DAG");
            assert!(links.insert(name.to_owned(), cid.to_owned()).is_none());
            let mut encoded = Vec::new();
            field(
                &mut encoded,
                0x0a,
                &cid.parse::<cid::Cid>().unwrap().to_bytes(),
            );
            field(&mut encoded, 0x12, name.as_bytes());
            encoded.push(0x18);
            varint(&mut encoded, size);
            field(&mut block, 0x12, &encoded);
            cumulative += size;
        }
        assert!(!links.is_empty());
        field(&mut block, 0x0a, &data);
        let cid = content_cid(0x70, &block);
        self.sizes
            .insert(cid.clone(), cumulative + block.len() as u64);
        self.directories.insert(cid.clone(), links);
        self.pins.insert(cid.clone());
        cid
    }
}

fn varint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 128 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn field(output: &mut Vec<u8>, tag: u8, value: &[u8]) {
    output.push(tag);
    varint(output, value.len() as u64);
    output.extend(value);
}

pub fn content_cid(codec: u64, bytes: &[u8]) -> String {
    let hash = cid::multihash::Multihash::<64>::wrap(0x12, &Sha256::digest(bytes)).unwrap();
    cid::Cid::new_v1(codec, hash).to_string()
}

fn multipart(request: &Request, filename: &str) -> Vec<u8> {
    assert!(request.body.len() <= 9 * 1024 * 1024);
    let content_type = request.headers["content-type"].to_str().unwrap();
    let boundary = content_type
        .strip_prefix("multipart/form-data; boundary=")
        .unwrap();
    let start = format!("--{boundary}\r\n");
    let end = format!("\r\n--{boundary}--\r\n");
    assert!(request.body.starts_with(start.as_bytes()));
    assert!(request.body.ends_with(end.as_bytes()));
    let split = request
        .body
        .windows(4)
        .position(|part| part == b"\r\n\r\n")
        .unwrap();
    let headers = std::str::from_utf8(&request.body[start.len()..split]).unwrap();
    assert!(headers.contains(&format!("name=\"file\"; filename=\"{filename}\"")));
    request.body[split + 4..request.body.len() - end.len()].to_vec()
}

fn expect_query(actual: &BTreeMap<String, String>, expected: &[(&str, &str)]) {
    assert_eq!(
        *actual,
        expected
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    );
}

pub struct HttpNode {
    pub server: MockServer,
    blocks: Arc<Mutex<Blocks>>,
}

impl HttpNode {
    pub async fn start(target: bool) -> Self {
        let server = MockServer::start().await;
        let blocks = Arc::new(Mutex::new(Blocks::default()));
        Mock::given(method("POST")).respond_with({
            let blocks = blocks.clone();
            move |request: &Request| {
                assert!(!request.headers.contains_key("authorization"));
                assert!(!request.headers.keys().any(|name| name.as_str().starts_with("x-amz-")));
                let args: BTreeMap<String, String> = request.url.query_pairs().into_owned().collect();
                assert_eq!(args.len(), request.url.query_pairs().count());
                let mut blocks = blocks.lock().unwrap();
                let json = match request.url.path() {
                    "/api/v0/add" => {
                        let progress = args["progress"].as_str();
                        assert!(matches!(progress, "true" | "false"));
                        let mut expected = vec![("cid-version","1"),("pin","false"),("wrap-with-directory","false"),("progress",progress)];
                        if target { expected.extend([("raw-leaves","true"),("chunker","size-262144"),("hash","sha2-256")]); }
                        expect_query(&args, &expected);
                        assert_eq!(request.headers["transfer-encoding"], "chunked");
                        assert!(!request.headers.contains_key("content-length"));
                        let bytes = multipart(request, "object");
                        let cid = content_cid(0x55, &bytes);
                        blocks.sizes.insert(cid.clone(), bytes.len() as u64);
                        blocks.files.insert(cid.clone(), bytes.clone());
                        serde_json::json!({"Name":"object","Hash":cid,"Size":bytes.len().to_string()})
                    }
                    "/api/v0/cat" => {
                        assert!(!target, "worker must read stored source bytes");
                        expect_query(&args, &[("arg", &args["arg"])]);
                        let cid = blocks.resolve(&args["arg"]);
                        return ResponseTemplate::new(200).set_body_bytes(blocks.files[&cid].clone());
                    }
                    "/api/v0/dag/put" => {
                        assert!(!target);
                        expect_query(&args, &[("input-codec","dag-json"),("store-codec","dag-pb"),("hash","sha2-256"),("pin","true")]);
                        let cid = blocks.directory(&multipart(request, "directory.json"));
                        serde_json::json!({"Cid":{"/":cid}})
                    }
                    "/api/v0/resolve" => {
                        assert!(!target);
                        expect_query(&args, &[("arg", &args["arg"])]);
                        serde_json::json!({"Path":format!("/ipfs/{}", blocks.resolve(&args["arg"]))})
                    }
                    "/api/v0/pin/add" => {
                        let cid = blocks.resolve(&args["arg"]);
                        let mut expected = vec![("arg",args["arg"].as_str())];
                        if target { expected.extend([("recursive","true"),("progress","false")]); }
                        else if args.contains_key("recursive") { expected.push(("recursive","true")); }
                        expect_query(&args, &expected);
                        blocks.pins.insert(cid.clone());
                        serde_json::json!({"Pins":[cid]})
                    }
                    "/api/v0/pin/ls" => {
                        let cid = blocks.resolve(&args["arg"]);
                        if target { expect_query(&args, &[("arg", &cid),("stream","false"),("names","false"),("type","all"),("offline","true")]); }
                        else { expect_query(&args, &[("arg", &cid),("type","recursive"),("offline","true")]); }
                        assert!(blocks.pins.contains(&cid));
                        serde_json::json!({"Keys":{(cid):{"Type":"recursive"}}})
                    }
                    "/api/v0/files/stat" => {
                        let cid = blocks.resolve(&args["arg"]);
                        let mut expected = vec![("arg",args["arg"].as_str())];
                        if target || args.contains_key("with-local") {
                            expected.extend([("with-local","true"),("offline","true")]);
                            assert!(blocks.pins.contains(&cid));
                        } else if args.contains_key("offline") {
                            expected.push(("offline", "true"));
                        }
                        expect_query(&args, &expected);
                        let kind = if blocks.files.contains_key(&cid) { "file" } else { "directory" };
                        serde_json::json!({"Hash":cid,"Type":kind,"Size":blocks.sizes[&cid],"CumulativeSize":blocks.sizes[&cid],"WithLocality":true,"Local":true})
                    }
                    "/api/v0/id" => {
                        expect_query(&args, &[("peerid-base","b58mh")]);
                        serde_json::json!({"ID":"QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE"})
                    }
                    endpoint => panic!("unexpected RPC (cleanup is forbidden): {endpoint}"),
                };
                ResponseTemplate::new(200).set_body_string(format!("{json}\n"))
            }
        }).mount(&server).await;
        Self { server, blocks }
    }

    pub fn stored(&self, cid: &str) -> Vec<u8> {
        self.blocks.lock().unwrap().files[cid].clone()
    }

    pub async fn calls(&self) -> BTreeMap<String, usize> {
        self.server.received_requests().await.unwrap().iter().fold(
            BTreeMap::new(),
            |mut counts, request| {
                *counts.entry(request.url.path().to_owned()).or_default() += 1;
                counts
            },
        )
    }
}
