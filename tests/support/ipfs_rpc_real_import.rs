//! Bounded fixture-only CAR import, including actual HTTP EOF and trailers.
//! This is not the provider transport implementation.
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, client::conn::http1};
use hyper_util::rt::TokioIo;
use reqwest::Url;
use serde::Deserialize;
use std::time::Duration;
use tokio::net::TcpStream;

#[derive(Deserialize)]
enum Record {
    Root(Root),
    Stats(Stats),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Root {
    #[serde(rename = "Cid")]
    cid: RootCid,
    #[serde(rename = "PinErrorMsg")]
    pin_error: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RootCid {
    #[serde(rename = "/")]
    value: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Stats {
    #[serde(rename = "BlockCount")]
    blocks: usize,
    #[serde(rename = "BlockBytesCount")]
    bytes: usize,
}

pub fn validate(raw: &[u8], root: &str, pin: bool, blocks: usize, bytes: usize) -> Result<()> {
    ensure!(raw.len() <= 64 * 1024, "import response exceeds budget");
    ensure!(raw.ends_with(b"\n"), "import NDJSON lacks terminal newline");
    let records: Vec<Record> = std::str::from_utf8(raw)?
        .split_terminator('\n')
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    ensure!(
        records.len() == 1 + usize::from(pin),
        "unexpected import record count"
    );
    // Kubo 0.43.0 only emits Root when pinning; unpinned import emits Stats
    // alone. Root identity for BOTH paths is independently proven by the CAR.
    if pin {
        let Record::Root(record) = &records[0] else {
            anyhow::bail!("pinned import lacks initial Root");
        };
        ensure!(record.pin_error.is_empty(), "preseed root pin failed");
        ensure!(
            super::car::canonical(&record.cid.value)? == super::car::canonical(root)?,
            "preseed response root mismatch"
        );
    }
    let Some(Record::Stats(stats)) = records.last() else {
        anyhow::bail!("import lacks terminal Stats");
    };
    ensure!(
        stats.blocks == blocks && stats.bytes == bytes,
        "import stats do not match complete input CAR"
    );
    Ok(())
}

pub async fn exchange(base: &str, car: &[u8], pin: bool) -> Result<Vec<u8>> {
    ensure!(car.len() <= 16 * 1024 * 1024, "fixture CAR exceeds budget");
    let mut url = Url::parse(&format!("{base}/api/v0/dag/import"))?;
    ensure!(
        url.scheme() == "http" && url.host_str() == Some("127.0.0.1"),
        "fixture import requires an owned loopback relay"
    );
    url.query_pairs_mut().extend_pairs([
        ("pin-roots", if pin { "true" } else { "false" }),
        ("stats", "true"),
        ("fast-provide-root", "false"),
        ("fast-provide-dag", "false"),
        ("fast-provide-wait", "false"),
    ]);
    let boundary = format!("rpc-real-{}", uuid::Uuid::new_v4().simple());
    let mut body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"preseed.car\"\r\nContent-Type: application/vnd.ipld.car\r\n\r\n"
    )
    .into_bytes();
    body.extend_from_slice(car);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    let port = url.port().context("explicit relay port")?;
    let request = Request::post(&url[url::Position::BeforePath..])
        .header("Host", format!("127.0.0.1:{port}"))
        .header("Connection", "close")
        .header(
            "Content-Type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(Full::new(Bytes::from(body)))?;

    tokio::time::timeout(Duration::from_secs(90), async {
        let stream = TcpStream::connect(("127.0.0.1", port)).await?;
        let (mut sender, connection) = http1::handshake(TokioIo::new(stream)).await?;
        // Poll the connection alongside the whole bounded response, without a
        // detached task that could survive an error/timeout or owned cleanup.
        let (reply, connection) = tokio::join!(
            async move {
                let mut response = sender.send_request(request).await?;
                ensure!(response.status().is_success(), "import HTTP {}", response.status());
                ensure!(
                    !response.headers().contains_key("x-stream-error"),
                    "import response has an error header"
                );
                let mut raw = Vec::new();
                let mut body_frames = 0;
                let mut trailer_frames = 0;
                while let Some(frame) = response.body_mut().frame().await {
                    let frame = frame.context("import response did not reach clean EOF")?;
                    match frame.into_data() {
                        Ok(data) => {
                            ensure!(raw.len() + data.len() <= 64 * 1024, "import response exceeds budget");
                            raw.extend_from_slice(&data);
                            body_frames += 1;
                        }
                        Err(frame) => {
                            let trailers = frame.into_trailers().map_err(|_| anyhow::anyhow!("unexpected import frame"))?;
                            ensure!(trailers.is_empty(), "import has nonempty/error trailers: {trailers:?}");
                            trailer_frames += 1;
                        }
                    }
                }
                let text = std::str::from_utf8(&raw)?;
                println!(
                    "fixture_import pin_roots={pin} status={} body_frames={body_frames} trailer_frames={trailer_frames} clean_eof=true bytes={} raw_ndjson={text:?}",
                    response.status(), raw.len()
                );
                Ok::<_, anyhow::Error>(raw)
            },
            connection
        );
        connection.context("import HTTP connection failed")?;
        reply
    })
    .await
    .context("fixture import deadline exceeded")?
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn unpinned_import_stats_only_are_not_missing_root_evidence() {
        let root = super::super::car::raw_cid(b"").unwrap();
        let stats = b"{\"Stats\":{\"BlockCount\":1,\"BlockBytesCount\":0}}\n";
        assert!(validate(stats, &root, false, 1, 0).is_ok());
        assert!(validate(stats, &root, true, 1, 0).is_err());
        let pinned = format!(
            "{{\"Root\":{{\"Cid\":{{\"/\":\"{root}\"}},\"PinErrorMsg\":\"\"}}}}\n{}",
            std::str::from_utf8(stats).unwrap()
        );
        assert!(validate(pinned.as_bytes(), &root, true, 1, 0).is_ok());
        assert!(validate(pinned.as_bytes(), &root, false, 1, 0).is_err());
        assert!(
            validate(
                pinned
                    .replace(&root, &super::super::car::raw_cid(b"other").unwrap())
                    .as_bytes(),
                &root,
                true,
                1,
                0
            )
            .is_err()
        );
    }

    #[test]
    fn import_stats_unknown_records_bad_counts_and_incomplete_ndjson_fail_closed() {
        let root = super::super::car::raw_cid(b"").unwrap();
        for raw in [
            "{\"Stats\":{\"BlockCount\":2,\"BlockBytesCount\":0}}\n",
            "{\"Stats\":{\"BlockCount\":1,\"BlockBytesCount\":1}}\n",
            "{\"Stats\":{\"BlockCount\":1,\"BlockBytesCount\":0,\"Extra\":true}}\n",
            "{\"Stats\":{\"BlockCount\":1,\"BlockBytesCount\":0},\"Root\":null}\n",
            "{\"Stats\":{\"BlockCount\":1,\"BlockBytesCount\":0}}",
            "{\"Stats\":{\"BlockCount\":1,\"BlockBytesCount\":0}}\n{\"Error\":\"late failure\"}\n",
            "{\"Stats\":{\"BlockCount\":1,\"BlockBytesCount\":0}}\n\n",
        ] {
            assert!(
                validate(raw.as_bytes(), &root, false, 1, 0).is_err(),
                "accepted {raw:?}"
            );
        }
    }

    #[tokio::test]
    async fn import_http_error_trailers_and_truncated_eof_fail_closed() {
        let record = "{\"Stats\":{\"BlockCount\":1,\"BlockBytesCount\":0}}\n";
        for (ending, success) in [
            ("0\r\n\r\n", true),
            ("0\r\nX-Stream-Error: failure\r\n\r\n", false),
            ("", false),
        ] {
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
                .await
                .unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let reply = format!(
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-Stream-Error\r\nConnection: close\r\n\r\n{:X}\r\n{record}\r\n{ending}",
                record.len()
            );
            let server = async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let mut buffer = [0; 1024];
                    let size = socket.read(&mut buffer).await.unwrap();
                    assert!(size > 0 && request.len() + size <= 4096);
                    request.extend_from_slice(&buffer[..size]);
                    if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = std::str::from_utf8(&request[..end]).unwrap();
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                let (key, value) = line.split_once(':')?;
                                key.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        if request.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                socket.write_all(reply.as_bytes()).await.unwrap();
                socket.shutdown().await.unwrap();
            };
            let (result, ()) = tokio::join!(exchange(&base, b"bounded fixture", false), server);
            assert_eq!(
                result.is_ok(),
                success,
                "ending={ending:?} result={result:?}"
            );
        }
    }
}
