use std::collections::HashSet;

use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use super::{
    KuboClient, KuboProgress, NdjsonBuffer, ProgressSender, next_response_frame, send_progress,
    send_request,
};
use crate::error::{AppError, AppResult};

#[derive(Deserialize)]
struct RoutingEvent {
    #[serde(rename = "Type")]
    event_type: Option<u8>,
    #[serde(rename = "Responses", default)]
    responses: Option<Vec<ProviderRecord>>,
}

#[derive(Deserialize)]
struct ProviderRecord {
    #[serde(rename = "ID")]
    peer_id: String,
}

pub async fn find_providers(
    kubo: &KuboClient,
    cid: &str,
    max_providers: usize,
    progress: ProgressSender,
    cancel: CancellationToken,
) -> AppResult<u32> {
    let canonical_cid = cid::Cid::try_from(cid)
        .map_err(|_| AppError::kubo_rpc_detail("invalid CID for Kubo provider discovery"))?
        .to_string();
    if !(1..=20).contains(&max_providers) {
        return Err(AppError::kubo_rpc_detail("invalid Kubo provider limit"));
    }

    let url = format!(
        "{}/api/v0/routing/findprovs?arg={canonical_cid}&num-providers={max_providers}&verbose=true",
        kubo.base_url()
    );
    let response = send_request(kubo.http().post(url), &cancel).await?;
    if !response.status().is_success() {
        let status = response.status();
        tracing::warn!(
            operation = "routing find providers",
            cid = %canonical_cid,
            status = status.as_u16(),
            "kubo rpc call failed"
        );
        return Err(AppError::kubo_rpc_status(status));
    }

    let mut body = response.bytes_stream();
    let mut lines = NdjsonBuffer::new();
    let mut peer_ids = HashSet::new();
    loop {
        let Some(frame) = next_response_frame(&mut body, kubo, &cancel).await? else {
            break;
        };
        lines.push(frame)?;
        while let Some(record) = lines.next_record()? {
            observe_providers(&record, &mut peer_ids, max_providers, &progress, &cancel).await?;
            if peer_ids.len() == max_providers {
                return Ok(peer_ids.len() as u32);
            }
        }
    }
    if let Some(record) = lines.finish() {
        observe_providers(&record, &mut peer_ids, max_providers, &progress, &cancel).await?;
    }
    Ok(peer_ids.len() as u32)
}

async fn observe_providers(
    record: &[u8],
    peer_ids: &mut HashSet<String>,
    max_providers: usize,
    progress: &ProgressSender,
    cancel: &CancellationToken,
) -> AppResult<()> {
    let event: RoutingEvent = serde_json::from_slice(record)
        .map_err(|_| AppError::kubo_rpc_detail("invalid Kubo provider response"))?;
    // Kubo's verbose routing stream also emits DHT traversal hops. Only type 4
    // records represent providers; the other peer IDs are not replicas.
    if event.event_type != Some(4) {
        return Ok(());
    }
    for provider in event.responses.unwrap_or_default() {
        if provider.peer_id.is_empty() {
            return Err(AppError::kubo_rpc_detail("invalid Kubo provider response"));
        }
        if peer_ids.len() == max_providers {
            break;
        }
        if peer_ids.insert(provider.peer_id.clone()) {
            send_progress(
                progress,
                KuboProgress::ProviderObserved {
                    peer_id: provider.peer_id,
                },
                cancel,
            )
            .await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::find_providers;
    use crate::kubo::{KuboClient, KuboProgress};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const CID: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";

    async fn chunked_server(
        chunks: Vec<Vec<u8>>,
        keep_open: bool,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut byte = [0_u8; 1];
                socket.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                .await
                .unwrap();
            for chunk in chunks {
                socket
                    .write_all(format!("{:X}\r\n", chunk.len()).as_bytes())
                    .await
                    .unwrap();
                socket.write_all(&chunk).await.unwrap();
                socket.write_all(b"\r\n").await.unwrap();
                socket.flush().await.unwrap();
            }
            if keep_open {
                std::future::pending::<()>().await;
            }
            socket.write_all(b"0\r\n\r\n").await.unwrap();
        });
        (endpoint, task)
    }

    #[tokio::test]
    async fn find_providers_deduplicates_fragmented_response_records() {
        let (endpoint, server) = chunked_server(
            vec![
                b"{\"Type\":4,\"Res".to_vec(),
                b"ponses\":[{\"ID\":\"peer-a\"}]}\r\n{\"Type\":4,".to_vec(),
                b"\"Responses\":[{\"ID\":\"peer-a\"},{\"ID\":\"peer-b\"}]}\n".to_vec(),
            ],
            false,
        )
        .await;

        let (progress, mut observed) = mpsc::channel(4);
        let count = find_providers(
            &KuboClient::new(endpoint),
            CID,
            2,
            progress,
            CancellationToken::new(),
        )
        .await
        .expect("provider discovery must succeed");

        assert_eq!(count, 2);
        assert_eq!(
            observed.recv().await,
            Some(KuboProgress::ProviderObserved {
                peer_id: "peer-a".to_owned()
            })
        );
        assert_eq!(
            observed.recv().await,
            Some(KuboProgress::ProviderObserved {
                peer_id: "peer-b".to_owned()
            })
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn find_providers_does_not_count_dht_traversal_hops_as_providers() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/routing/findprovs"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "{\"Type\":0,\"Responses\":null}\n{\"Type\":4,\"Responses\":[{\"ID\":\"provider\"}]}\n",
            ))
            .mount(&server)
            .await;
        let (progress, mut observed) = mpsc::channel(2);
        let count = find_providers(
            &KuboClient::new(server.uri()),
            CID,
            1,
            progress,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(count, 1);
        assert_eq!(
            observed.recv().await,
            Some(KuboProgress::ProviderObserved {
                peer_id: "provider".to_owned()
            })
        );
    }

    #[tokio::test]
    async fn find_providers_validates_input_and_redacts_non_success() {
        let (progress, _observed) = mpsc::channel(1);
        let invalid = find_providers(
            &KuboClient::new("http://127.0.0.1:1".to_owned()),
            "not-a-cid",
            1,
            progress,
            CancellationToken::new(),
        )
        .await
        .expect_err("invalid CID must fail before RPC");
        assert_eq!(invalid.to_string(), "kubo rpc failure");

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/routing/findprovs"))
            .respond_with(ResponseTemplate::new(502).set_body_string("private Kubo body"))
            .mount(&server)
            .await;
        let (progress, _observed) = mpsc::channel(1);
        let error = find_providers(
            &KuboClient::new(server.uri()),
            CID,
            1,
            progress,
            CancellationToken::new(),
        )
        .await
        .expect_err("non-success must fail");
        assert_eq!(error.to_string(), "kubo rpc failure");
        assert!(!error.to_string().contains("private Kubo body"));
    }

    #[tokio::test]
    async fn find_providers_rejects_bad_records_and_honors_cancel_and_idle_timeout() {
        for body in [
            b"{\"Responses\":\n".to_vec(),
            [
                vec![b'x'; super::super::MAX_NDJSON_RECORD_BYTES + 1],
                vec![b'\n'],
            ]
            .concat(),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/v0/routing/findprovs"))
                .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
                .mount(&server)
                .await;
            let (progress, _observed) = mpsc::channel(1);
            let error = find_providers(
                &KuboClient::new(server.uri()),
                CID,
                1,
                progress,
                CancellationToken::new(),
            )
            .await
            .expect_err("invalid provider record must fail");
            assert_eq!(error.to_string(), "kubo rpc failure");
        }

        let canceled = CancellationToken::new();
        canceled.cancel();
        let (progress, _observed) = mpsc::channel(1);
        let error = find_providers(
            &KuboClient::new("http://127.0.0.1:1".to_owned()),
            CID,
            1,
            progress,
            canceled,
        )
        .await
        .expect_err("canceled provider lookup must fail");
        assert_eq!(error.to_string(), "kubo rpc failure");

        let (endpoint, server) = chunked_server(Vec::new(), true).await;
        let client = KuboClient::new_with_timeouts(
            endpoint,
            Duration::from_secs(5),
            Duration::from_millis(50),
        );
        let (progress, _observed) = mpsc::channel(1);
        let error = find_providers(&client, CID, 1, progress, CancellationToken::new())
            .await
            .expect_err("stalled provider lookup must time out");
        assert_eq!(error.to_string(), "kubo rpc failure");
        server.abort();
        let _ = server.await;
    }
}
