use std::collections::BTreeMap;

use http_body_util::BodyExt as _;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::KuboClient;
use crate::error::{AppError, AppResult};

const MAX_VERIFICATION_RESPONSE_BYTES: usize = 64 * 1024;

/// Evidence that one Kubo node reported the requested CID as both a recursive
/// pin and a completely local UnixFS DAG.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LocalResidencyVerificationReceipt {
    pub node_identity: String,
    pub cid: String,
}

#[derive(Deserialize)]
struct PinListResponse {
    #[serde(rename = "Keys")]
    keys: BTreeMap<String, PinListEntry>,
}

#[derive(Deserialize)]
struct PinListEntry {
    #[serde(rename = "Type")]
    pin_type: String,
}

#[derive(Deserialize)]
struct LocalStatResponse {
    #[serde(rename = "Hash")]
    hash: String,
    #[serde(rename = "WithLocality")]
    with_locality: Option<bool>,
    #[serde(rename = "Local")]
    local: Option<bool>,
}

impl KuboClient {
    /// Verify that `cid` is a complete recursive pin in this Kubo node's local
    /// blockstore, without permitting network retrieval.
    ///
    /// The pin check brackets an offline `files/stat?with-local=true` walk so a
    /// concurrent unpin cannot make a transient local walk look durable. The
    /// node identity is also checked before and after the proof. Every response
    /// uses the existing bounded control-plane client and is capped at 64 KiB.
    pub async fn verify_local_residency(
        &self,
        cid: &str,
    ) -> AppResult<LocalResidencyVerificationReceipt> {
        let expected_cid = cid::Cid::try_from(cid)
            .map_err(|_| AppError::kubo_rpc_detail("invalid CID for local verification"))?;

        let node_identity = self.local_node_identity().await?;
        self.verify_recursive_pin(&expected_cid, cid).await?;
        self.verify_complete_local_dag(&expected_cid, cid).await?;
        self.verify_recursive_pin(&expected_cid, cid).await?;

        if self.local_node_identity().await? != node_identity {
            return Err(AppError::kubo_rpc_detail(
                "Kubo node identity changed during local verification",
            ));
        }

        Ok(LocalResidencyVerificationReceipt {
            node_identity,
            // Preserve the caller's public CID spelling. Kubo response CIDs are
            // parsed and compared by CID identity rather than string encoding.
            cid: cid.to_owned(),
        })
    }

    async fn verify_recursive_pin(
        &self,
        expected_cid: &cid::Cid,
        requested_cid: &str,
    ) -> AppResult<()> {
        let mut url = reqwest::Url::parse(&format!("{}/api/v0/pin/ls", self.base_url()))
            .map_err(|_| AppError::kubo_rpc_detail("invalid Kubo RPC URL"))?;
        url.query_pairs_mut()
            .append_pair("arg", requested_cid)
            .append_pair("type", "recursive")
            .append_pair("offline", "true");
        let response: PinListResponse =
            bounded_control_json(self.http().post(url), "local verification recursive pin").await?;

        if response.keys.len() != 1 {
            return Err(AppError::kubo_rpc_detail(
                "Kubo recursive pin response omitted requested CID",
            ));
        }
        let (reported_cid, pin) = response
            .keys
            .into_iter()
            .next()
            .expect("length checked above");
        let reported_cid = cid::Cid::try_from(reported_cid.as_str())
            .map_err(|_| AppError::kubo_rpc_detail("invalid Kubo recursive pin response"))?;
        if reported_cid != *expected_cid || pin.pin_type != "recursive" {
            return Err(AppError::kubo_rpc_detail(
                "Kubo did not report the requested recursive pin",
            ));
        }
        Ok(())
    }

    async fn verify_complete_local_dag(
        &self,
        expected_cid: &cid::Cid,
        requested_cid: &str,
    ) -> AppResult<()> {
        let mut url = reqwest::Url::parse(&format!("{}/api/v0/files/stat", self.base_url()))
            .map_err(|_| AppError::kubo_rpc_detail("invalid Kubo RPC URL"))?;
        let path = format!("/ipfs/{requested_cid}");
        url.query_pairs_mut()
            .append_pair("arg", path.as_str())
            .append_pair("with-local", "true")
            .append_pair("offline", "true");
        let response: LocalStatResponse =
            bounded_control_json(self.http().post(url), "local verification DAG stat").await?;

        let reported_cid = cid::Cid::try_from(response.hash.as_str())
            .map_err(|_| AppError::kubo_rpc_detail("invalid Kubo local DAG response"))?;
        if reported_cid != *expected_cid
            || response.with_locality != Some(true)
            || response.local != Some(true)
        {
            return Err(AppError::kubo_rpc_detail(
                "Kubo local DAG verification was incomplete",
            ));
        }
        Ok(())
    }
}

pub(crate) async fn bounded_control_json<T: DeserializeOwned>(
    request: reqwest::RequestBuilder,
    operation: &'static str,
) -> AppResult<T> {
    let response = request
        .send()
        .await
        .map_err(|_| AppError::kubo_rpc_detail("Kubo local verification request failed"))?;
    if !response.status().is_success() {
        let status = response.status();
        tracing::warn!(operation, status = status.as_u16(), "kubo rpc call failed");
        return Err(AppError::kubo_rpc_status(status));
    }
    if response.headers().contains_key("x-stream-error") {
        return Err(AppError::kubo_rpc_detail(
            "Kubo local verification stream reported failure",
        ));
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_VERIFICATION_RESPONSE_BYTES as u64)
    {
        return Err(AppError::kubo_rpc_detail(
            "Kubo local verification response exceeds limit",
        ));
    }

    let mut body: reqwest::Body = response.into();
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| {
            AppError::kubo_rpc_detail("Kubo local verification response stream failed")
        })?;
        match frame.into_data() {
            Ok(data) => {
                if bytes.len().saturating_add(data.len()) > MAX_VERIFICATION_RESPONSE_BYTES {
                    return Err(AppError::kubo_rpc_detail(
                        "Kubo local verification response exceeds limit",
                    ));
                }
                bytes.extend_from_slice(&data);
            }
            Err(frame) => match frame.into_trailers() {
                Ok(trailers) if trailers.contains_key("x-stream-error") => {
                    return Err(AppError::kubo_rpc_detail(
                        "Kubo local verification stream reported failure",
                    ));
                }
                Ok(_) | Err(_) => {}
            },
        }
    }

    serde_json::from_slice(&bytes)
        .map_err(|_| AppError::kubo_rpc_detail("invalid Kubo local verification response"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    const OTHER_CID: &str = "bafkreigh2akiscaildc6ii5zji4bq7kly5k3s7svv6q2wx2nn5rtj5xuu4";
    const NODE_ID: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";

    async fn mount_identity(server: &MockServer, body: &str, expected_calls: u64) {
        Mock::given(method("POST"))
            .and(path("/api/v0/id"))
            .and(query_param("peerid-base", "b58mh"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .expect(expected_calls)
            .mount(server)
            .await;
    }

    async fn mount_recursive_pin(server: &MockServer, body: String) {
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/ls"))
            .and(query_param("arg", CID))
            .and(query_param("type", "recursive"))
            .and(query_param("offline", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .expect(2)
            .mount(server)
            .await;
    }

    async fn mount_local_stat(server: &MockServer, body: String) {
        Mock::given(method("POST"))
            .and(path("/api/v0/files/stat"))
            .and(query_param("arg", format!("/ipfs/{CID}")))
            .and(query_param("with-local", "true"))
            .and(query_param("offline", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .expect(1)
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn verifies_same_node_recursive_pin_and_complete_local_dag() {
        let server = MockServer::start().await;
        mount_identity(&server, &format!(r#"{{"ID":"{NODE_ID}"}}"#), 2).await;
        mount_recursive_pin(
            &server,
            format!(r#"{{"Keys":{{"{CID}":{{"Type":"recursive"}}}}}}"#),
        )
        .await;
        mount_local_stat(
            &server,
            format!(r#"{{"Hash":"{CID}","WithLocality":true,"Local":true,"SizeLocal":42}}"#),
        )
        .await;

        let receipt = KuboClient::new(server.uri())
            .verify_local_residency(CID)
            .await
            .expect("complete recursive local residency must verify");

        assert_eq!(
            receipt,
            LocalResidencyVerificationReceipt {
                node_identity: NODE_ID.to_owned(),
                cid: CID.to_owned(),
            }
        );
    }

    #[tokio::test]
    async fn rejects_invalid_requested_cid_without_calling_kubo() {
        let server = MockServer::start().await;
        let error = KuboClient::new(server.uri())
            .verify_local_residency("not-a-cid")
            .await
            .expect_err("invalid CID must fail closed");

        assert_eq!(error.to_string(), "kubo rpc failure");
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn rejects_missing_or_non_recursive_pin() {
        for pin_body in [
            r#"{"Keys":{}}"#.to_owned(),
            format!(r#"{{"Keys":{{"{CID}":{{"Type":"direct"}}}}}}"#),
        ] {
            let server = MockServer::start().await;
            mount_identity(&server, &format!(r#"{{"ID":"{NODE_ID}"}}"#), 1).await;
            Mock::given(method("POST"))
                .and(path("/api/v0/pin/ls"))
                .respond_with(ResponseTemplate::new(200).set_body_string(pin_body))
                .expect(1)
                .mount(&server)
                .await;

            KuboClient::new(server.uri())
                .verify_local_residency(CID)
                .await
                .expect_err("missing or direct pin must fail closed");
        }
    }

    #[tokio::test]
    async fn rejects_incomplete_local_dag() {
        let server = MockServer::start().await;
        mount_identity(&server, &format!(r#"{{"ID":"{NODE_ID}"}}"#), 1).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/ls"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(format!(r#"{{"Keys":{{"{CID}":{{"Type":"recursive"}}}}}}"#)),
            )
            .expect(1)
            .mount(&server)
            .await;
        mount_local_stat(
            &server,
            format!(r#"{{"Hash":"{CID}","WithLocality":true,"Local":false,"SizeLocal":1}}"#),
        )
        .await;

        KuboClient::new(server.uri())
            .verify_local_residency(CID)
            .await
            .expect_err("a partial local DAG must fail closed");
    }

    #[tokio::test]
    async fn rejects_mismatched_reported_cid() {
        let server = MockServer::start().await;
        mount_identity(&server, &format!(r#"{{"ID":"{NODE_ID}"}}"#), 1).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/ls"))
            .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                r#"{{"Keys":{{"{OTHER_CID}":{{"Type":"recursive"}}}}}}"#
            )))
            .expect(1)
            .mount(&server)
            .await;

        KuboClient::new(server.uri())
            .verify_local_residency(CID)
            .await
            .expect_err("a different CID in the pin response must fail closed");
    }

    #[tokio::test]
    async fn rejects_invalid_response_and_http_error() {
        for response in [
            ResponseTemplate::new(200).set_body_string(r#"{"ID":"not a peer id"}"#),
            ResponseTemplate::new(200).set_body_string("not-json"),
            ResponseTemplate::new(503).set_body_string("private backend details"),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/v0/id"))
                .and(query_param("peerid-base", "b58mh"))
                .respond_with(response)
                .expect(1)
                .mount(&server)
                .await;

            let error = KuboClient::new(server.uri())
                .verify_local_residency(CID)
                .await
                .expect_err("invalid or failed identity response must fail closed");
            assert_eq!(error.to_string(), "kubo rpc failure");
            assert!(!error.to_string().contains("private backend details"));
        }
    }

    #[tokio::test]
    async fn rejects_oversized_control_response() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/id"))
            .and(query_param("peerid-base", "b58mh"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![
                b'x';
                MAX_VERIFICATION_RESPONSE_BYTES
                    + 1
            ]))
            .expect(1)
            .mount(&server)
            .await;

        KuboClient::new(server.uri())
            .verify_local_residency(CID)
            .await
            .expect_err("oversized control JSON must fail without unbounded collection");
    }

    #[tokio::test]
    async fn bounded_control_json_rejects_late_stream_error_trailer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
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
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-Stream-Error\r\nConnection: close\r\n\r\n{:X}\r\n{{\"ID\":\"{NODE_ID}\"}}\r\n0\r\nX-Stream-Error: late verification failure\r\n\r\n",
                        format!(r#"{{"ID":"{NODE_ID}"}}"#).len(),
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });

        KuboClient::new(endpoint)
            .local_node_identity()
            .await
            .expect_err("a successful JSON record cannot hide a late Kubo error trailer");
        server.await.unwrap();
    }
}
