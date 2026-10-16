//! Local HTTP fixtures only: even a complete CAR prefix must reach clean EOF
//! before it can become a management attestation.
use sea_orm::{ConnectionTrait, DatabaseConnection};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_util::sync::CancellationToken;

pub fn serve(
    listener: TcpListener,
    case: &'static str,
    db: DatabaseConnection,
    cid: String,
    car: Vec<u8>,
    cancel: CancellationToken,
    node: &'static str,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut identities = 0;
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                request.push(socket.read_u8().await.unwrap());
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
                assert!(request.len() < 8192);
            }
            let request = String::from_utf8(request).unwrap();
            let body = if request.contains(" /api/v0/id?") {
                identities += 1;
                let changed = case == "node_before" || (case == "node_after" && identities >= 2);
                serde_json::json!({"ID":if changed {"ChangedValidPeerIdentity"} else {node}})
                    .to_string()
            } else if request.contains(" /api/v0/pin/ls?") {
                assert!(request.contains("offline=true") && request.contains(&cid));
                serde_json::json!({"Keys":{(cid.clone()):{"Type":"recursive"}}}).to_string()
            } else if request.contains(" /api/v0/files/stat?") {
                assert!(request.contains("offline=true") && request.contains(&cid));
                serde_json::json!({"Hash":cid,"WithLocality":true,"Local":true,"SizeLocal":"1"})
                    .to_string()
            } else {
                assert!(
                    request.contains(" /api/v0/dag/export?")
                        && request.contains("offline=true")
                        && request.contains(&cid)
                );
                if case == "header" || case == "car_budget_header" {
                    let extra = if case == "header" {
                        "X-Stream-Error: PRIVATE_ERROR\r\n"
                    } else {
                        ""
                    };
                    let length = if case == "car_budget_header" {
                        9_109_622
                    } else {
                        car.len()
                    };
                    socket.write_all(format!("HTTP/1.1 200 OK\r\nConnection: close\r\n{extra}Content-Length: {length}\r\n\r\n").as_bytes()).await.unwrap();
                    // The client may reject headers immediately; no body is needed.
                    continue;
                }
                socket.write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\nTrailer: X-Stream-Error\r\n\r\n").await.unwrap();
                // Split within both CAR varints and hashed blocks, rather than
                // trusting a collected fixture/parser result as the authority.
                for part in car.chunks(17) {
                    socket
                        .write_all(format!("{:X}\r\n", part.len()).as_bytes())
                        .await
                        .unwrap();
                    socket.write_all(part).await.unwrap();
                    socket.write_all(b"\r\n").await.unwrap();
                }
                match case {
                    "trailer" => socket
                        .write_all(b"0\r\nX-Stream-Error: PRIVATE_ERROR\r\n\r\n")
                        .await
                        .unwrap(),
                    "partial_trailer" => socket
                        .write_all(b"0\r\nX-Stream-Error: PRIVATE_ERROR\r\n")
                        .await
                        .unwrap(),
                    "late_eof" => {}
                    "idle" => tokio::time::sleep(std::time::Duration::from_secs(2)).await,
                    "cancel" => {
                        cancel.cancel();
                    }
                    "metadata" => {
                        db.execute_unprepared(
                            "UPDATE objects SET metadata='{\"private\":\"during-proof\"}'",
                        )
                        .await
                        .unwrap();
                        socket.write_all(b"0\r\n\r\n").await.unwrap();
                    }
                    "version" => {
                        db.execute_unprepared(
                            "UPDATE object_versions SET created_at='2001-01-01T00:00:00Z'",
                        )
                        .await
                        .unwrap();
                        socket.write_all(b"0\r\n\r\n").await.unwrap();
                    }
                    "authorization" => {
                        db.execute_unprepared("UPDATE pin_management_records SET database_scope='changed-scope' WHERE kind='database_scope'").await.unwrap();
                        socket.write_all(b"0\r\n\r\n").await.unwrap();
                    }
                    "expired" => {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        socket.write_all(b"0\r\n\r\n").await.unwrap();
                    }
                    "node_after" => socket.write_all(b"0\r\n\r\n").await.unwrap(),
                    _ => panic!("unexpected fixture case"),
                }
                continue;
            };
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
    })
}
