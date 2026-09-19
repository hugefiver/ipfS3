//! Test-only TCP round robin; preserves the signed Host and request bytes.
use std::{io, net::TcpListener, net::TcpStream, thread};

#[test]
#[ignore = "NOT RUN: runner-owned long-lived TCP load balancer process"]
fn serve_f1_balancer() {
    let bind = std::env::var("IPFS_S3_F1_BALANCER_BIND").expect("runner bind required");
    let a = std::env::var("IPFS_S3_F1_GATEWAY_A_BIND").expect("gateway A required");
    let b = std::env::var("IPFS_S3_F1_GATEWAY_B_BIND").expect("gateway B required");
    let listener = TcpListener::bind(bind).expect("bind test balancer");
    for (index, incoming) in listener.incoming().enumerate() {
        let upstream = if index % 2 == 0 { a.clone() } else { b.clone() };
        thread::spawn(move || {
            let Ok(mut client) = incoming else { return };
            let Ok(mut server) = TcpStream::connect(upstream) else {
                return;
            };
            let mut client_read = client.try_clone().expect("clone client");
            let mut server_write = server.try_clone().expect("clone server");
            let upload = thread::spawn(move || {
                let _ = io::copy(&mut client_read, &mut server_write);
                let _ = server_write.shutdown(std::net::Shutdown::Write);
            });
            let _ = io::copy(&mut server, &mut client);
            let _ = client.shutdown(std::net::Shutdown::Both);
            let _ = server.shutdown(std::net::Shutdown::Both);
            let _ = upload.join();
        });
    }
}
