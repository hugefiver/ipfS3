use super::{network, tests::*, *};
use reqwest::dns::{Name, Resolve, Resolving};
use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[derive(Clone)]
pub(super) enum Answer {
    Addresses(Vec<SocketAddr>),
    Fail,
    Stall,
}

pub(super) struct Dns {
    pub answer: Mutex<Answer>,
    pub calls: AtomicUsize,
    pub active: Arc<AtomicUsize>,
    pub names: Mutex<Vec<String>>,
}
impl Dns {
    pub fn new(answer: Answer) -> Arc<Self> {
        Arc::new(Self {
            answer: Mutex::new(answer),
            calls: AtomicUsize::new(0),
            active: Arc::new(AtomicUsize::new(0)),
            names: Mutex::new(Vec::new()),
        })
    }
}
struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl Resolve for Dns {
    fn resolve(&self, name: Name) -> Resolving {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.names.lock().unwrap().push(name.as_str().to_owned());
        let answer = self.answer.lock().unwrap().clone();
        let active = self.active.clone();
        Box::pin(async move {
            active.fetch_add(1, Ordering::SeqCst);
            let _active = Active(active);
            match answer {
                Answer::Addresses(values) => {
                    Ok(Box::new(values.into_iter()) as reqwest::dns::Addrs)
                }
                Answer::Fail => Err(Box::new(std::io::Error::other("fixture DNS error"))
                    as Box<dyn std::error::Error + Send + Sync>),
                Answer::Stall => std::future::pending().await,
            }
        })
    }
}

#[test]
fn public_policy_rejects_all_private_special_and_mixed_dns_answers() {
    use ClusterNetworkPolicy::{AllowPrivate, PublicOnly};
    for ip in [
        "0.0.0.0",
        "10.0.0.1",
        "100.100.100.200",
        "127.0.0.1",
        "169.254.169.254",
        "172.16.0.1",
        "192.168.1.1",
        "192.0.0.1",
        "192.0.2.1",
        "198.18.0.1",
        "198.51.100.1",
        "203.0.113.1",
        "224.0.0.1",
        "255.255.255.255",
        "::",
        "::1",
        "fd00:ec2::254",
        "fe80::1",
        "ff02::1",
        "::ffff:127.0.0.1",
        "2001:db8::1",
        "2002:7f00:1::1",
        "64:ff9b::a00:1",
    ] {
        let value = SocketAddr::new(ip.parse().unwrap(), 0);
        assert!(!network::public_ip(value.ip()), "allowed {ip}");
        assert!(network::addresses(PublicOnly, vec![value]).is_err());
        assert!(
            network::addresses(PublicOnly, vec!["8.8.8.8:0".parse().unwrap(), value]).is_err(),
            "filtered a mixed answer instead of rejecting it: {ip}"
        );
        assert!(network::addresses(AllowPrivate, vec![value]).is_ok());
    }
    assert!(network::addresses(PublicOnly, Vec::new()).is_err());
    assert!(network::addresses(AllowPrivate, Vec::new()).is_err());
    assert!(network::addresses(PublicOnly, vec!["8.8.8.8:0".parse().unwrap(); 65]).is_err());
    let values = network::addresses(
        PublicOnly,
        vec![
            "8.8.8.8:9999".parse().unwrap(),
            "[2606:4700:4700::1111]:123".parse().unwrap(),
        ],
    )
    .unwrap()
    .collect::<Vec<_>>();
    assert!(values.iter().all(|a| a.port() == 0));
}

#[test]
fn both_policies_share_strict_endpoint_syntax_and_public_hostname_validation() {
    use ClusterNetworkPolicy::{AllowPrivate, PublicOnly};
    for value in [
        "https://@example.com",
        "https://user:secret@example.com",
        "https://example.com?token=s",
        "https://example.com#s",
        "file:///tmp/x",
        "http://",
    ] {
        assert!(network::endpoint(value, PublicOnly).is_err());
        assert!(network::endpoint(value, AllowPrivate).is_err());
    }
    for value in [
        "https://example.com",
        "https://example.com.",
        "https://xn--bcher-kva.de/base",
        "https://8.8.8.8",
        "https://[2606:4700:4700::1111]",
    ] {
        assert!(network::endpoint(value, PublicOnly).is_ok(), "{value}");
    }
    for value in [
        "http://example.com",
        "https://localhost",
        "https://localhost.",
        "https://service.localhost.",
        "https://metadata.google.internal",
        "https://service.local",
        "https://bad_name.example.com",
        "https://example..com",
        "https://example.com..",
    ] {
        assert!(network::endpoint(value, PublicOnly).is_err(), "{value}");
        assert!(
            network::endpoint(value, AllowPrivate).is_ok(),
            "explicit private syntax {value}"
        );
    }
}

#[tokio::test]
async fn connection_uses_only_the_current_dns_answer_without_hostname_rewrite_or_pool_reuse() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/bound"))
        .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
        .expect(2)
        .mount(&server)
        .await;
    let address: SocketAddr = server.address().to_owned();
    let base = reqwest::Url::parse(&format!(
        "http://bound.cluster.example.com:{}",
        address.port()
    ))
    .unwrap();
    let dns = Dns::new(Answer::Addresses(vec![SocketAddr::new(address.ip(), 1)]));
    let client = network::client_with_resolver(
        &base,
        ClusterNetworkPolicy::AllowPrivate,
        Duration::from_millis(250),
        Some(Duration::from_secs(1)),
        dns.clone(),
    )
    .unwrap();
    for _ in 0..2 {
        assert_eq!(
            client
                .get(format!("{base}bound"))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
            "ok"
        );
    }
    assert_eq!(
        dns.calls.load(Ordering::SeqCst),
        2,
        "a previous pooled connection skipped DNS policy"
    );
    assert!(
        dns.names
            .lock()
            .unwrap()
            .iter()
            .all(|name| name == "bound.cluster.example.com")
    );
    let expected = format!("bound.cluster.example.com:{}", address.port());
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| r.headers["host"] == expected)
    );
    *dns.answer.lock().unwrap() = Answer::Fail;
    assert!(client.get(format!("{base}bound")).send().await.is_err());
    assert_eq!(dns.calls.load(Ordering::SeqCst), 3);
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn public_verifier_dns_denial_never_connects_or_mints_proof() {
    let server = MockServer::start().await;
    let base = format!(
        "https://bound.cluster.example.com:{}",
        server.address().port()
    );
    let p = ClusterRestProfile::new(
        ClusterPinset::new("cluster", "policy").unwrap(),
        Some(&base),
        Some(ClusterAuth::Bearer("auth-sentinel".into())),
        1,
        RestLimits::default(),
    )
    .unwrap();
    let dns = Dns::new(Answer::Addresses(vec![
        "8.8.8.8:0".parse().unwrap(),
        *server.address(),
    ]));
    let verifier = ClusterReplicaVerifier::with_resolver(p, dns.clone()).unwrap();
    let proof = verifier.observe(CID).await;
    assert_eq!(proof.outcome(), ReplicaOutcome::Unknown);
    assert_eq!(proof.failure().unwrap().code, VerificationCode::Transport);
    assert!(proof.evidence().is_none());
    assert_eq!(dns.calls.load(Ordering::SeqCst), 1);
    assert!(server.received_requests().await.unwrap().is_empty());
    assert!(!format!("{proof:?}").contains("auth-sentinel"));
}

#[tokio::test]
async fn denied_empty_failed_or_deadline_dns_supersedes_proof_without_revival() {
    let server = MockServer::start().await;
    mounts(
        &server,
        allocation(&["a", "b"]),
        global(&["a", "b"], &[("a", "pinned"), ("b", "pinned")]),
    )
    .await;
    let base = format!(
        "http://bound.cluster.example.com:{}",
        server.address().port()
    );
    let mut p = profile(&base, 2); // Explicit AllowPrivate loopback fixture.
    p.limits.connect_timeout = Duration::from_millis(60);
    p.limits.idle_timeout = Duration::from_millis(300);
    p.limits.total_timeout = Duration::from_secs(1);
    let dns = Dns::new(Answer::Addresses(vec![*server.address()]));
    let verifier = ClusterReplicaVerifier::with_resolver(p, dns.clone()).unwrap();
    let mut previous = verifier.observe(CID).await;
    assert_eq!(previous.outcome(), ReplicaOutcome::Complete);
    for answer in [Answer::Addresses(Vec::new()), Answer::Fail, Answer::Stall] {
        *dns.answer.lock().unwrap() = answer;
        let denied = verifier.observe(CID).await;
        assert_eq!(denied.outcome(), ReplicaOutcome::Unknown);
        assert!(denied.evidence().is_none());
        assert_eq!(denied.failure().unwrap().code, VerificationCode::Transport);
        assert_eq!(previous.outcome(), ReplicaOutcome::Unknown);
        assert_eq!(
            previous.failure().unwrap().code,
            VerificationCode::StaleObservation
        );
        assert_eq!(previous.evidence().unwrap().pinned_peers.len(), 2);
        assert_eq!(
            dns.active.load(Ordering::SeqCst),
            0,
            "DNS future escaped its deadline"
        );
        *dns.answer.lock().unwrap() = Answer::Addresses(vec![*server.address()]);
        let refreshed = verifier.observe(CID).await;
        assert_eq!(refreshed.outcome(), ReplicaOutcome::Complete);
        assert_eq!(
            previous.outcome(),
            ReplicaOutcome::Unknown,
            "a new success resurrected an old proof"
        );
        previous = refreshed;
    }
    assert_eq!(dns.calls.load(Ordering::SeqCst), 19); // 4 + (1 denied + 4 fresh) * 3.
}
