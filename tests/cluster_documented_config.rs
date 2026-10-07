use ipfs_s3_gateway::{
    config::Config,
    kubo::KuboClient,
    pinning::{
        config::{ProviderKind, ValidatedPinningConfig},
        coordinator::PinningCoordinator,
    },
};

fn documented_cluster() -> String {
    let example = include_str!("../config.example.toml")
        .split("# Stage 6 opt-in independent Cluster provider.")
        .nth(1)
        .unwrap();
    let block = example
        .split("# [[pinning.providers]]")
        .nth(1)
        .unwrap()
        .split("# Authentication defaults")
        .next()
        .unwrap();
    let lines: Vec<_> = block
        .lines()
        .map(|line| line.strip_prefix("# ").unwrap_or(""))
        .collect();
    format!(
        "[pinning_identity]\nprimary_storage_domain = 'kubo:primary'\n\n[[pinning.providers]]{}",
        lines.join("\n")
    )
}

#[test]
fn documented_cluster_profiles_validate_and_register_without_network_io() {
    for protocol in ["proxy", "rest"] {
        let mut text = documented_cluster();
        if protocol == "rest" {
            text = text
                .replace("protocol = \"proxy\"", "protocol = \"rest\"")
                .replace("proxy_endpoint = \"http://127.0.0.1:9095\"\n", "")
                .replace("cluster-proxy", "cluster-rest");
        }
        let config: Config = toml::from_str(&text).unwrap();
        let validated = ValidatedPinningConfig::from_config(&config, |_| None).unwrap();
        assert_eq!(validated.providers[0].kind, ProviderKind::Cluster);
        assert!(
            validated.providers[0]
                .route_snapshot()
                .confirmation
                .is_some()
        );
        let key = validated.providers[0].identity.allocation_key();
        let coordinator = PinningCoordinator::build_with_kubo(
            validated,
            Some(KuboClient::new(config.kubo.rpc_url)),
        )
        .unwrap();
        assert_eq!(
            coordinator.provider(&key).unwrap().invocation_route(),
            (
                if protocol == "proxy" {
                    "cluster-proxy"
                } else {
                    "cluster-rest"
                },
                "car"
            )
        );
    }
}
