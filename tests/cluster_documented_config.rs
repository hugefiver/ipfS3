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
        .split("# [[pinning.providers]]")
        .find(|block| {
            block
                .lines()
                .take_while(|line| !line.starts_with("# ["))
                .any(|line| line == "# kind = \"cluster\"")
        })
        .expect("Cluster provider example");
    let lines: Vec<_> = example
        .lines()
        .map(|line| line.strip_prefix("# ").unwrap_or(""))
        .take_while(|line| {
            line.is_empty()
                || line.starts_with('[')
                || line
                    .split_once(" = ")
                    .is_some_and(|(key, _)| !key.contains(' '))
        })
        .collect();
    format!(
        "[pinning_identity]\nprimary_storage_domain = 'kubo:primary'\n\n[[pinning.providers]]\n{}",
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
        let provider = &validated.providers[0];
        let options = provider.cluster.as_ref().unwrap();
        assert_eq!(
            options.protocol.api_profile(),
            format!("cluster-{protocol}")
        );
        assert_eq!(options.strategy.as_str(), "car");
        let key = provider.identity.allocation_key();
        let coordinator = PinningCoordinator::build_with_kubo(
            validated,
            Some(KuboClient::new(config.kubo.rpc_url)),
        )
        .unwrap();
        assert_eq!(coordinator.provider(&key).unwrap().name(), key);
    }
}
