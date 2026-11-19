use ipfs_s3_gateway::{
    config::Config,
    pinning::{config::ValidatedPinningConfig, coordinator::PinningCoordinator},
};

fn config(names: &[(&str, &str)]) -> Config {
    let mut text = String::from("[pinning_identity]\nprimary_storage_domain='local'\n");
    for (name, id) in names {
        text.push_str(&format!("[[pinning_identity.providers]]\nconfig_name='{name}'\nprovider_id='{id}'\ndisplay_name='{name}'\nbackend='noop'\nscope='account'\nstorage_domain='remote'\ncredential_revision=1\nendpoint_revision=1\napi_profile='noop'\nstrategy='cid'\n"));
    }
    for (name, _) in names {
        text.push_str(&format!("[[pinning.providers]]\nname='{name}'\nkind='noop'\npriority=1\nmax_bytes=100\nmax_pins=1\n"));
    }
    toml::from_str(&text).unwrap()
}

#[test]
fn same_backend_aliases_share_capacity_identity() {
    let cfg = config(&[("first", "stable-one"), ("second", "stable-two")]);
    let coordinator =
        PinningCoordinator::build(ValidatedPinningConfig::from_config(&cfg, |_| None).unwrap())
            .unwrap();
    assert_eq!(coordinator.provider_limits().len(), 1);
}

#[test]
fn rename_keeps_capacity_identity() {
    let first = PinningCoordinator::build(
        ValidatedPinningConfig::from_config(&config(&[("old", "stable")]), |_| None).unwrap(),
    )
    .unwrap();
    let second = PinningCoordinator::build(
        ValidatedPinningConfig::from_config(&config(&[("new", "stable")]), |_| None).unwrap(),
    )
    .unwrap();
    assert_eq!(first.provider_limits(), second.provider_limits());
}
