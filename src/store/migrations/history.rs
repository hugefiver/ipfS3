// These deployed layouts already have the current shared import and ZIP tables.
const DEPLOYED: &[&str] = &[
    "m20261027_000001_schema_baseline",
    "m20261108_000001_current_pinning_config",
    "m20261109_000001_pin_cid_comparisons",
    "m20261110_000001_requeue_retained",
];

pub(super) fn supported(versions: &[String]) -> bool {
    (2..=DEPLOYED.len()).contains(&versions.len())
        && versions
            .iter()
            .map(String::as_str)
            .eq(DEPLOYED[..versions.len()].iter().copied())
}
