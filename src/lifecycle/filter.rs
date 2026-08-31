#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use crate::lifecycle::model::{CanonicalFilter, CanonicalRuleSelector, CanonicalTag};
    use crate::pinning::tags::ObjectTag;

    #[test]
    fn matches_filter_uses_exact_prefix_tags_and_exclusive_size_bounds() {
        let tag = ObjectTag::new("class", "cold");
        let filter = CanonicalRuleSelector::Modern {
            filter: CanonicalFilter::And {
                prefix: Some("logs/".to_owned()),
                tags: vec![CanonicalTag {
                    key: "class".to_owned(),
                    value: "cold".to_owned(),
                }],
                object_size_greater_than: Some(1),
                object_size_less_than: Some(10),
            },
        };

        assert!(super::matches_filter(&filter, "logs/a", 2, &[tag]));
        assert!(!super::matches_filter(&filter, "Logs/a", 2, &[]));
        assert!(!super::matches_filter(&filter, "logs/a", 1, &[]));
        assert!(!super::matches_filter(&filter, "logs/a", 10, &[]));
    }

    #[test]
    fn matches_filter_models_markers_and_all_without_special_cases() {
        let all = CanonicalRuleSelector::Modern {
            filter: CanonicalFilter::All,
        };
        assert!(super::matches_filter(&all, "marker", 0, &[]));

        let tag = CanonicalRuleSelector::Modern {
            filter: CanonicalFilter::Tag {
                tag: CanonicalTag {
                    key: "class".to_owned(),
                    value: "cold".to_owned(),
                },
            },
        };
        assert!(!super::matches_filter(&tag, "marker", 0, &[]));
        assert!(!super::matches_filter(
            &tag,
            "object",
            1,
            &[ObjectTag::new("class", "warm")]
        ));

        let prefix = CanonicalRuleSelector::LegacyPrefix {
            prefix: "caf\u{e9}/".to_owned(),
        };
        assert!(super::matches_filter(&prefix, "caf\u{e9}/entry", 0, &[]));
        assert!(!super::matches_filter(&prefix, "cafe\u{301}/entry", 0, &[]));
    }
}
use crate::lifecycle::model::{CanonicalFilter, CanonicalRuleSelector, CanonicalTag};
use crate::pinning::tags::ObjectTag;

pub fn matches_filter(
    filter: &CanonicalRuleSelector,
    key: &str,
    size: i64,
    tags: &[ObjectTag],
) -> bool {
    match filter {
        CanonicalRuleSelector::LegacyPrefix { prefix } => key.starts_with(prefix),
        CanonicalRuleSelector::Modern { filter } => matches_modern_filter(filter, key, size, tags),
    }
}

fn matches_modern_filter(
    filter: &CanonicalFilter,
    key: &str,
    size: i64,
    tags: &[ObjectTag],
) -> bool {
    match filter {
        CanonicalFilter::All => true,
        CanonicalFilter::Prefix { prefix } => key.starts_with(prefix),
        CanonicalFilter::Tag { tag } => tag_matches(tag, tags),
        CanonicalFilter::ObjectSizeGreaterThan { bytes } => size > *bytes,
        CanonicalFilter::ObjectSizeLessThan { bytes } => size < *bytes,
        CanonicalFilter::And {
            prefix,
            tags: expected_tags,
            object_size_greater_than,
            object_size_less_than,
        } => {
            prefix.as_ref().is_none_or(|prefix| key.starts_with(prefix))
                && expected_tags.iter().all(|tag| tag_matches(tag, tags))
                && bounds_match(size, *object_size_greater_than, *object_size_less_than)
        }
    }
}

fn bounds_match(size: i64, greater: Option<i64>, less: Option<i64>) -> bool {
    greater.is_none_or(|bound| size > bound) && less.is_none_or(|bound| size < bound)
}

fn tag_matches(expected: &CanonicalTag, tags: &[ObjectTag]) -> bool {
    tags.iter()
        .any(|actual| actual.key == expected.key && actual.value == expected.value)
}
