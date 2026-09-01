use http::{HeaderName, Method};

use crate::cors::model::{CorsConfiguration, CorsRule};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AllowOrigin {
    Any,
    Echo,
}

pub struct CorsMatch<'a> {
    pub rule: &'a CorsRule,
    pub allow_origin: AllowOrigin,
}

pub fn first_match<'a>(
    config: &'a CorsConfiguration,
    origin: &str,
    method: &Method,
    requested_headers: &[HeaderName],
) -> Option<CorsMatch<'a>> {
    for rule in &config.rules {
        let Some(allow_origin) = match_origin(&rule.allowed_origins, origin) else {
            continue;
        };
        if !rule
            .allowed_methods
            .iter()
            .any(|allowed| allowed == method.as_str())
        {
            continue;
        }
        if !requested_headers.iter().all(|name| {
            rule.allowed_headers
                .iter()
                .any(|pattern| header_pattern_matches(pattern, name.as_str()))
        }) {
            continue;
        }
        return Some(CorsMatch { rule, allow_origin });
    }
    None
}

fn match_origin(patterns: &[String], origin: &str) -> Option<AllowOrigin> {
    patterns.iter().find_map(|pattern| {
        if pattern == "*" {
            Some(AllowOrigin::Any)
        } else {
            pattern_matches(pattern, origin).then_some(AllowOrigin::Echo)
        }
    })
}

fn header_pattern_matches(pattern: &str, header: &str) -> bool {
    match pattern.split_once('*') {
        Some((prefix, suffix)) => {
            header.len() >= prefix.len() + suffix.len()
                && ascii_case_insensitive_prefix(header, prefix)
                && ascii_case_insensitive_suffix(header, suffix)
        }
        None => pattern.eq_ignore_ascii_case(header),
    }
}

fn pattern_matches(pattern: &str, value: &str) -> bool {
    match pattern.split_once('*') {
        Some((prefix, suffix)) => {
            value.len() >= prefix.len() + suffix.len()
                && value.starts_with(prefix)
                && value.ends_with(suffix)
        }
        None => pattern == value,
    }
}

fn ascii_case_insensitive_prefix(value: &str, prefix: &str) -> bool {
    value
        .get(..prefix.len())
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix))
}

fn ascii_case_insensitive_suffix(value: &str, suffix: &str) -> bool {
    value
        .get(value.len().saturating_sub(suffix.len())..)
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(suffix))
}

#[cfg(test)]
mod tests {
    use http::{HeaderName, Method};

    use super::{AllowOrigin, first_match};
    use crate::cors::model::{CorsConfiguration, CorsRule};

    fn rule(origins: Vec<&str>, methods: Vec<&str>, headers: Vec<&str>) -> CorsRule {
        CorsRule {
            allowed_origins: origins.into_iter().map(str::to_owned).collect(),
            allowed_methods: methods.into_iter().map(str::to_owned).collect(),
            allowed_headers: headers.into_iter().map(str::to_owned).collect(),
            expose_headers: vec![],
            id: None,
            max_age_seconds: None,
        }
    }

    fn config(rules: Vec<CorsRule>) -> CorsConfiguration {
        CorsConfiguration { rules }
    }

    fn headers(names: &[&str]) -> Vec<HeaderName> {
        names
            .iter()
            .map(|name| HeaderName::from_bytes(name.as_bytes()).unwrap())
            .collect()
    }

    #[test]
    fn exact_origin_match_echoes_the_origin() {
        let config = config(vec![rule(vec!["https://app.example"], vec!["GET"], vec![])]);

        let matched = first_match(&config, "https://app.example", &Method::GET, &[]).unwrap();
        assert!(std::ptr::eq(matched.rule, &config.rules[0]));
        assert_eq!(matched.allow_origin, AllowOrigin::Echo);
    }

    #[test]
    fn partial_origin_wildcard_is_anchored_and_echoes_the_origin() {
        let config = config(vec![rule(vec!["https://*.example"], vec!["GET"], vec![])]);

        assert_eq!(
            first_match(&config, "https://api.example", &Method::GET, &[])
                .unwrap()
                .allow_origin,
            AllowOrigin::Echo
        );
        assert!(first_match(&config, "https://api.example.evil", &Method::GET, &[]).is_none());
        assert!(first_match(&config, "https://example", &Method::GET, &[]).is_none());
    }

    #[test]
    fn exact_wildcard_origin_returns_any() {
        let config = config(vec![rule(vec!["*"], vec!["GET"], vec![])]);

        assert_eq!(
            first_match(&config, "https://any.example", &Method::GET, &[])
                .unwrap()
                .allow_origin,
            AllowOrigin::Any
        );
    }

    #[test]
    fn origin_comparison_is_byte_and_case_sensitive() {
        let config = config(vec![rule(vec!["https://App.example"], vec!["GET"], vec![])]);

        assert!(first_match(&config, "https://app.example", &Method::GET, &[]).is_none());
        assert!(first_match(&config, "https://App.example/", &Method::GET, &[]).is_none());
    }

    #[test]
    fn methods_are_exact_and_uppercase() {
        let config = config(vec![rule(vec!["https://app.example"], vec!["GET"], vec![])]);

        assert!(first_match(&config, "https://app.example", &Method::GET, &[]).is_some());
        let lowercase = Method::from_bytes(b"get").unwrap();
        assert!(first_match(&config, "https://app.example", &lowercase, &[]).is_none());
    }

    #[test]
    fn exact_and_partial_header_patterns_are_ascii_case_insensitive() {
        let exact = config(vec![rule(
            vec!["https://app.example"],
            vec!["GET"],
            vec!["X-Request-Id"],
        )]);
        assert!(
            first_match(
                &exact,
                "https://app.example",
                &Method::GET,
                &headers(&["x-request-id"]),
            )
            .is_some()
        );

        let partial = config(vec![rule(
            vec!["https://app.example"],
            vec!["GET"],
            vec!["X-*-Id"],
        )]);
        assert!(
            first_match(
                &partial,
                "https://app.example",
                &Method::GET,
                &headers(&["x-request-id"]),
            )
            .is_some()
        );
    }

    #[test]
    fn every_requested_header_must_match_the_same_rule() {
        let config = config(vec![rule(
            vec!["https://app.example"],
            vec!["GET"],
            vec!["x-request-id"],
        )]);

        assert!(
            first_match(
                &config,
                "https://app.example",
                &Method::GET,
                &headers(&["x-request-id", "x-client-id"]),
            )
            .is_none()
        );
    }

    #[test]
    fn first_matching_rule_wins_without_combining_later_rules() {
        let config = config(vec![
            rule(
                vec!["https://app.example"],
                vec!["GET"],
                vec!["x-request-id"],
            ),
            rule(vec!["*"], vec!["GET"], vec!["x-client-id"]),
        ]);

        let matched = first_match(
            &config,
            "https://app.example",
            &Method::GET,
            &headers(&["x-request-id"]),
        )
        .unwrap();
        assert!(std::ptr::eq(matched.rule, &config.rules[0]));
        assert_eq!(matched.allow_origin, AllowOrigin::Echo);
        assert!(
            first_match(
                &config,
                "https://app.example",
                &Method::GET,
                &headers(&["x-request-id", "x-client-id"]),
            )
            .is_none()
        );
    }

    #[test]
    fn origin_list_order_selects_the_first_matching_pattern_and_preserves_duplicates() {
        let wildcard_first = config(vec![rule(
            vec!["*", "https://api.example", "https://api.example"],
            vec!["GET"],
            vec![],
        )]);
        assert_eq!(wildcard_first.rules[0].allowed_origins.len(), 3);
        assert_eq!(
            first_match(&wildcard_first, "https://api.example", &Method::GET, &[])
                .unwrap()
                .allow_origin,
            AllowOrigin::Any
        );

        let exact_first = config(vec![rule(
            vec!["https://api.example", "*", "https://api.example"],
            vec!["GET"],
            vec![],
        )]);
        assert_eq!(
            first_match(&exact_first, "https://api.example", &Method::GET, &[])
                .unwrap()
                .allow_origin,
            AllowOrigin::Echo
        );
    }
}
