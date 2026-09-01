use http::HeaderName;
use s3s::dto::{CORSConfiguration, CORSRule};

use crate::{
    cors::{
        MAX_CORS_CONFIGURATION_BYTES,
        model::{CorsConfiguration as CanonicalCorsConfiguration, CorsRule as CanonicalCorsRule},
    },
    error::{AppError, AppResult},
};

pub fn validate_and_canonicalize(
    input: CORSConfiguration,
) -> AppResult<CanonicalCorsConfiguration> {
    let config = CanonicalCorsConfiguration {
        rules: input
            .cors_rules
            .into_iter()
            .map(|rule| CanonicalCorsRule {
                allowed_origins: rule.allowed_origins,
                allowed_methods: rule.allowed_methods,
                allowed_headers: rule.allowed_headers.unwrap_or_default(),
                expose_headers: rule.expose_headers.unwrap_or_default(),
                id: rule.id,
                max_age_seconds: rule.max_age_seconds,
            })
            .collect(),
    };
    validate_model(&config)?;
    Ok(config)
}

pub fn canonical_json(config: &CanonicalCorsConfiguration) -> AppResult<String> {
    validate_model(config)?;
    let json = serde_json::to_string(config).map_err(|_| AppError::InvalidCorsConfiguration)?;
    (json.len() <= MAX_CORS_CONFIGURATION_BYTES)
        .then_some(json)
        .ok_or(AppError::InvalidCorsConfiguration)
}

pub fn from_canonical_json(raw: &str) -> AppResult<CanonicalCorsConfiguration> {
    let config = serde_json::from_str(raw).map_err(|_| AppError::CorruptCorsConfiguration)?;
    validate_model(&config).map_err(|_| AppError::CorruptCorsConfiguration)?;
    let json = serde_json::to_string(&config).map_err(|_| AppError::CorruptCorsConfiguration)?;
    (json.len() <= MAX_CORS_CONFIGURATION_BYTES)
        .then_some(config)
        .ok_or(AppError::CorruptCorsConfiguration)
}

pub fn to_s3_configuration(config: &CanonicalCorsConfiguration) -> CORSConfiguration {
    if canonical_json(config).is_err() {
        return CORSConfiguration { cors_rules: vec![] };
    }

    CORSConfiguration {
        cors_rules: config
            .rules
            .iter()
            .map(|rule| CORSRule {
                allowed_headers: (!rule.allowed_headers.is_empty())
                    .then(|| rule.allowed_headers.clone()),
                allowed_methods: rule.allowed_methods.clone(),
                allowed_origins: rule.allowed_origins.clone(),
                expose_headers: (!rule.expose_headers.is_empty())
                    .then(|| rule.expose_headers.clone()),
                id: rule.id.clone(),
                max_age_seconds: rule.max_age_seconds,
            })
            .collect(),
    }
}

fn validate_model(config: &CanonicalCorsConfiguration) -> AppResult<()> {
    if !(1..=100).contains(&config.rules.len()) {
        return Err(AppError::InvalidCorsConfiguration);
    }

    for rule in &config.rules {
        if rule.allowed_origins.is_empty() || rule.allowed_methods.is_empty() {
            return Err(AppError::InvalidCorsConfiguration);
        }
        if rule.id.as_ref().is_some_and(|id| id.chars().count() > 255)
            || rule.max_age_seconds.is_some_and(|max_age| max_age < 0)
        {
            return Err(AppError::InvalidCorsConfiguration);
        }
        for origin in &rule.allowed_origins {
            validate_pattern(origin)?;
        }
        for method in &rule.allowed_methods {
            validate_method(method)?;
        }
        for header in &rule.allowed_headers {
            validate_pattern(header)?;
        }
        for header in &rule.expose_headers {
            if HeaderName::from_bytes(header.as_bytes()).is_err() {
                return Err(AppError::InvalidCorsConfiguration);
            }
        }
    }

    Ok(())
}

fn validate_pattern(value: &str) -> AppResult<()> {
    (!value.is_empty()
        && value
            .chars()
            .all(|ch| !ch.is_control() && !ch.is_whitespace())
        && value.chars().filter(|ch| *ch == '*').count() <= 1)
        .then_some(())
        .ok_or(AppError::InvalidCorsConfiguration)
}

fn validate_method(value: &str) -> AppResult<()> {
    matches!(value, "GET" | "PUT" | "HEAD" | "POST" | "DELETE")
        .then_some(())
        .ok_or(AppError::InvalidCorsConfiguration)
}

#[cfg(test)]
mod tests {
    use s3s::dto::{CORSConfiguration, CORSRule};

    use super::{
        canonical_json, from_canonical_json, to_s3_configuration, validate_and_canonicalize,
    };
    use crate::{
        cors::{
            MAX_CORS_CONFIGURATION_BYTES,
            model::{CorsConfiguration, CorsRule},
        },
        error::AppError,
    };

    fn rule(origins: Vec<&str>, methods: Vec<&str>) -> CORSRule {
        CORSRule {
            allowed_headers: Some(vec!["x-request-id".to_owned()]),
            allowed_methods: methods.into_iter().map(str::to_owned).collect(),
            allowed_origins: origins.into_iter().map(str::to_owned).collect(),
            expose_headers: Some(vec!["x-response-id".to_owned()]),
            id: Some("rule-id".to_owned()),
            max_age_seconds: Some(0),
        }
    }

    fn input(cors_rules: Vec<CORSRule>) -> CORSConfiguration {
        CORSConfiguration { cors_rules }
    }

    fn assert_invalid(input: CORSConfiguration) {
        assert!(matches!(
            validate_and_canonicalize(input),
            Err(AppError::InvalidCorsConfiguration)
        ));
    }

    #[test]
    fn requires_between_one_and_one_hundred_rules() {
        assert_invalid(input(vec![]));
        assert!(
            validate_and_canonicalize(input(vec![rule(vec!["https://one.example"], vec!["GET"],)]))
                .is_ok()
        );

        let hundred = (0..100)
            .map(|_| rule(vec!["https://one.example"], vec!["GET"]))
            .collect();
        assert!(validate_and_canonicalize(input(hundred)).is_ok());

        let hundred_and_one = (0..101)
            .map(|_| rule(vec!["https://one.example"], vec!["GET"]))
            .collect();
        assert_invalid(input(hundred_and_one));
    }

    #[test]
    fn requires_origins_and_methods() {
        assert_invalid(input(vec![rule(vec![], vec!["GET"])]));
        assert_invalid(input(vec![rule(vec!["https://one.example"], vec![])]));
    }

    #[test]
    fn accepts_only_documented_uppercase_methods() {
        for method in ["GET", "PUT", "HEAD", "POST", "DELETE"] {
            assert!(
                validate_and_canonicalize(input(vec![rule(
                    vec!["https://one.example"],
                    vec![method],
                )]))
                .is_ok()
            );
        }

        for method in ["*", "PATCH", "get"] {
            assert_invalid(input(vec![rule(vec!["https://one.example"], vec![method])]));
        }
    }

    #[test]
    fn validates_id_and_max_age_bounds() {
        let mut accepted = rule(vec!["https://one.example"], vec!["GET"]);
        accepted.id = Some("a".repeat(255));
        accepted.max_age_seconds = Some(0);
        assert!(validate_and_canonicalize(input(vec![accepted])).is_ok());

        let mut oversized_id = rule(vec!["https://one.example"], vec!["GET"]);
        oversized_id.id = Some("a".repeat(256));
        assert_invalid(input(vec![oversized_id]));

        let mut negative_max_age = rule(vec!["https://one.example"], vec!["GET"]);
        negative_max_age.max_age_seconds = Some(-1);
        assert_invalid(input(vec![negative_max_age]));
    }

    #[test]
    fn validates_origin_and_allowed_header_patterns() {
        for pattern in [
            "",
            "https://a example",
            "https://a.example\n",
            "https://a.example\u{1f}",
            "*a*",
        ] {
            assert_invalid(input(vec![rule(vec![pattern], vec!["GET"])]));
        }
        assert!(
            validate_and_canonicalize(input(vec![rule(vec!["https://*.example"], vec!["GET"],)]))
                .is_ok()
        );

        for pattern in [
            "",
            "x request",
            "x-request\n",
            "x-request\u{1f}",
            "x**request",
        ] {
            let mut invalid = rule(vec!["https://one.example"], vec!["GET"]);
            invalid.allowed_headers = Some(vec![pattern.to_owned()]);
            assert_invalid(input(vec![invalid]));
        }
        let mut accepted = rule(vec!["https://one.example"], vec!["GET"]);
        accepted.allowed_headers = Some(vec!["x-*-id".to_owned()]);
        assert!(validate_and_canonicalize(input(vec![accepted])).is_ok());
    }

    #[test]
    fn validates_exposed_headers_as_http_names() {
        let mut invalid = rule(vec!["https://one.example"], vec!["GET"]);
        invalid.expose_headers = Some(vec!["bad header".to_owned()]);
        assert_invalid(input(vec![invalid]));

        let mut accepted = rule(vec!["https://one.example"], vec!["GET"]);
        accepted.expose_headers = Some(vec!["x-response-id".to_owned()]);
        assert!(validate_and_canonicalize(input(vec![accepted])).is_ok());
    }

    #[test]
    fn canonical_json_enforces_the_exact_size_boundary() {
        for (size, accepted) in [
            (MAX_CORS_CONFIGURATION_BYTES, true),
            (MAX_CORS_CONFIGURATION_BYTES + 1, false),
        ] {
            let mut config = CorsConfiguration {
                rules: vec![CorsRule {
                    allowed_origins: vec!["https://".to_owned()],
                    allowed_methods: vec!["GET".to_owned()],
                    allowed_headers: vec![],
                    expose_headers: vec![],
                    id: None,
                    max_age_seconds: None,
                }],
            };
            let base_size = serde_json::to_string(&config).unwrap().len();
            config.rules[0].allowed_origins[0].push_str(&"a".repeat(size - base_size));

            let result = canonical_json(&config);
            assert_eq!(result.is_ok(), accepted);
            if let Ok(json) = result {
                assert_eq!(json.len(), size);
            }
        }
    }

    #[test]
    fn canonical_round_trip_preserves_rule_list_and_duplicate_order() {
        let mut first = rule(
            vec!["https://a.example", "https://a.example"],
            vec!["GET", "GET"],
        );
        first.allowed_headers = Some(vec!["X-Request-Id".to_owned(), "X-Request-Id".to_owned()]);
        first.expose_headers = Some(vec!["X-Response-Id".to_owned(), "X-Response-Id".to_owned()]);
        let canonical = validate_and_canonicalize(input(vec![
            first,
            rule(vec!["https://*.example"], vec!["PUT"]),
        ]))
        .unwrap();

        let json = canonical_json(&canonical).unwrap();
        assert_eq!(from_canonical_json(&json).unwrap(), canonical);
        assert_eq!(canonical.rules[0].allowed_origins.len(), 2);
        assert_eq!(canonical.rules[0].allowed_methods.len(), 2);
        assert_eq!(canonical.rules[0].allowed_headers.len(), 2);
        assert_eq!(canonical.rules[0].expose_headers.len(), 2);
        assert_eq!(canonical.rules[1].allowed_methods, vec!["PUT"]);
    }

    #[test]
    fn optional_dto_lists_map_to_empty_and_back_to_none() {
        let mut source = rule(vec!["https://one.example"], vec!["GET"]);
        source.allowed_headers = None;
        source.expose_headers = None;
        let canonical = validate_and_canonicalize(input(vec![source])).unwrap();

        assert!(canonical.rules[0].allowed_headers.is_empty());
        assert!(canonical.rules[0].expose_headers.is_empty());
        let output = to_s3_configuration(&canonical);
        assert_eq!(output.cors_rules[0].allowed_headers, None);
        assert_eq!(output.cors_rules[0].expose_headers, None);

        let mut empty_lists = rule(vec!["https://one.example"], vec!["GET"]);
        empty_lists.allowed_headers = Some(vec![]);
        empty_lists.expose_headers = Some(vec![]);
        let empty_canonical = validate_and_canonicalize(input(vec![empty_lists])).unwrap();
        let empty_output = to_s3_configuration(&empty_canonical);
        assert_eq!(empty_output.cors_rules[0].allowed_headers, None);
        assert_eq!(empty_output.cors_rules[0].expose_headers, None);
    }

    #[test]
    fn stored_json_syntax_and_semantic_tampering_are_corruption() {
        for raw in [
            "not json",
            r#"{"rules":[]}"#,
            r#"{"rules":[{"allowed_origins":["https://one.example"],"allowed_methods":["get"],"allowed_headers":[],"expose_headers":[],"id":null,"max_age_seconds":null}]}"#,
        ] {
            assert!(matches!(
                from_canonical_json(raw),
                Err(AppError::CorruptCorsConfiguration)
            ));
        }
    }

    #[test]
    fn projection_fails_closed_for_an_invalid_in_memory_configuration() {
        let output = to_s3_configuration(&CorsConfiguration { rules: vec![] });
        assert!(output.cors_rules.is_empty());
    }
}
