use glob::Pattern;
use serde::{Deserialize, Deserializer, de::Error};
use url::Url;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DownloadRedirectConfig {
    #[serde(rename = "gateway_url", deserialize_with = "deserialize_gateway_url")]
    gateway_ipfs_prefix: String,
    #[serde(default)]
    rules: Vec<DownloadRedirectRule>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct DownloadRedirectRule {
    #[serde(default)]
    mode: DownloadMode,
    bucket: Option<String>,
    prefix: Option<String>,
    user: Option<String>,
    anonymous: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_filename_glob")]
    filename_glob: Option<Pattern>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DownloadMode {
    #[default]
    Redirect,
    OnRequest,
    Proxy,
}

impl DownloadRedirectConfig {
    pub fn location(
        &self,
        bucket: &str,
        key: &str,
        user: Option<&str>,
        cid: &str,
        requested: bool,
    ) -> Option<String> {
        let filename = key.rsplit_once('/').map_or(key, |(_, filename)| filename);
        let mut redirect = false;
        for rule in self
            .rules
            .iter()
            .filter(|rule| rule.matches(bucket, key, user, filename))
        {
            match rule.mode {
                DownloadMode::Redirect => redirect = true,
                DownloadMode::OnRequest => redirect |= requested,
                DownloadMode::Proxy => return None,
            }
        }
        redirect.then(|| format!("{}{cid}", self.gateway_ipfs_prefix))
    }
}

impl DownloadRedirectRule {
    fn matches(&self, bucket: &str, key: &str, user: Option<&str>, filename: &str) -> bool {
        self.bucket.as_deref().is_none_or(|value| value == bucket)
            && self
                .prefix
                .as_deref()
                .is_none_or(|value| key.starts_with(value))
            && self.user.as_deref().is_none_or(|value| user == Some(value))
            && self.anonymous.is_none_or(|value| user.is_none() == value)
            && self
                .filename_glob
                .as_ref()
                .is_none_or(|pattern| pattern.matches(filename))
    }
}

fn deserialize_filename_glob<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Pattern>, D::Error> {
    let pattern = String::deserialize(deserializer)?;
    Pattern::new(&pattern).map(Some).map_err(D::Error::custom)
}

fn deserialize_gateway_url<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    let raw = String::deserialize(deserializer)?;
    let invalid = || {
        D::Error::custom(
            "gateway_url must be an absolute HTTP(S) URL without credentials, a query, or a fragment",
        )
    };
    let url = Url::parse(&raw).map_err(|_| invalid())?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid());
    }

    Ok(format!("{}/ipfs/", url.as_str().trim_end_matches('/')))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rules_match_all_selectors_within_a_rule_and_any_rule_across_the_list() {
        let config: DownloadRedirectConfig = toml::from_str(
            r#"
                gateway_url = 'https://gateway.example:8443/edge%20cache/site///'
                [[rules]]
                bucket = 'photos'
                prefix = 'public/'
                user = 'writer'
                filename_glob = 'logo-?.png'
                [[rules]]
                bucket = 'public-assets'
                anonymous = true
                [[rules]]
                bucket = 'private-assets'
                anonymous = false
            "#,
        )
        .unwrap();
        let expected = "https://gateway.example:8443/edge%20cache/site/ipfs/bafyexample";
        for (bucket, key, user, redirects) in [
            ("photos", "public/nested/logo-1.png", Some("writer"), true),
            ("public-assets", "logo.png", None, true),
            ("private-assets", "logo.png", Some("reader"), true),
            ("other", "public/logo-1.png", Some("writer"), false),
            ("photos", "publicity/logo-1.png", Some("writer"), false),
            ("photos", "public/logo-1.png", Some("reader"), false),
            ("photos", "public/logo-1.png", None, false),
            ("photos", "public/logo-10.png", Some("writer"), false),
            ("public-assets", "logo.png", Some("reader"), false),
            ("private-assets", "logo.png", None, false),
        ] {
            assert_eq!(
                config
                    .location(bucket, key, user, "bafyexample", false)
                    .as_deref(),
                redirects.then_some(expected),
                "{bucket}/{key} as {user:?}",
            );
        }
    }

    #[test]
    fn proxy_overrides_redirect_modes_regardless_of_rule_order() {
        let redirect = "[[rules]]\nbucket = 'eager'\nmode = 'redirect'\n";
        let on_request = "[[rules]]\nbucket = 'opt-in'\nmode = 'on_request'\n";
        let proxy = "[[rules]]\nprefix = 'private/'\nmode = 'proxy'\n";
        for rules in [[redirect, on_request, proxy], [proxy, on_request, redirect]] {
            let config: DownloadRedirectConfig = toml::from_str(&format!(
                "gateway_url = 'http://gateway.example'\n{}",
                rules.concat(),
            ))
            .unwrap();
            for (bucket, key, requested, redirects) in [
                ("eager", "public/logo.png", false, true),
                ("opt-in", "public/logo.png", false, false),
                ("opt-in", "public/logo.png", true, true),
                ("eager", "private/logo.png", false, false),
                ("opt-in", "private/logo.png", true, false),
            ] {
                assert_eq!(
                    config
                        .location(bucket, key, None, "bafyexample", requested)
                        .is_some(),
                    redirects,
                    "{bucket}/{key} with requested={requested}",
                );
            }
        }
    }

    #[test]
    fn omitted_or_empty_rules_do_not_redirect() {
        for rules in ["", "rules = []"] {
            let config: DownloadRedirectConfig =
                toml::from_str(&format!("gateway_url = 'http://gateway.example'\n{rules}"))
                    .unwrap();
            assert_eq!(
                config.location("photos", "logo.png", None, "bafyexample", true),
                None,
            );
        }
    }

    #[test]
    fn invalid_gateway_urls_fail_during_configuration_read() {
        for url in [
            "gateway.example/ipfs",
            "https://",
            "ftp://gateway.example",
            "https://gateway.example/path?",
            "https://gateway.example/path#fragment",
            "https://user:secret@gateway.example",
        ] {
            assert!(
                toml::from_str::<DownloadRedirectConfig>(&format!("gateway_url = '{url}'"))
                    .is_err(),
                "accepted {url}",
            );
        }
    }

    #[test]
    fn invalid_rules_fail_during_configuration_read() {
        for invalid in [
            "rulse = []",
            "[[rules]]\nprefx = 'public/'",
            "[[rules]]\nmode = 'unknown'",
            "[[rules]]\nfilename_glob = '['",
        ] {
            assert!(
                toml::from_str::<DownloadRedirectConfig>(&format!(
                    "gateway_url = 'https://gateway.example'\n{invalid}"
                ))
                .is_err(),
                "accepted {invalid}",
            );
        }
    }
}
