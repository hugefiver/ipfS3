use crate::pinning::{config::SecretToken, psa::PsaClient};

pub const PINATA_BASE_URL: &str = "https://api.pinata.cloud/psa";

pub fn build_pinata(name: String, token: SecretToken, endpoint: Option<String>) -> PsaClient {
    PsaClient::new(
        name,
        endpoint.unwrap_or_else(|| PINATA_BASE_URL.to_owned()),
        token,
    )
}

#[cfg(test)]
mod tests {
    use crate::pinning::pinata::{PINATA_BASE_URL, build_pinata};

    #[test]
    fn pinata_uses_the_psa_default_and_normalizes_an_override() {
        let provider = build_pinata(
            "pinata".to_owned(),
            crate::pinning::psa::test_token("provider-token"),
            Some("http://example.test/psa///".to_owned()),
        );

        assert_eq!(PINATA_BASE_URL, "https://api.pinata.cloud/psa");
        assert_eq!(provider.base_url(), "http://example.test/psa");
    }
}
