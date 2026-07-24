use crate::pinning::{config::SecretToken, psa::PsaClient};

pub const FILEBASE_BASE_URL: &str = "https://api.filebase.io/v1/ipfs";

pub fn build_filebase(name: String, token: SecretToken, endpoint: Option<String>) -> PsaClient {
    PsaClient::new(
        name,
        endpoint.unwrap_or_else(|| FILEBASE_BASE_URL.to_owned()),
        token,
    )
}

#[cfg(test)]
mod tests {
    use crate::pinning::filebase::{FILEBASE_BASE_URL, build_filebase};

    #[test]
    fn filebase_uses_the_psa_default_and_normalizes_an_override() {
        let provider = build_filebase(
            "filebase".to_owned(),
            crate::pinning::psa::test_token("provider-token"),
            Some("http://example.test/v1/ipfs///".to_owned()),
        );

        assert_eq!(FILEBASE_BASE_URL, "https://api.filebase.io/v1/ipfs");
        assert_eq!(provider.base_url(), "http://example.test/v1/ipfs");
    }
}
