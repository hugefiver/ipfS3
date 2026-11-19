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
    use super::build_filebase;

    #[test]
    fn filebase_uses_the_psa_default_and_normalizes_an_override() {
        let token = crate::pinning::psa::test_token("provider-token");
        let default_provider = build_filebase("filebase".to_owned(), token.clone(), None);
        let custom_provider = build_filebase(
            "filebase".to_owned(),
            token,
            Some("http://example.test/v1/ipfs///".to_owned()),
        );

        assert_eq!(
            default_provider.base_url(),
            "https://api.filebase.io/v1/ipfs"
        );
        assert_eq!(custom_provider.base_url(), "http://example.test/v1/ipfs");
    }
}
