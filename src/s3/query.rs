use http::Uri;
use s3s::S3Result;

fn decode_component(raw: &str) -> S3Result<String> {
    percent_encoding::percent_decode_str(&raw.replace('+', " "))
        .decode_utf8()
        .map(std::borrow::Cow::into_owned)
        .map_err(|_| s3s::s3_error!(InvalidArgument, "query component is not valid UTF-8"))
}

/// Return form-style percent-decoded query name/value pairs in wire order.
///
/// Components without `=` have an empty value. Raw `+` decodes as a space to
/// match s3s's SigV4 canonical-query semantics; `%2B` remains a literal plus.
pub fn decoded_query_pairs(uri: &Uri) -> S3Result<Vec<(String, String)>> {
    uri.query()
        .unwrap_or("")
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (name, value) = part.split_once('=').unwrap_or((part, ""));
            Ok((decode_component(name)?, decode_component(value)?))
        })
        .collect()
}

/// Check whether a decoded query parameter name is present without validating
/// its value. This is only used for custom-route selection.
pub fn query_key_is_present(uri: &Uri, expected: &str) -> bool {
    uri.query()
        .unwrap_or("")
        .split('&')
        .filter(|part| !part.is_empty())
        .any(|part| {
            let raw_name = part.split_once('=').map_or(part, |(name, _)| name);
            decode_component(raw_name).is_ok_and(|name| name == expected)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decoded_pairs_decode_slashes_apply_form_plus_semantics_and_keep_empty_values() {
        let uri =
            "/bucket/key?decompress-zip=prefix%2Fnested%2F&empty=&uploads&space=a+b&literal=a%2Bb"
                .parse::<Uri>()
                .unwrap();

        let pairs = decoded_query_pairs(&uri).unwrap();

        assert_eq!(
            pairs,
            vec![
                ("decompress-zip".to_string(), "prefix/nested/".to_string()),
                ("empty".to_string(), String::new()),
                ("uploads".to_string(), String::new()),
                ("space".to_string(), "a b".to_string()),
                ("literal".to_string(), "a+b".to_string()),
            ]
        );
    }

    #[test]
    fn invalid_query_utf8_is_invalid_argument_but_route_key_is_still_detectable() {
        let uri = "/bucket/key?decompress-zip=%FF".parse::<Uri>().unwrap();

        assert!(query_key_is_present(&uri, "decompress-zip"));
        assert_eq!(
            decoded_query_pairs(&uri).unwrap_err().code().as_str(),
            "InvalidArgument"
        );
    }

    #[test]
    fn query_key_presence_decodes_names_without_decoding_values() {
        let uri = "/bucket/key?decompress%2Dzip=%FF".parse::<Uri>().unwrap();

        assert!(query_key_is_present(&uri, "decompress-zip"));
        assert_eq!(
            decoded_query_pairs(&uri).unwrap_err().code().as_str(),
            "InvalidArgument"
        );
    }

    #[test]
    fn query_key_presence_uses_form_semantics_for_names() {
        let uri = "/bucket/key?custom+name=value".parse::<Uri>().unwrap();

        assert!(query_key_is_present(&uri, "custom name"));
        assert!(!query_key_is_present(&uri, "custom+name"));
    }
}
