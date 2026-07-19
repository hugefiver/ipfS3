use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SanitizedEntry {
    File { key: String },
    Directory,
}

fn has_windows_drive(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

fn validation_error(value: &str, parameter: bool) -> AppError {
    if parameter {
        AppError::InvalidZipParameter(value.to_owned())
    } else {
        AppError::ZipSlip(value.to_owned())
    }
}

fn validate_segments(value: &str, parameter: bool) -> AppResult<()> {
    if value.contains('\\') || value.starts_with('/') || has_windows_drive(value) {
        return Err(validation_error(value, parameter));
    }

    for segment in value.split('/') {
        if segment == "." || segment == ".." {
            return Err(validation_error(value, parameter));
        }
    }

    Ok(())
}

pub fn normalize_target_prefix(prefix: &str) -> AppResult<String> {
    if prefix.is_empty() {
        return Ok(String::new());
    }

    validate_segments(prefix, true)?;
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        return Err(AppError::InvalidZipParameter(prefix.to_owned()));
    }

    Ok(format!("{trimmed}/"))
}

pub fn sanitize_entry(name: &str, target_prefix: &str) -> AppResult<SanitizedEntry> {
    if name.is_empty() {
        return Err(AppError::InvalidZipEntry("empty entry name".to_string()));
    }

    validate_segments(name, false)?;
    let is_directory = name.ends_with('/');
    let trimmed = name.trim_matches('/');
    if trimmed.is_empty() {
        return Err(AppError::InvalidZipEntry(name.to_owned()));
    }

    if is_directory {
        return Ok(SanitizedEntry::Directory);
    }

    let key = format!("{target_prefix}{trimmed}");
    if !target_prefix.is_empty() && !key.starts_with(target_prefix) {
        return Err(AppError::ZipSlip(name.to_owned()));
    }

    Ok(SanitizedEntry::File { key })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_prefix_adds_trailing_slash() {
        assert_eq!(normalize_target_prefix("prefix").unwrap(), "prefix/");
        assert_eq!(normalize_target_prefix("prefix/").unwrap(), "prefix/");
        assert_eq!(normalize_target_prefix("").unwrap(), "");
    }

    #[test]
    fn normalize_prefix_rejects_escape_segments() {
        for prefix in ["../x", "/abs", "C:/x", "dir\\x"] {
            assert!(matches!(
                normalize_target_prefix(prefix),
                Err(AppError::InvalidZipParameter(_))
            ));
        }
    }

    #[test]
    fn sanitize_safe_file_under_prefix() {
        assert_eq!(
            sanitize_entry("foo/bar.txt", "prefix/").unwrap(),
            SanitizedEntry::File {
                key: "prefix/foo/bar.txt".to_string()
            }
        );
    }

    #[test]
    fn sanitize_safe_directory_skips_it() {
        assert_eq!(
            sanitize_entry("foo/", "prefix/").unwrap(),
            SanitizedEntry::Directory
        );
    }

    #[test]
    fn sanitize_rejects_unsafe_names() {
        assert!(matches!(
            sanitize_entry("", "prefix/"),
            Err(AppError::InvalidZipEntry(_))
        ));

        for name in [
            "../escape.txt",
            "/etc/passwd",
            "C:/Windows/x",
            "dir\\file.txt",
            "a/./b",
            "a/../b",
        ] {
            assert!(matches!(
                sanitize_entry(name, "prefix/"),
                Err(AppError::ZipSlip(_))
            ));
        }
    }
}
