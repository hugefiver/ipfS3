//! Bounded raw JSON decoding and streaming measurement for private records.
use super::*;
mod typed;
pub(super) use typed::{charge_json_owned, charge_json_scan, decode_json, measure};

fn count_nodes(input: &[u8], limit: usize) -> Result<usize> {
    let mut index = 0;
    let mut nodes = 0usize;
    while index < input.len() {
        let byte = input[index];
        index += 1;
        match byte {
            b'}' | b']' | b':' | b',' | b' ' | b'\r' | b'\n' | b'\t' => continue,
            b'"' => {
                while index < input.len() {
                    let byte = input[index];
                    index += 1;
                    if byte == b'\\' {
                        index += 1;
                    } else if byte == b'"' {
                        break;
                    }
                }
            }
            b'{' | b'[' => {}
            _ => {
                while index < input.len()
                    && !matches!(
                        input[index],
                        b',' | b']' | b'}' | b' ' | b'\r' | b'\n' | b'\t'
                    )
                {
                    index += 1;
                }
            }
        }
        nodes = nodes.checked_add(1).ok_or(ManagementError::Budget)?;
        if nodes > limit {
            return Err(ManagementError::Budget);
        }
    }
    Ok(nodes)
}

pub(super) fn serialize_bounded<T: Serialize>(value: &T, limit: usize) -> Result<String> {
    struct Output {
        bytes: Vec<u8>,
        limit: usize,
    }
    impl std::io::Write for Output {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self
                .bytes
                .len()
                .checked_add(bytes.len())
                .is_none_or(|n| n > self.limit)
            {
                return Err(std::io::Error::other("private evidence budget"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut output = Output {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut output, value).map_err(|_| ManagementError::Budget)?;
    String::from_utf8(output.bytes).map_err(|_| ManagementError::InvalidEvidence)
}
