//! Lexical comparison of installed predicates, preserving literals and grouping.
//! SQLite exposes CHECK expressions only through the stored table definition.
use super::{ManagementError, Result};

fn tokens(sql: &str, normalize_identifiers: bool) -> Result<Vec<String>> {
    let mut chars = sql.chars().peekable();
    let mut tokens = Vec::new();
    while let Some(c) = chars.next() {
        if c.is_whitespace() {
            continue;
        }
        if c == '-' && chars.peek() == Some(&'-') {
            chars.next();
            for c in chars.by_ref() {
                if c == '\n' {
                    break;
                }
            }
            continue;
        }
        if c == '/' && chars.peek() == Some(&'*') {
            chars.next();
            let mut closed = false;
            while let Some(c) = chars.next() {
                if c == '*' && chars.peek() == Some(&'/') {
                    chars.next();
                    closed = true;
                    break;
                }
            }
            if !closed {
                return Err(ManagementError::Schema);
            }
            continue;
        }
        if matches!(c, '\'' | '"' | '`' | '[') {
            let end = if c == '[' { ']' } else { c };
            let mut value = String::new();
            loop {
                let next = chars.next().ok_or(ManagementError::Schema)?;
                if next == end {
                    if chars.peek() == Some(&end) && c != '[' {
                        value.push(next);
                        chars.next();
                    } else {
                        break;
                    }
                } else {
                    value.push(next);
                }
            }
            tokens.push(if c == '\'' || !normalize_identifiers {
                format!("{c}{}{end}", value.replace(end, &format!("{end}{end}")))
            } else {
                value.to_ascii_lowercase()
            });
        } else if c.is_ascii_alphanumeric() || c == '_' {
            let mut word = c.to_ascii_lowercase().to_string();
            while chars
                .peek()
                .is_some_and(|c| c.is_ascii_alphanumeric() || *c == '_')
            {
                word.push(chars.next().unwrap().to_ascii_lowercase());
            }
            tokens.push(word);
        } else {
            tokens.push(c.to_string());
        }
    }
    Ok(tokens)
}

fn normalized(mut tokens: &[String]) -> Vec<String> {
    while tokens.first().is_some_and(|t| t == "(") && tokens.last().is_some_and(|t| t == ")") {
        let mut depth = 0;
        let wraps_all = tokens.iter().enumerate().all(|(i, token)| {
            if token == "(" {
                depth += 1;
            }
            if token == ")" {
                depth -= 1;
            }
            depth != 0 || i == tokens.len() - 1
        });
        if !wraps_all {
            break;
        }
        tokens = &tokens[1..tokens.len() - 1];
    }
    tokens.to_vec()
}

pub(super) fn canonical(sql: &str) -> Result<Vec<String>> {
    Ok(normalized(&tokens(sql, true)?))
}

pub(super) fn canonical_default(sql: &str) -> Result<Vec<String>> {
    // SQLite accepts double-quoted text as a default; it is not a keyword.
    Ok(normalized(&tokens(sql, false)?))
}

pub(super) fn checks(sql: &str) -> Result<Vec<Vec<String>>> {
    let tokens = tokens(sql, true)?;
    let mut result = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        if tokens[index] != "check" {
            index += 1;
            continue;
        }
        index += 1;
        if tokens.get(index).is_none_or(|t| t != "(") {
            return Err(ManagementError::Schema);
        }
        let start = index;
        let mut depth = 0;
        loop {
            let token = tokens.get(index).ok_or(ManagementError::Schema)?;
            if token == "(" {
                depth += 1;
            }
            if token == ")" {
                depth -= 1;
            }
            index += 1;
            if depth == 0 {
                break;
            }
        }
        result.push(normalized(&tokens[start..index]));
    }
    Ok(result)
}
