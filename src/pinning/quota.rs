use chrono::{DateTime, Utc};

use crate::{
    error::{AppError, AppResult},
    pinning::config::ProviderLimits,
};

pub type DateTimeUtc = DateTime<Utc>;

/// Locally reserved provider capacity for distinct CIDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderUsage {
    pub reserved_bytes: i64,
    pub reserved_pins: i64,
}

/// The pure result of attempting to add one previously unreserved CID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacityDecision {
    Reserve,
    Wait,
    Block,
}

/// A capacity-holding CID annotated with its newest active desired-reference touch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvictionCandidate {
    pub provider: String,
    pub cid: String,
    pub cid_size: i64,
    pub last_active_touch: DateTimeUtc,
}

/// Validates an incoming unique-CID reservation against a provider's local limits.
///
/// Arithmetic is checked so corrupt accounting cannot wrap into an apparent capacity grant.
pub fn reservation_decision(
    usage: ProviderUsage,
    cid_size: i64,
    limits: &ProviderLimits,
) -> AppResult<CapacityDecision> {
    validate_usage(usage)?;
    if cid_size < 0 {
        return Err(invalid_quota("CID size cannot be negative"));
    }
    if limits.max_bytes < 0 || limits.max_pins < 0 {
        return Err(invalid_quota("provider limits cannot be negative"));
    }
    if cid_size > limits.max_bytes {
        return Ok(CapacityDecision::Block);
    }

    let next_bytes = usage
        .reserved_bytes
        .checked_add(cid_size)
        .ok_or_else(|| invalid_quota("reserved byte accounting overflow"))?;
    let next_pins = usage
        .reserved_pins
        .checked_add(1)
        .ok_or_else(|| invalid_quota("reserved pin accounting overflow"))?;
    if next_bytes <= limits.max_bytes && next_pins <= limits.max_pins {
        Ok(CapacityDecision::Reserve)
    } else {
        Ok(CapacityDecision::Wait)
    }
}

/// Returns the oldest candidates required to make room, preserving deterministic ties.
///
/// If no oldest prefix can make enough room, nothing is selected: destructive eviction cannot
/// satisfy the incoming reservation and would only reduce unrelated availability.
pub fn select_eviction_candidates(
    usage: ProviderUsage,
    cid_size: i64,
    limits: &ProviderLimits,
    mut candidates: Vec<EvictionCandidate>,
) -> AppResult<Vec<(String, String)>> {
    if reservation_decision(usage, cid_size, limits)? != CapacityDecision::Wait {
        return Ok(Vec::new());
    }
    candidates.sort_by(|left, right| {
        left.last_active_touch
            .cmp(&right.last_active_touch)
            .then_with(|| left.provider.cmp(&right.provider))
            .then_with(|| left.cid.cmp(&right.cid))
    });

    let mut remaining = usage;
    let mut selected = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        if candidate.cid_size < 0 {
            return Err(invalid_quota("remote CID size cannot be negative"));
        }
        remaining.reserved_bytes = remaining
            .reserved_bytes
            .checked_sub(candidate.cid_size)
            .ok_or_else(|| invalid_quota("reserved byte accounting underflow"))?;
        remaining.reserved_pins = remaining
            .reserved_pins
            .checked_sub(1)
            .ok_or_else(|| invalid_quota("reserved pin accounting underflow"))?;
        selected.push((candidate.provider, candidate.cid));
        if reservation_decision(remaining, cid_size, limits)? == CapacityDecision::Reserve {
            return Ok(selected);
        }
    }
    Ok(Vec::new())
}

fn validate_usage(usage: ProviderUsage) -> AppResult<()> {
    if usage.reserved_bytes < 0 || usage.reserved_pins < 0 {
        return Err(invalid_quota("reserved provider usage cannot be negative"));
    }
    Ok(())
}

fn invalid_quota(message: &str) -> AppError {
    AppError::InvalidPinningRequest(message.to_owned())
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};

    use super::{
        CapacityDecision, EvictionCandidate, ProviderUsage, reservation_decision,
        select_eviction_candidates,
    };
    use crate::pinning::config::ProviderLimits;

    fn time(seconds: i64) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 22, 0, 0, 0).single().unwrap() + Duration::seconds(seconds)
    }

    fn limits(max_bytes: i64, max_pins: i64) -> ProviderLimits {
        ProviderLimits {
            priority: 1,
            max_bytes,
            max_pins,
            enabled: true,
        }
    }

    #[test]
    fn oversize_blocks_without_selecting_unrelated_evictions() {
        let limits = limits(100, 2);
        let usage = ProviderUsage {
            reserved_bytes: 80,
            reserved_pins: 2,
        };
        assert_eq!(
            reservation_decision(usage, 101, &limits).unwrap(),
            CapacityDecision::Block
        );
        assert!(
            select_eviction_candidates(
                usage,
                101,
                &limits,
                vec![EvictionCandidate {
                    provider: "pinata".to_owned(),
                    cid: "bafy-old".to_owned(),
                    cid_size: 40,
                    last_active_touch: time(1),
                }],
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn eviction_selection_is_oldest_then_cid_and_stops_at_projected_headroom() {
        let selected = select_eviction_candidates(
            ProviderUsage {
                reserved_bytes: 100,
                reserved_pins: 3,
            },
            60,
            &limits(100, 3),
            vec![
                EvictionCandidate {
                    provider: "pinata".to_owned(),
                    cid: "bafy-z".to_owned(),
                    cid_size: 30,
                    last_active_touch: time(1),
                },
                EvictionCandidate {
                    provider: "pinata".to_owned(),
                    cid: "bafy-a".to_owned(),
                    cid_size: 20,
                    last_active_touch: time(1),
                },
                EvictionCandidate {
                    provider: "pinata".to_owned(),
                    cid: "bafy-new".to_owned(),
                    cid_size: 50,
                    last_active_touch: time(2),
                },
            ],
        )
        .unwrap();

        assert_eq!(
            selected,
            vec![
                ("pinata".to_owned(), "bafy-a".to_owned()),
                ("pinata".to_owned(), "bafy-z".to_owned()),
                ("pinata".to_owned(), "bafy-new".to_owned()),
            ]
        );
    }

    #[test]
    fn insufficient_oldest_prefix_returns_no_selection() {
        let selected = select_eviction_candidates(
            ProviderUsage {
                reserved_bytes: 100,
                reserved_pins: 2,
            },
            90,
            &limits(100, 2),
            vec![EvictionCandidate {
                provider: "pinata".to_owned(),
                cid: "bafy-only-small-candidate".to_owned(),
                cid_size: 10,
                last_active_touch: time(1),
            }],
        )
        .unwrap();

        assert!(selected.is_empty());
    }
}
