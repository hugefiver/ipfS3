use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;
use http::{HeaderMap, HeaderValue, StatusCode, header::RETRY_AFTER};
use http_body_util::StreamBody;
use hyper::body::Frame;
use s3s::{S3Error, S3ErrorCode, S3Result, dto::StreamingBlob};
use tokio::time::{Instant, sleep_until};

use crate::config::{CallerLimits, RateLimits};

mod client_ip;

impl CallerLimits {
    fn applies_to(self, get_object: bool) -> bool {
        self.requests_per_second.is_some()
            || (get_object && self.proxy_download_bytes_per_second.is_some())
    }
}

const CALLER_IDLE: Duration = Duration::from_secs(60);
const MAX_SLICE: u64 = 64 * 1024;
const UNITS_PER_TOKEN: u128 = 1_000_000_000;

pub struct RateLimiter {
    config: RateLimits,
    state: Mutex<CallerState>,
}

#[derive(Clone)]
pub(crate) struct DownloadBudget {
    bucket: Arc<Mutex<TokenBucket>>,
    slice_len: usize,
}

#[derive(Eq, Hash, PartialEq)]
enum CallerKey {
    User(String),
    Peer(IpAddr),
}

struct CallerState {
    callers: HashMap<CallerKey, Caller>,
    next_cleanup: Instant,
}

struct Caller {
    requests: Option<TokenBucket>,
    download: Option<DownloadBudget>,
    last_request: Instant,
}

struct TokenBucket {
    rate: u128,
    capacity: u128,
    available: u128,
    updated_at: Instant,
}

impl RateLimiter {
    pub fn new(config: RateLimits) -> Option<Self> {
        if !config.anonymous.applies_to(true) && !config.authenticated.applies_to(true) {
            return None;
        }
        Some(Self {
            config,
            state: Mutex::new(CallerState {
                callers: HashMap::new(),
                next_cleanup: Instant::now() + CALLER_IDLE,
            }),
        })
    }

    pub(crate) fn admit(
        &self,
        user: Option<&str>,
        peer: Option<IpAddr>,
        get_object: bool,
    ) -> S3Result<Option<DownloadBudget>> {
        let limits = if user.is_some() {
            self.config.authenticated
        } else {
            self.config.anonymous
        };
        if !limits.applies_to(get_object) {
            return Ok(None);
        }
        let key = match (user, peer) {
            (Some(user), _) => CallerKey::User(user.into()),
            (None, Some(peer)) => CallerKey::Peer(peer.to_canonical()),
            (None, None) => {
                tracing::error!("anonymous S3 request is missing its peer IP for rate limiting");
                return Err(S3Error::with_message(
                    S3ErrorCode::InternalError,
                    "internal error",
                ));
            }
        };
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        if now >= state.next_cleanup {
            state.callers.retain(|_, caller| !caller.is_idle(now));
            state.next_cleanup = now + CALLER_IDLE;
        }
        let caller = state.callers.entry(key).or_insert_with(|| Caller {
            requests: limits.requests_per_second.map(|rate| {
                let rate = u64::from(rate.get());
                TokenBucket::new(rate, rate, now)
            }),
            download: None,
            last_request: now,
        });
        caller.last_request = now;
        if let Some(requests) = &mut caller.requests
            && requests.try_take(1, now).is_err()
        {
            drop(state);
            let mut error = S3Error::with_message(S3ErrorCode::SlowDown, "request rate exceeded");
            error.set_status_code(StatusCode::SERVICE_UNAVAILABLE);
            let mut headers = HeaderMap::new();
            headers.insert(RETRY_AFTER, HeaderValue::from_static("1"));
            error.set_headers(headers);
            return Err(error);
        }
        if get_object && let Some(rate) = limits.proxy_download_bytes_per_second {
            return Ok(Some(
                caller
                    .download
                    .get_or_insert_with(|| DownloadBudget::new(rate.get(), now))
                    .clone(),
            ));
        }
        Ok(None)
    }
}

impl Caller {
    fn is_idle(&self, now: Instant) -> bool {
        now - self.last_request >= CALLER_IDLE
            && self.download.as_ref().is_none_or(|download| {
                Arc::strong_count(&download.bucket) == 1
                    && now.saturating_duration_since(download.bucket.lock().unwrap().updated_at)
                        >= CALLER_IDLE
            })
    }
}

impl DownloadBudget {
    fn new(rate: u64, now: Instant) -> Self {
        let capacity = (rate / 20).max(1);
        Self {
            bucket: Arc::new(Mutex::new(TokenBucket::new(rate, capacity, now))),
            slice_len: capacity.min(MAX_SLICE) as usize,
        }
    }

    pub(crate) fn wrap(&self, mut body: StreamingBlob) -> StreamingBlob {
        let budget = self.clone();
        let stream = async_stream::stream! {
            while let Some(chunk) = body.next().await {
                let mut bytes = match chunk {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                };
                while !bytes.is_empty() {
                    let len = bytes.len().min(budget.slice_len);
                    budget.take(len).await;
                    yield Ok(Frame::data(bytes.split_to(len)));
                }
            }
        };
        StreamingBlob::from(s3s::Body::http_body(StreamBody::new(stream)))
    }

    async fn take(&self, len: usize) {
        loop {
            let deadline = {
                let mut bucket = self.bucket.lock().unwrap();
                let now = Instant::now();
                match bucket.try_take(len as u64, now) {
                    Ok(()) => return,
                    Err(deficit) => {
                        now + Duration::from_nanos(deficit.div_ceil(bucket.rate) as u64)
                    }
                }
            };
            sleep_until(deadline).await;
        }
    }
}

impl TokenBucket {
    fn new(rate: u64, capacity: u64, now: Instant) -> Self {
        let capacity = u128::from(capacity) * UNITS_PER_TOKEN;
        Self {
            rate: u128::from(rate),
            capacity,
            available: capacity,
            updated_at: now,
        }
    }

    fn try_take(&mut self, count: u64, now: Instant) -> Result<(), u128> {
        // Fixed-point units retain fractional tokens; every bucket refills in at most one second.
        let elapsed = (now - self.updated_at).as_nanos().min(UNITS_PER_TOKEN);
        self.available = self.capacity.min(self.available + elapsed * self.rate);
        self.updated_at = now;
        let cost = u128::from(count) * UNITS_PER_TOKEN;
        if self.available < cost {
            return Err(cost - self.available);
        }
        self.available -= cost;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
