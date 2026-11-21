use std::convert::Infallible;
use std::io;
use std::net::Ipv4Addr;
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use futures_util::{FutureExt, stream};
use tokio::time::{advance, timeout};

use super::*;
use crate::config::Config;

const PEER_A: IpAddr = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
const PEER_B: IpAddr = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2));

fn limiter(rps: u32, bytes_per_second: u64) -> RateLimiter {
    let limits = CallerLimits {
        requests_per_second: NonZeroU32::new(rps),
        proxy_download_bytes_per_second: NonZeroU64::new(bytes_per_second),
    };
    RateLimiter::new(RateLimits {
        authenticated: limits,
        anonymous: limits,
        ..Default::default()
    })
    .unwrap()
}

fn body(bytes: &'static [u8]) -> StreamingBlob {
    StreamingBlob::from(s3s::Body::from(Bytes::from_static(bytes)))
}

fn ready_chunk(body: &mut StreamingBlob) -> Bytes {
    body.next()
        .now_or_never()
        .expect("chunk should be ready without advancing time")
        .expect("stream should yield a chunk")
        .unwrap()
}

#[tokio::test(start_paused = true)]
async fn request_burst_is_shared_by_user_and_refills_continuously() {
    let limiter = Arc::new(limiter(2, 0));
    let tasks = [PEER_A, PEER_B, PEER_A].map(|peer| {
        let limiter = Arc::clone(&limiter);
        tokio::spawn(async move { limiter.admit(Some("alice"), Some(peer), false) })
    });
    let mut admitted = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(None) => admitted += 1,
            Ok(Some(_)) => panic!("bandwidth limiting is disabled"),
            Err(error) => {
                assert_eq!(error.code().as_str(), "SlowDown");
                assert_eq!(error.status_code(), Some(StatusCode::SERVICE_UNAVAILABLE));
                assert_eq!(error.headers().unwrap()[RETRY_AFTER], "1");
            }
        }
    }
    assert_eq!(admitted, 2);
    assert!(limiter.admit(Some("bob"), Some(PEER_A), false).is_ok());

    for _ in 0..2 {
        assert!(limiter.admit(None, Some(PEER_A), false).is_ok());
    }
    assert!(limiter.admit(None, Some(PEER_A), false).is_err());
    assert!(limiter.admit(None, Some(PEER_B), false).is_ok());

    advance(Duration::from_millis(499)).await;
    assert!(limiter.admit(Some("alice"), None, false).is_err());
    advance(Duration::from_millis(1)).await;
    assert!(limiter.admit(Some("alice"), None, false).is_ok());
    assert!(limiter.admit(Some("alice"), None, false).is_err());

    advance(Duration::from_secs(2)).await;
    for _ in 0..2 {
        assert!(limiter.admit(Some("alice"), None, false).is_ok());
    }
    assert!(limiter.admit(Some("alice"), None, false).is_err());
}

#[tokio::test(start_paused = true)]
async fn configuring_one_caller_group_leaves_the_other_unlimited() {
    for (group, limited_user, unlimited_user) in [
        ("anonymous", None, Some("alice")),
        ("authenticated", Some("alice"), None),
    ] {
        let config: Config = toml::from_str(&format!(
            "[rate_limits.{group}]\nrequests_per_second = 1\nproxy_download_bytes_per_second = 20"
        ))
        .unwrap();
        let limiter = RateLimiter::new(config.rate_limits).unwrap();

        assert!(
            limiter
                .admit(limited_user, Some(PEER_A), true)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            limiter
                .admit(limited_user, Some(PEER_A), true)
                .err()
                .unwrap()
                .code()
                .as_str(),
            "SlowDown"
        );
        for _ in 0..3 {
            assert!(limiter.admit(unlimited_user, None, true).unwrap().is_none());
        }
    }
}

#[test]
fn disabled_limits_and_missing_anonymous_peer() {
    assert!(RateLimiter::new(RateLimits::default()).is_none());
    let limiter = limiter(0, 20);
    let error = limiter.admit(None, None, true).err().unwrap();
    assert_eq!(error.code().as_str(), "InternalError");
    assert!(limiter.admit(None, Some(PEER_A), false).unwrap().is_none());
    assert!(limiter.admit(Some("alice"), None, false).unwrap().is_none());
    assert!(limiter.admit(None, None, false).unwrap().is_none());
    assert!(limiter.state.lock().unwrap().callers.is_empty());
}

#[tokio::test(start_paused = true)]
async fn shared_download_bucket_charges_actual_chunk_length() {
    let limiter = limiter(0, 100);
    let first = limiter
        .admit(Some("alice"), Some(PEER_A), true)
        .unwrap()
        .unwrap();
    let second = limiter
        .admit(Some("alice"), Some(PEER_B), true)
        .unwrap()
        .unwrap();
    let other = limiter
        .admit(Some("bob"), Some(PEER_A), true)
        .unwrap()
        .unwrap();
    let mut first = first.wrap(body(b"abcd"));
    let mut second = second.wrap(body(b"xy"));
    let mut other = other.wrap(body(b"12345"));

    assert_eq!(ready_chunk(&mut first).as_ref(), b"abcd");
    assert!(second.next().now_or_never().is_none());
    assert_eq!(ready_chunk(&mut other).as_ref(), b"12345");
    let start = Instant::now();
    let chunk = timeout(Duration::from_millis(15), second.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(chunk.as_ref(), b"xy");
    assert!(start.elapsed() >= Duration::from_millis(10));
}

#[tokio::test(start_paused = true)]
async fn caller_groups_use_distinct_download_rates() {
    let config: Config = toml::from_str(
        r#"
        [rate_limits.authenticated]
        proxy_download_bytes_per_second = 100
        [rate_limits.anonymous]
        proxy_download_bytes_per_second = 40
        "#,
    )
    .unwrap();
    let limiter = RateLimiter::new(config.rate_limits).unwrap();
    let [mut signed, mut anonymous] = [Some("alice"), None].map(|user| {
        limiter
            .admit(user, Some(PEER_A), true)
            .unwrap()
            .unwrap()
            .wrap(body(b"abcdef"))
    });

    assert_eq!(ready_chunk(&mut signed).as_ref(), b"abcde");
    assert_eq!(ready_chunk(&mut anonymous).as_ref(), b"ab");
    assert!(signed.next().now_or_never().is_none());
    assert!(anonymous.next().now_or_never().is_none());

    let signed_chunk = timeout(Duration::from_millis(15), signed.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(signed_chunk.as_ref(), b"f");
    assert!(anonymous.next().now_or_never().is_none());

    let anonymous_chunk = timeout(Duration::from_millis(55), anonymous.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(anonymous_chunk.as_ref(), b"cd");
}

#[tokio::test(start_paused = true)]
async fn download_slices_without_copying_or_reading_ahead_and_forwards_errors() {
    let limiter = limiter(0, 100);
    let budget = limiter.admit(None, Some(PEER_A), true).unwrap().unwrap();
    let pulls = Arc::new(AtomicUsize::new(0));
    let witness = Arc::clone(&pulls);
    let bytes = Bytes::from_static(b"abcdefghi");
    let input = bytes.clone();
    let source = async_stream::stream! {
        witness.fetch_add(1, Ordering::Relaxed);
        yield Ok(input);
        witness.fetch_add(1, Ordering::Relaxed);
        yield Err(io::Error::from(io::ErrorKind::ConnectionReset));
    };
    let mut wrapped = budget.wrap(StreamingBlob::wrap(source));
    assert_eq!(pulls.load(Ordering::Relaxed), 0);
    let first = ready_chunk(&mut wrapped);
    assert_eq!(first.as_ref(), b"abcde");
    assert_eq!(first.as_ptr(), bytes.as_ptr());
    assert!(wrapped.next().now_or_never().is_none());
    assert_eq!(pulls.load(Ordering::Relaxed), 1);

    let tail = timeout(Duration::from_millis(45), wrapped.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(tail.as_ref(), b"fghi");
    assert_eq!(tail.as_ptr(), bytes.slice(5..).as_ptr());
    assert_eq!(pulls.load(Ordering::Relaxed), 1);
    let error = wrapped.next().now_or_never().unwrap().unwrap().unwrap_err();
    assert_eq!(
        error.downcast_ref::<io::Error>().unwrap().kind(),
        io::ErrorKind::ConnectionReset
    );
    assert!(wrapped.next().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn high_download_rate_can_refill_multiple_slices_per_millisecond() {
    const RATE: u64 = 256_000_000;
    const SLICE: usize = MAX_SLICE as usize;
    static INPUT: [u8; 2 * SLICE] = [0; 2 * SLICE];
    let limiter = limiter(0, RATE);
    let budget = limiter.admit(None, Some(PEER_A), true).unwrap().unwrap();
    let input = stream::repeat(Ok::<_, Infallible>(Bytes::from_static(&INPUT)));
    let mut wrapped = budget.wrap(StreamingBlob::wrap(input));
    for _ in 0..(RATE / 20 / MAX_SLICE) {
        assert_eq!(ready_chunk(&mut wrapped).len(), SLICE);
    }
    assert!(wrapped.next().now_or_never().is_none());
    advance(Duration::from_millis(2)).await;
    assert_eq!(wrapped.next().await.unwrap().unwrap().len(), SLICE);
    for _ in 0..7 {
        assert_eq!(ready_chunk(&mut wrapped).len(), SLICE);
    }
    assert!(wrapped.next().now_or_never().is_none());
}

#[tokio::test(start_paused = true)]
async fn cancelled_waiter_does_not_reserve_bytes_and_live_waiters_recheck() {
    let limiter = limiter(0, 20);
    let mut waiting = [b"ab".as_slice(), b"c", b"d"].map(|data| {
        limiter
            .admit(None, Some(PEER_A), true)
            .unwrap()
            .unwrap()
            .wrap(body(data))
    });
    assert_eq!(ready_chunk(&mut waiting[0]).as_ref(), b"a");
    for stream in &mut waiting {
        assert!(stream.next().now_or_never().is_none());
    }
    let [cancelled, mut next, mut last] = waiting;
    drop(cancelled);

    let chunk = timeout(Duration::from_millis(75), next.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(chunk.as_ref(), b"c");
    assert!(last.next().now_or_never().is_none());
    let chunk = timeout(Duration::from_millis(75), last.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(chunk.as_ref(), b"d");
}

#[tokio::test(start_paused = true)]
async fn lazy_cleanup_preserves_live_downloads_and_reclaims_expired_callers() {
    let limiter = limiter(0, 1);
    drop(limiter.admit(Some("idle"), None, true).unwrap());
    let mut live = limiter
        .admit(Some("live"), None, true)
        .unwrap()
        .unwrap()
        .wrap(body(b"ab"));

    advance(CALLER_IDLE).await;
    let mut same = limiter
        .admit(Some("live"), None, true)
        .unwrap()
        .unwrap()
        .wrap(body(b"c"));
    assert_eq!(limiter.state.lock().unwrap().callers.len(), 1);
    assert_eq!(ready_chunk(&mut live).as_ref(), b"a");
    assert!(same.next().now_or_never().is_none());
    drop(live);
    drop(same);

    advance(CALLER_IDLE).await;
    let new = limiter.admit(Some("new"), None, true).unwrap().unwrap();
    assert_eq!(limiter.state.lock().unwrap().callers.len(), 1);
    assert_eq!(ready_chunk(&mut new.wrap(body(b"d"))).as_ref(), b"d");
}
