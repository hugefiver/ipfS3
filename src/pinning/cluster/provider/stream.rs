use super::{
    ClusterFailureCode as Code, ClusterTransferLimits, ClusterWriteFailure as Failure, car::Car,
    http::Body,
};
use bytes::Bytes;
use http_body_util::BodyExt as _;
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{sync::watch, time::Instant};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(super) struct Progress(watch::Sender<Instant>);
impl Progress {
    pub fn new() -> Self {
        Self(watch::channel(Instant::now()).0)
    }
    pub fn advance(&self) {
        self.0.send_replace(Instant::now());
    }
    pub async fn run<T>(
        &self,
        idle: Duration,
        cancel: &CancellationToken,
        future: impl Future<Output = Result<T, Failure>>,
    ) -> Result<T, Failure> {
        let mut updates = self.0.subscribe();
        tokio::pin!(future);
        loop {
            let deadline = *updates.borrow_and_update() + idle;
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(Failure::new(Code::Cancelled)),
                result = &mut future => return result,
                _ = updates.changed() => {},
                _ = tokio::time::sleep_until(deadline) => {
                    if Instant::now() >= *updates.borrow() + idle { return Err(Failure::new(Code::IdleTimeout)); }
                }
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct Transfer {
    source_eof: Arc<AtomicBool>,
    upload_eof: Arc<AtomicBool>,
    failure: Arc<Mutex<Option<Failure>>>,
    pub progress: Progress,
}
impl Transfer {
    pub fn new() -> Self {
        Self {
            source_eof: Arc::new(AtomicBool::new(false)),
            upload_eof: Arc::new(AtomicBool::new(false)),
            failure: Arc::new(Mutex::new(None)),
            progress: Progress::new(),
        }
    }
    pub fn failure(&self) -> Option<Failure> {
        *self.failure.lock().expect("transfer failure lock poisoned")
    }
    pub fn complete(&self) -> Result<(), Failure> {
        if let Some(failure) = self.failure() {
            return Err(failure);
        }
        if !self.source_eof.load(Ordering::Acquire) || !self.upload_eof.load(Ordering::Acquire) {
            return Err(Failure::new(Code::IncompleteUpload));
        }
        Ok(())
    }
    pub fn source(
        &self,
        response: reqwest::Response,
        limits: ClusterTransferLimits,
        mut car: Option<Car>,
    ) -> Result<reqwest::Body, Failure> {
        let mut body = Body::new(response, limits.max_car_bytes)?;
        let transfer = self.clone();
        // Source is pulled ONLY by the destination multipart body. No producer
        // task, whole-object collection or unbounded queue defeats backpressure.
        let stream = async_stream::stream! {
            loop {
                let next = match tokio::time::timeout(limits.idle_timeout, body.next()).await {
                    Ok(value) => value,
                    Err(_) => Err(Failure::new(Code::IdleTimeout)),
                };
                let next = next.and_then(|data| {
                    if let Some(validator) = &mut car {
                        match &data { Some(bytes) => validator.feed(bytes)?, None => validator.finish()? }
                    }
                    Ok(data)
                });
                match next {
                    Ok(Some(data)) => { transfer.progress.advance(); yield Ok::<Bytes, std::io::Error>(data); },
                    Ok(None) => { transfer.source_eof.store(true, Ordering::Release); transfer.progress.advance(); return; },
                    Err(error) => {
                        *transfer.failure.lock().expect("transfer failure lock poisoned") = Some(error);
                        yield Err(std::io::Error::other("Cluster stored source failed")); return;
                    }
                }
            }
        };
        Ok(reqwest::Body::wrap_stream(stream))
    }
    pub fn wrap(&self, request: &mut reqwest::Request) -> Result<(), Failure> {
        let mut body = request
            .body_mut()
            .take()
            .ok_or_else(|| Failure::new(Code::Protocol))?;
        let transfer = self.clone();
        let stream = async_stream::stream! {
            loop {
                match body.frame().await {
                    Some(Ok(frame)) => match frame.into_data() {
                        Ok(data) => { if !data.is_empty() { transfer.progress.advance(); } yield Ok::<Bytes, std::io::Error>(data); },
                        Err(_) => { yield Err(std::io::Error::other("unexpected upload trailer")); return; },
                    },
                    Some(Err(_)) => { yield Err(std::io::Error::other("Cluster upload failed")); return; },
                    None => { transfer.upload_eof.store(true, Ordering::Release); transfer.progress.advance(); return; },
                }
            }
        };
        *request.body_mut() = Some(reqwest::Body::wrap_stream(stream));
        Ok(())
    }
}
