use super::{
    body::ResponseBody,
    error::{error, protocol},
};
use crate::pinning::provider::{ProviderError, ProviderErrorClass};
use bytes::Bytes;
use futures_util::Stream;
use http_body_util::BodyExt as _;
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};
use tokio::sync::watch;
use tokio::time::Instant;

const PENDING: u8 = 0;
const COMPLETE: u8 = 1;
const FAILED: u8 = 2;

#[derive(Clone)]
pub(super) struct Progress(watch::Sender<Instant>);

impl Progress {
    pub fn new() -> Self {
        Self(watch::channel(Instant::now()).0)
    }
    pub fn advance(&self) {
        self.0.send_replace(Instant::now());
    }

    /// No absolute upload timeout and no one-shot reqwest read_timeout. Only
    /// genuine consumed payload/monotonic RPC progress advances the deadline.
    pub async fn run<T>(
        &self,
        idle: Duration,
        future: impl Future<Output = Result<T, ProviderError>>,
    ) -> Result<T, ProviderError> {
        let mut updates = self.0.subscribe();
        tokio::pin!(future);
        loop {
            let deadline = *updates.borrow_and_update() + idle;
            tokio::select! {
                biased;
                result = &mut future => return result,
                _ = updates.changed() => {},
                _ = tokio::time::sleep_until(deadline) => {
                    if Instant::now() >= *updates.borrow() + idle {
                        return Err(error(ProviderErrorClass::Transient, "RPC upload made no progress within idle limit"));
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct Transfer {
    source: Arc<AtomicU8>,
    upload: Arc<AtomicU8>,
    pub progress: Progress,
}

impl Transfer {
    pub fn new() -> Self {
        Self {
            source: Arc::new(AtomicU8::new(PENDING)),
            upload: Arc::new(AtomicU8::new(PENDING)),
            progress: Progress::new(),
        }
    }
    pub fn ensure_complete(&self) -> Result<(), ProviderError> {
        if self.source.load(Ordering::Acquire) != COMPLETE
            || self.upload.load(Ordering::Acquire) != COMPLETE
        {
            return Err(protocol("RPC upload or source did not reach complete EOF"));
        }
        Ok(())
    }

    pub fn source_body(
        &self,
        response: reqwest::Response,
        idle: Duration,
    ) -> Result<reqwest::Body, ProviderError> {
        let mut body = ResponseBody::new(response, None)?;
        let transfer = self.clone();
        let stream = async_stream::stream! {
            loop {
                match tokio::time::timeout(idle, body.next()).await {
                    Ok(Ok(Some(data))) => {
                        transfer.progress.advance();
                        yield Ok::<Bytes, std::io::Error>(data);
                    }
                    Ok(Ok(None)) => {
                        transfer.source.store(COMPLETE, Ordering::Release);
                        transfer.progress.advance();
                        return;
                    }
                    _ => {
                        transfer.source.store(FAILED, Ordering::Release);
                        yield Err(std::io::Error::other("RPC source stream failed"));
                        return;
                    }
                }
            }
        };
        Ok(reqwest::Body::wrap_stream(stream))
    }

    /// Observe the entire multipart body, including its closing boundary. Source
    /// EOF alone is insufficient if the target stops reading an upload early.
    pub fn wrap_request(&self, request: &mut reqwest::Request) -> Result<(), ProviderError> {
        let body = request
            .body_mut()
            .take()
            .ok_or_else(|| protocol("missing RPC upload body"))?;
        *request.body_mut() = Some(reqwest::Body::wrap_stream(self.upload_stream(body)));
        Ok(())
    }

    fn upload_stream(
        &self,
        mut body: reqwest::Body,
    ) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
        let transfer = self.clone();
        async_stream::stream! {
            loop {
                match body.frame().await {
                    Some(Ok(frame)) => match frame.into_data() {
                        Ok(data) => {
                            if !data.is_empty() { transfer.progress.advance(); }
                            yield Ok(data);
                        }
                        Err(_) => {
                            transfer.upload.store(FAILED, Ordering::Release);
                            yield Err(std::io::Error::other("unexpected RPC upload trailer"));
                            return;
                        }
                    },
                    Some(Err(_)) => {
                        transfer.upload.store(FAILED, Ordering::Release);
                        yield Err(std::io::Error::other("RPC upload stream failed"));
                        return;
                    }
                    None => {
                        transfer.upload.store(COMPLETE, Ordering::Release);
                        transfer.progress.advance();
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    #[tokio::test]
    async fn source_eof_does_not_replace_multipart_eof() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/source"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"stored bytes"))
            .mount(&server)
            .await;
        let client = reqwest::Client::new();
        let response = client
            .post(format!("{}/source", server.uri()))
            .send()
            .await
            .unwrap();
        let transfer = Transfer::new();
        let source = transfer
            .source_body(response, Duration::from_secs(1))
            .unwrap();
        let form =
            reqwest::multipart::Form::new().part("file", reqwest::multipart::Part::stream(source));
        let mut request = client
            .post(format!("{}/target", server.uri()))
            .multipart(form)
            .build()
            .unwrap();
        transfer.wrap_request(&mut request).unwrap();
        let body = request.body_mut().as_mut().unwrap();
        assert!(transfer.ensure_complete().is_err());
        while transfer.source.load(Ordering::Acquire) != COMPLETE {
            assert!(body.frame().await.unwrap().is_ok());
        }
        assert_eq!(transfer.upload.load(Ordering::Acquire), PENDING);
        assert!(transfer.ensure_complete().is_err());
        while let Some(frame) = body.frame().await {
            frame.unwrap();
        }
        transfer.ensure_complete().unwrap();
    }
}
