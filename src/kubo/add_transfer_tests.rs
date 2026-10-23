use super::*;

#[tokio::test]
async fn producer_hands_off_shared_bytes_with_bounded_backpressure() {
    let input = Bytes::from(vec![b'x'; ADD_CHUNK_BYTES * 3 + 7]);
    let original = input.clone();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let cancel = CancellationToken::new();
    let producer = pump_source(
        futures_util::stream::iter([Ok::<_, std::io::Error>(input)]),
        sender,
        cancel,
    );
    tokio::pin!(producer);
    // A full handoff slot must stop the producer even for an immediately ready source.
    assert!(futures_util::poll!(&mut producer).is_pending());
    assert_eq!(receiver.len(), 1);
    let consume = async {
        let mut offset = 0;
        while let Some(frame) = receiver.recv().await {
            assert!(frame.len() <= ADD_CHUNK_BYTES);
            assert_eq!(frame.as_ptr(), original[offset..].as_ptr());
            assert_eq!(&frame[..], &original[offset..offset + frame.len()]);
            offset += frame.len();
        }
        assert_eq!(offset, original.len());
    };
    let (outcome, ()) = tokio::join!(producer, consume);
    assert!(matches!(outcome, ProducerOutcome::Complete));
}

#[tokio::test]
async fn producer_cancellation_interrupts_a_full_handoff_slot() {
    let (sender, _receiver) = tokio::sync::mpsc::channel(1);
    let cancel = CancellationToken::new();
    let input = Bytes::from(vec![0; ADD_CHUNK_BYTES * 3]);
    let producer = pump_source(
        futures_util::stream::iter([Ok::<_, std::io::Error>(input)]),
        sender,
        cancel.clone(),
    );
    tokio::pin!(producer);
    assert!(futures_util::poll!(&mut producer).is_pending());
    cancel.cancel();
    assert!(matches!(producer.await, ProducerOutcome::Canceled));
}

#[tokio::test]
async fn producer_observes_receiver_closure_without_consuming_the_whole_source() {
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    drop(receiver);
    let consumed = std::sync::atomic::AtomicUsize::new(0);
    let source = futures_util::stream::iter((0..3).map(|_| {
        consumed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok::<_, std::io::Error>(Bytes::from_static(b"payload"))
    }));
    let outcome = pump_source(source, sender, CancellationToken::new()).await;
    assert!(matches!(outcome, ProducerOutcome::ReceiverClosed));
    assert_eq!(consumed.load(std::sync::atomic::Ordering::Relaxed), 1);
}

#[tokio::test]
async fn complete_early_root_cannot_hide_a_late_source_error() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = KuboClient::new(format!("http://{}", listener.local_addr().unwrap()));
    let (root_sent, root_received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut headers = Vec::new();
        while !headers.ends_with(b"\r\n\r\n") {
            headers.push(socket.read_u8().await.unwrap());
        }
        let body = b"{\"Hash\":\"QmEarlyRoot\",\"Size\":\"5\"}\n";
        socket
            .write_all(
                format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).as_bytes(),
            )
            .await
            .unwrap();
        socket.write_all(body).await.unwrap();
        root_sent.send(()).unwrap();
        // Keep the connection available while the caller resolves the source.
        let mut buffer = [0_u8; 1024];
        while let Ok(read) = socket.read(&mut buffer).await {
            if read == 0 {
                break;
            }
        }
    });
    let (release, released) = tokio::sync::oneshot::channel();
    let source = async_stream::stream! {
        yield Ok::<_, std::io::Error>(Bytes::from_static(b"first"));
        released.await.unwrap();
        yield Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "late source failure"));
    };
    let (progress, _events) = tokio::sync::mpsc::channel(1);
    let add = stream_add_with_progress(&client, source, 1, progress, CancellationToken::new());
    tokio::pin!(add);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::select! {
            result = &mut add => panic!("add returned before source completion: {result:?}"),
            result = root_received => result.unwrap(),
        }
        tokio::select! {
            result = &mut add => panic!("early root completed the source: {result:?}"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {},
        }
        release.send(()).unwrap();
        assert!(matches!(add.await, Err(StreamAddError::Source(error))
            if error.kind() == std::io::ErrorKind::InvalidData));
    })
    .await
    .expect("early response and late source failure must resolve");
    server.abort();
    let _ = server.await;
}
