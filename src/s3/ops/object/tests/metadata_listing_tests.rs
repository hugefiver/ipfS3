use super::*;
use std::collections::BTreeSet;

mod snapshot;

#[tokio::test]
async fn listing_projection_excludes_large_metadata_and_encryption_payloads() {
    let state = list_state_with_keys(&["a", "目录/é", "目录/猫", "😀"]).await;
    let payload = "private".repeat(64 * 1024);
    let wrap = "wrapped-key".repeat(16 * 1024);
    state.store.db().execute(sea_orm::Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "UPDATE objects SET metadata = $1, key_wrap = $2, content_type = $3, sse_c_key_fingerprint = $4, encrypted = TRUE",
        [serde_json::json!({"large": payload}).to_string().into(), wrap.clone().into(), payload.clone().into(), payload.clone().into()],
    )).await.unwrap();
    let full = crate::store::object::query_meter::QueryMeter::new(state.store.db());
    crate::store::object::get_latest(&full, "bucket", "a")
        .await
        .unwrap();
    assert!(full.text_bytes() > 1024 * 1024);

    for delimiter in [None, Some("/")] {
        let meter = crate::store::object::query_meter::QueryMeter::new(state.store.db());
        let page = build_listing_page(
            &meter,
            ListingRequest {
                bucket: "bucket",
                prefix: "",
                delimiter,
                cursor: None,
                max_keys: 2,
            },
        )
        .await
        .unwrap();
        assert_eq!(page.entries.len(), 2);
        assert!(page.is_truncated);
        assert!(meter.text_bytes() < 1024);
        assert_eq!(
            meter.fields(),
            BTreeSet::from(["id", "bucket", "key", "cid", "size", "etag", "created_at"])
        );
        let ListingEntry::Object(first) = &page.entries[0] else {
            panic!("expected object")
        };
        let full_row = crate::store::object::get_latest(state.store.db(), "bucket", "a")
            .await
            .unwrap();
        assert_eq!(first.id, full_row.id);
        assert_eq!(first.cid, full_row.cid);
        assert_eq!(first.etag, full_row.etag);
        assert_eq!(first.size, full_row.size);
        assert_eq!(first.created_at, full_row.created_at);
    }
}

#[tokio::test]
async fn delimiter_scan_keeps_bounded_batches_and_unicode_cursor_across_batches() {
    let state = list_state_with_keys(&[]).await;
    let mut keys = vec!["a".to_owned()];
    keys.extend((0..1001).map(|index| format!("目录/{index:04}")));
    keys.push("😀".to_owned());
    let txn = state.store.db().begin().await.unwrap();
    for chunk in keys.chunks(100) {
        object::Entity::insert_many(chunk.iter().map(|key| object::ActiveModel {
            id: Set(key.clone()),
            bucket: Set("bucket".to_owned()),
            key: Set(key.clone()),
            cid: Set("cid".to_owned()),
            etag: Set("etag".to_owned()),
            size: Set(7),
            ..Default::default()
        }))
        .exec(&txn)
        .await
        .unwrap();
    }
    txn.commit().await.unwrap();
    let txn = super::super::super::storage_class::begin_read_snapshot(state.store.db())
        .await
        .unwrap();
    let meter = crate::store::object::query_meter::QueryMeter::new(&txn);
    let (first, objects, prefixes) = listing_page_in_snapshot(
        &meter,
        ListingRequest {
            bucket: "bucket",
            prefix: "",
            delimiter: Some("/"),
            cursor: None,
            max_keys: 2,
        },
        false,
    )
    .await
    .unwrap();
    assert_eq!(first.object_keys(), vec!["a"]);
    assert_eq!(first.common_prefixes(), vec!["目录/"]);
    assert_eq!(objects.len(), 1);
    assert_eq!(prefixes.len(), 1);
    assert_eq!(first.next_cursor.as_deref(), Some("目录/1000"));
    assert!(first.is_truncated);
    assert_eq!(meter.max_rows(), 1000);
    assert_eq!(meter.counts().1, 1004);
    assert!(
        meter.counts().0 <= 5,
        "residency lookup must remain batched"
    );
    let second = build_listing_page(
        &txn,
        ListingRequest {
            bucket: "bucket",
            prefix: "",
            delimiter: Some("/"),
            cursor: first.next_cursor.as_deref(),
            max_keys: 2,
        },
    )
    .await
    .unwrap();
    assert_eq!(second.object_keys(), vec!["😀"]);
    assert!(second.common_prefixes().is_empty());
    assert!(!second.is_truncated);
    txn.commit().await.unwrap();
}

#[tokio::test]
async fn flat_small_page_reads_only_page_and_one_lookahead_object() {
    let state = list_state_with_keys(&["a", "b", "c", "d"]).await;
    let meter = crate::store::object::query_meter::QueryMeter::new(state.store.db());
    let first = build_listing_page(
        &meter,
        ListingRequest {
            bucket: "bucket",
            prefix: "",
            delimiter: None,
            cursor: None,
            max_keys: 1,
        },
    )
    .await
    .unwrap();
    assert_eq!(first.object_keys(), vec!["a"]);
    assert!(first.is_truncated);
    assert_eq!(first.next_cursor.as_deref(), Some("a"));
    // One bucket-existence row and two object rows, without a second scan.
    assert_eq!(meter.counts(), (2, 3));

    let meter = crate::store::object::query_meter::QueryMeter::new(state.store.db());
    let last = build_listing_page(
        &meter,
        ListingRequest {
            bucket: "bucket",
            prefix: "",
            delimiter: Some(""),
            cursor: Some("c"),
            max_keys: 1,
        },
    )
    .await
    .unwrap();
    assert_eq!(last.object_keys(), vec!["d"]);
    assert!(!last.is_truncated);
    assert_eq!(last.next_cursor, None);
    assert_eq!(meter.counts(), (2, 2));
}
