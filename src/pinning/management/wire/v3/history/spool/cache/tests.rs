use super::*;
use crate::store::pinning::management::native::meter::Limits;

#[tokio::test]
async fn cached_wire_loads_in_bounded_pages_without_per_chunk_round_trips() {
    let mut setup = Budget::default();
    let spool = Spool::create(&mut setup).await.unwrap();
    let input: Vec<u8> = (0..40 * 65536).map(|index| (index % 251) as u8).collect();
    let mut source = files::stream::temporary().unwrap();
    source.write_all(&input).unwrap();
    spool.store_wire(&mut source, &mut setup).await.unwrap();

    let mut budget = Budget::new(Limits {
        sql: 8,
        ..Limits::default()
    });
    let mut loaded = spool.load_wire(&mut budget).await.unwrap();
    let mut actual = Vec::new();
    loaded.read_to_end(&mut actual).unwrap();
    assert_eq!(actual, input);
    assert_eq!(budget.work.rows, 40);
    assert_eq!(budget.work.raw_bytes, input.len() as u64);
    assert_eq!(budget.work.copied_bytes, input.len() as u64);
    spool.close().await.unwrap();
}

#[tokio::test]
async fn cached_wire_rejects_a_gap_between_pages() {
    let mut budget = Budget::default();
    let spool = Spool::create(&mut budget).await.unwrap();
    for ordinal in 0..18i64 {
        if ordinal != 16 {
            exec(
                &spool.db,
                "INSERT INTO sealed_wire VALUES($1,$2)",
                vec![ordinal.into(), vec![1u8].into()],
                &mut budget,
            )
            .await
            .unwrap();
        }
    }
    assert!(spool.load_wire(&mut budget).await.is_err());
    spool.close().await.unwrap();
}

#[tokio::test]
async fn cached_wire_rejects_empty_and_oversized_payloads() {
    let mut budget = Budget::default();
    let spool = Spool::create(&mut budget).await.unwrap();
    for payload in [Vec::new(), vec![1u8; 65537]] {
        exec(
            &spool.db,
            "INSERT OR REPLACE INTO sealed_wire VALUES(0,$1)",
            vec![payload.into()],
            &mut budget,
        )
        .await
        .unwrap();
        assert!(spool.load_wire(&mut budget).await.is_err());
    }
    spool.close().await.unwrap();
}
