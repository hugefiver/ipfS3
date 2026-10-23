use super::*;
use std::sync::Arc;

/// Original bytes are owned; catalog labels and one row key are shared by
/// the cells of a bounded source read.
#[derive(Clone, PartialEq, Eq)]
pub(in crate::pinning::management::retirement) struct Part {
    pub table: &'static str,
    pub key: Arc<NativeKey>,
    pub column: &'static str,
    pub offset: u64,
    pub length: Option<u64>,
    pub bytes: Vec<u8>,
}

impl Part {
    pub(super) fn checksum_with_key(&self, key: &str) -> String {
        ArchivePart::checksum_parts(
            self.table,
            key,
            self.column,
            self.offset,
            self.length,
            &self.bytes,
        )
    }
}

impl PartialEq<ArchivePart> for Part {
    fn eq(&self, other: &ArchivePart) -> bool {
        self.table == other.table
            && self.key.as_ref() == &other.key
            && self.column == other.column
            && self.offset == other.offset
            && self.length == other.length
            && self.bytes == other.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn staging_bills_key_encoding_scalar_slots_and_duplicate_verification() {
        let mut workspace = Workspace::create().unwrap();
        let (path, file) = workspace.reserve().await.unwrap();
        let spool = InventorySpool::allocate(&path, file, None).await.unwrap();
        let limits = RunLimits::default();
        let run = super::super::super::spool::task_run(&spool.db, &limits)
            .await
            .unwrap();
        let key = Arc::new(NativeKey {
            table: "objects".into(),
            values: vec!["source-秘密🔒".into()],
        });
        let encoded = encode(key.as_ref()).unwrap();
        let parts = [
            Part {
                table: "objects",
                key: key.clone(),
                column: "content_type",
                offset: 0,
                length: None,
                bytes: Vec::new(),
            },
            Part {
                table: "objects",
                key,
                column: "key",
                offset: 0,
                length: Some(3),
                bytes: "界".as_bytes().to_vec(),
            },
        ];
        let written = encoded.len() as u64
            + parts
                .iter()
                .map(|part| {
                    (part.table.len()
                        + encoded.len()
                        + part.column.len()
                        + part.bytes.len()
                        + 64
                        + 2 * 8) as u64
                })
                .sum::<u64>();
        let tx = spool.db.begin().await.unwrap();
        let mut work = QueryWork::default();
        stage::parts(&tx, &parts, &run, &mut work).await.unwrap();
        assert_eq!(work.bytes, written);
        tx.commit().await.unwrap();
        for part in &parts {
            let original = ArchivePart {
                table: part.table.into(),
                key: part.key.as_ref().clone(),
                column: part.column.into(),
                offset: part.offset,
                length: part.length,
                bytes: part.bytes.clone(),
            };
            let row = one(
                &spool.db,
                "SELECT checksum,length,bytes FROM parts WHERE column_name=$1",
                vec![part.column.into()],
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                get::<String>(&row, "checksum").unwrap(),
                original.checksum().unwrap()
            );
            assert_eq!(
                get::<Option<i64>>(&row, "length").unwrap(),
                part.length.map(|n| n as i64)
            );
            assert_eq!(get::<Vec<u8>>(&row, "bytes").unwrap(), part.bytes);
        }
        let tx = spool.db.begin().await.unwrap();
        let mut retry = QueryWork::default();
        stage::parts(&tx, &parts, &run, &mut retry).await.unwrap();
        let verified = parts
            .iter()
            .map(|part| {
                (part.table.len() + encoded.len() + part.column.len() + part.bytes.len() + 2 * 8)
                    as u64
            })
            .sum::<u64>();
        assert_eq!(retry.bytes, written + verified);
        tx.commit().await.unwrap();
        spool.close().await.unwrap();
        workspace.cleanup().unwrap();
    }
}
