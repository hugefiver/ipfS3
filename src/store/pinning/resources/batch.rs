use std::collections::BTreeMap;

use sea_orm::{ConnectionTrait, DatabaseTransaction, Value};

use super::{AppResult, stmt};

type RefKey<'a> = (&'a str, &'a str, String, &'a str, &'a str, &'a str);

pub(super) const ATTACH_RESOURCE: &str = "ON CONFLICT (provider,cid) DO UPDATE SET
    status=CASE WHEN pin_resources.status='cleaned' THEN 'waiting'
                WHEN pin_resources.status='pending' AND pin_resources.action='delete' AND pin_resources.attempt=0 THEN 'accepted' ELSE pin_resources.status END,
    action=CASE WHEN pin_resources.status='cleaned' THEN 'submit'
                WHEN pin_resources.status='pending' AND pin_resources.action='delete' AND pin_resources.attempt=0 THEN NULL ELSE pin_resources.action END,
    next_at=CASE WHEN pin_resources.status='cleaned' THEN excluded.next_at ELSE pin_resources.next_at END,
    source_cid=CASE WHEN pin_resources.status='cleaned' OR pin_resources.source_cid IS NULL OR substr(excluded.source_cid,1,2)='Qm'
                    THEN excluded.source_cid ELSE pin_resources.source_cid END,
    logical_size=CASE WHEN pin_resources.status='cleaned' THEN excluded.logical_size ELSE pin_resources.logical_size END
    WHERE pin_resources.status='cleaned' OR (pin_resources.status='pending' AND pin_resources.action='delete' AND pin_resources.attempt=0)
       OR pin_resources.source_cid IS NULL OR (substr(excluded.source_cid,1,2)='Qm' AND pin_resources.source_cid!=excluded.source_cid)";

#[derive(Default)]
pub(super) struct RefBatch<'a> {
    pub resources: BTreeMap<(&'a str, String), (i64, &'a str)>,
    pub refs: BTreeMap<RefKey<'a>, (i64, String)>,
}

impl RefBatch<'_> {
    pub(super) async fn flush(&mut self, tx: &DatabaseTransaction, now: i64) -> AppResult<()> {
        let resources = std::mem::take(&mut self.resources).into_iter().map(
            |((provider, cid), (size, source_cid))| {
                vec![
                    provider.into(),
                    cid.into(),
                    source_cid.into(),
                    size.into(),
                    "waiting".into(),
                    "submit".into(),
                    now.into(),
                ]
            },
        );
        // Upsert in key order. An unsent removal is cancelled; an attempted removal finishes first.
        insert(
            tx,
            "pin_resources(provider,cid,source_cid,logical_size,status,action,next_at)",
            resources,
            ATTACH_RESOURCE,
        )
        .await?;
        let refs = std::mem::take(&mut self.refs).into_iter().map(
            |((owner, provider, cid, source, policy, content), (expires, remaining))| {
                vec![
                    owner.into(),
                    provider.into(),
                    cid.into(),
                    source.into(),
                    policy.into(),
                    content.into(),
                    expires.into(),
                    remaining.into(),
                ]
            },
        );
        insert(
            tx,
            "pin_refs(owner_object_id,provider,cid,source,policy_id,content_mode,expires_at,remaining_providers)",
            refs,
            "ON CONFLICT (owner_object_id,provider,cid,source,policy_id,content_mode) DO UPDATE
             SET expires_at=excluded.expires_at WHERE excluded.expires_at>pin_refs.expires_at",
        )
        .await
    }
}

async fn insert(
    tx: &DatabaseTransaction,
    table: &str,
    mut rows: impl Iterator<Item = Vec<Value>>,
    on_conflict: &str,
) -> AppResult<()> {
    loop {
        let mut values = Vec::new();
        let mut groups = Vec::new();
        // Eight parameters per reference; also fit SQLite builds with a 999-bind limit.
        for row in rows.by_ref().take(120) {
            let group = (values.len() + 1..=values.len() + row.len())
                .map(|n| format!("${n}"))
                .collect::<Vec<_>>()
                .join(",");
            groups.push(format!("({group})"));
            values.extend(row);
        }
        if groups.is_empty() {
            return Ok(());
        }
        tx.execute(stmt(
            tx,
            &format!(
                "INSERT INTO {table} VALUES {} {on_conflict}",
                groups.join(",")
            ),
            values,
        ))
        .await?;
    }
}
