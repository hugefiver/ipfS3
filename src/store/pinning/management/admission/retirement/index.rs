// The ordinary route index checks the actual complete action receipt against
// its confirmed entry, without parsing retained native_before/inventory blobs.
pub(super) const SQLITE_ROUTES: &str = r#"
SELECT r.allocation_domain,r.database_scope,r.native_identity,r.disposition,a.original_result AS receipt,
 CASE WHEN r.record_kind='action' AND r.state IN ('retired','needs_attention') AND r.disposition IN ('retain','migrate','controlled_cleanup')
  AND a.state='applied' AND a.completed_at IS NOT NULL AND a.binding=r.binding
  AND a.principal=r.principal AND a.native_identity=r.native_identity
  AND json_extract(r.binding,'$[0].database_scope')=r.database_scope
  AND json_extract(r.binding,'$[0].database_identity')=r.native_identity
  AND json_extract(r.binding,'$[0].principal')=r.principal
  AND json_extract(r.binding,'$[0].request.plan_id')=r.plan_id
  AND json_extract(r.binding,'$[0].confirmation.plan_id')=r.plan_id
  AND json_extract(a.result,'$.intent_ids')=a.intent_ids AND json_extract(a.result,'$.job_ids')=a.job_ids
  AND (SELECT count(*) FROM json_each(r.binding,'$[0].confirmation.bindings') e
       WHERE json_extract(e.value,'$.request.item_id')=r.item_id)=1
  AND json_extract(a.result,'$.binding')=(SELECT json(e.value) FROM json_each(r.binding,'$[0].confirmation.bindings') e
       WHERE json_extract(e.value,'$.request.item_id')=r.item_id)
 THEN 1 ELSE 0 END AS sealed
FROM pin_management_retirements r LEFT JOIN
 (SELECT database_scope,plan_id,item_id,kind,state,completed_at,binding,principal,native_identity,intent_ids,job_ids,
   result AS original_result,COALESCE(json_extract(result,'$.receipt'),result) AS result FROM pin_management_records) a
 ON a.database_scope=r.database_scope AND a.plan_id=r.plan_id AND a.item_id=r.item_id AND a.kind='action'
ORDER BY r.allocation_domain
"#;

pub(super) const POSTGRES_ROUTES: &str = r#"
SELECT r.allocation_domain,r.database_scope,r.native_identity,r.disposition,a.original_result AS receipt,
 CASE WHEN r.record_kind='action' AND r.state IN ('retired','needs_attention') AND r.disposition IN ('retain','migrate','controlled_cleanup')
  AND a.state='applied' AND a.completed_at IS NOT NULL AND a.binding=r.binding
  AND a.principal=r.principal AND a.native_identity=r.native_identity
  AND r.binding::jsonb->0->>'database_scope'=r.database_scope
  AND r.binding::jsonb->0->>'database_identity'=r.native_identity
  AND r.binding::jsonb->0->>'principal'=r.principal
  AND r.binding::jsonb->0->'request'->>'plan_id'=r.plan_id
  AND r.binding::jsonb->0->'confirmation'->>'plan_id'=r.plan_id
  AND a.result::jsonb->'intent_ids'=a.intent_ids::jsonb AND a.result::jsonb->'job_ids'=a.job_ids::jsonb
  AND (SELECT count(*) FROM jsonb_array_elements(r.binding::jsonb->0->'confirmation'->'bindings') e
       WHERE e->'request'->>'item_id'=r.item_id)=1
  AND a.result::jsonb->'binding'=(SELECT e FROM jsonb_array_elements(r.binding::jsonb->0->'confirmation'->'bindings') e
       WHERE e->'request'->>'item_id'=r.item_id)
 THEN 1::bigint ELSE 0::bigint END AS sealed
FROM pin_management_retirements r LEFT JOIN
 (SELECT database_scope,plan_id,item_id,kind,state,completed_at,binding,principal,native_identity,intent_ids,job_ids,
   result AS original_result,COALESCE(result::jsonb->'receipt',result::jsonb)::text AS result FROM pin_management_records) a
 ON a.database_scope=r.database_scope AND a.plan_id=r.plan_id AND a.item_id=r.item_id AND a.kind='action'
ORDER BY r.allocation_domain
"#;

// Validate the exact receipt/binding/native-before/inventory/private relation.
// The record owner's decoder restores the original route at this selected point;
// SQL never interprets a packed snapshot as a raw typed route.
pub(super) const SQLITE: &str = r#"
SELECT r.allocation_domain,r.database_scope,r.native_identity,
 CASE WHEN r.record_kind='action' AND r.state IN ('retired','needs_attention') AND r.disposition IN ('retain','migrate','controlled_cleanup')
  AND a.state='applied' AND a.completed_at IS NOT NULL AND a.binding=r.binding
  AND a.principal=r.principal AND a.native_identity=r.native_identity
  AND json_extract(r.binding,'$[0].database_scope')=r.database_scope
  AND json_extract(r.binding,'$[0].database_identity')=r.native_identity
  AND json_extract(r.binding,'$[0].principal')=r.principal
  AND json_extract(r.binding,'$[0].request.plan_id')=r.plan_id
  AND json_extract(r.binding,'$[0].confirmation.plan_id')=r.plan_id
  AND json_extract(r.binding,'$[0].confirmation.native_frame.database_scope')=r.database_scope
  AND json_extract(r.binding,'$[0].confirmation.native_frame.database_identity')=r.native_identity
  AND json_extract(a.native_before,'$[0].binding.request')=json_extract(r.binding,'$[0].request')
  AND json_extract(a.native_before,'$[0].binding.confirmation.native_frame')=json_extract(r.inventory,'$[0]')
  AND json_extract(a.native_before,'$[1]')=json_extract(r.inventory,'$[1]')
  AND json_extract(r.inventory,'$[0]')=json_extract(r.binding,'$[0].confirmation.native_frame')
  AND json_extract(r.inventory,'$[1]')=json_extract(r.binding,'$[1]')
  AND json_extract(a.result,'$.intent_ids')=a.intent_ids AND json_extract(a.result,'$.job_ids')=a.job_ids
  AND (SELECT count(*) FROM json_each(r.binding,'$[0].confirmation.bindings') e
       WHERE json_extract(e.value,'$.request.item_id')=r.item_id)=1
  AND json_extract(a.result,'$.binding')=(SELECT json(e.value) FROM json_each(r.binding,'$[0].confirmation.bindings') e
   WHERE json_extract(e.value,'$.request.item_id')=r.item_id
       AND json_extract(e.value,'$.step.retire.stop_new_allocations')=1)
 THEN 1 ELSE 0 END AS sealed
FROM pin_management_retirements r LEFT JOIN
 (SELECT database_scope,plan_id,item_id,kind,state,completed_at,binding,principal,native_identity,native_before,intent_ids,job_ids,
  COALESCE(json_extract(result,'$.receipt'),result) AS result FROM pin_management_records) a
 ON a.database_scope=r.database_scope AND a.plan_id=r.plan_id AND a.item_id=r.item_id AND a.kind='action'
WHERE r.allocation_domain=$1
ORDER BY r.allocation_domain
"#;

pub(super) const POSTGRES: &str = r#"
SELECT r.allocation_domain,r.database_scope,r.native_identity,
 CASE WHEN r.record_kind='action' AND r.state IN ('retired','needs_attention') AND r.disposition IN ('retain','migrate','controlled_cleanup')
  AND a.state='applied' AND a.completed_at IS NOT NULL AND a.binding=r.binding
  AND a.principal=r.principal AND a.native_identity=r.native_identity
  AND r.binding::jsonb->0->>'database_scope'=r.database_scope
  AND r.binding::jsonb->0->>'database_identity'=r.native_identity
  AND r.binding::jsonb->0->>'principal'=r.principal
  AND r.binding::jsonb->0->'request'->>'plan_id'=r.plan_id
  AND r.binding::jsonb->0->'confirmation'->>'plan_id'=r.plan_id
  AND r.binding::jsonb->0->'confirmation'->'native_frame'->>'database_scope'=r.database_scope
  AND r.binding::jsonb->0->'confirmation'->'native_frame'->>'database_identity'=r.native_identity
  AND a.native_before::jsonb->0->'binding'->'request'=r.binding::jsonb->0->'request'
  AND a.native_before::jsonb->0->'binding'->'confirmation'->'native_frame'=r.inventory::jsonb->0
  AND a.native_before::jsonb->1=r.inventory::jsonb->1
  AND r.inventory::jsonb->0=r.binding::jsonb->0->'confirmation'->'native_frame'
  AND r.inventory::jsonb->1=r.binding::jsonb->1
  AND a.result::jsonb->'intent_ids'=a.intent_ids::jsonb AND a.result::jsonb->'job_ids'=a.job_ids::jsonb
  AND (SELECT count(*) FROM jsonb_array_elements(r.binding::jsonb->0->'confirmation'->'bindings') e
       WHERE e->'request'->>'item_id'=r.item_id)=1
  AND a.result::jsonb->'binding'=(SELECT e FROM jsonb_array_elements(r.binding::jsonb->0->'confirmation'->'bindings') e
   WHERE e->'request'->>'item_id'=r.item_id
       AND e->'step'->'retire'->>'stop_new_allocations'='true')
 THEN 1::bigint ELSE 0::bigint END AS sealed
FROM pin_management_retirements r LEFT JOIN
 (SELECT database_scope,plan_id,item_id,kind,state,completed_at,binding,principal,native_identity,native_before,intent_ids,job_ids,
  COALESCE(result::jsonb->'receipt',result::jsonb)::text AS result FROM pin_management_records) a
 ON a.database_scope=r.database_scope AND a.plan_id=r.plan_id AND a.item_id=r.item_id AND a.kind='action'
WHERE r.allocation_domain=$1
ORDER BY r.allocation_domain
"#;
