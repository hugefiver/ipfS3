use sea_orm::{ConnectionTrait, Statement};

pub(crate) async fn seed_direct_request_receipt<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) {
    let backend = db.get_database_backend();
    let row = db.query_one(Statement::from_sql_and_values(backend,
        "SELECT r.request_id, r.epoch, l.route FROM remote_pins r JOIN remote_pin_ledger l ON l.provider=r.provider AND l.cid=r.cid WHERE r.provider=$1 AND r.cid=$2",
        [provider.into(), cid.into()])).await.unwrap().unwrap();
    use crate::pinning::{
        identity::Ownership,
        provider::{ObservedResource, ObservedResourceStatus},
    };
    let raw = row.try_get::<String>("", "route").unwrap();
    let route: crate::pinning::identity::ProviderRouteSnapshot =
        serde_json::from_str(&raw).unwrap();
    let resource = super::ledger::submission::resource_evidence_for_route(
        &route,
        ObservedResource {
            resource_type: route.resource_type(),
            cid: cid.into(),
            request_id: row.try_get("", "request_id").unwrap(),
            status: ObservedResourceStatus::Reported,
            ownership: Ownership::Unknown,
            replica_evidence: None,
        },
    );
    let resources = serde_json::to_string(&[resource]).unwrap();
    let id = format!("submit:{provider}:{cid}");
    db.execute(Statement::from_sql_and_values(backend,
        "INSERT INTO pin_submit_observations (id,job_id,submit_call,provider,expected_cid,route,remote_epoch,claim_until,started_at,observed_at,effect,resources,outcome,needs_attention) VALUES ($1,$1,1,$2,$3,$4,$5,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'observed',$6,'ReportedPinned',FALSE)",
        [id.into(), provider.into(), cid.into(), raw.into(), row.try_get::<i64>("", "epoch").unwrap().into(), resources.into()])).await.unwrap();
}
