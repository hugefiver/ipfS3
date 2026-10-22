use super::super::*;
use crate::{
    pinning::identity::{CleanupMode as NativeCleanupMode, ProviderRouteSnapshot},
    store::{
        self,
        pinning::management::{ManagementError, Result},
    },
};
use retirement::{
    InventorySpool, PlanUnion, Progress, RetirementReview, RunLimits, SourceCheckpoint, Workspace,
};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, Statement, TransactionTrait};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

pub(super) struct Fixture {
    db: DatabaseConnection,
    pub route: ProviderRouteSnapshot,
    configuration: ConfigurationBinding,
    workspace: Workspace,
}

impl Fixture {
    pub async fn new() -> Self {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        store::run_migrations(&db).await.unwrap();
        store::bucket::create(&db, "bucket", None).await.unwrap();
        let route = ProviderRouteSnapshot {
            provider_id: "selected".into(),
            backend: "filebase".into(),
            scope: "account".into(),
            storage_domain: "remote".into(),
            credential_revision: 1,
            endpoint_revision: 1,
            secret_ref: Some("env:RETIRED_CREDENTIAL".into()),
            api_profile: "psa".into(),
            strategy: "cid".into(),
            cleanup: NativeCleanupMode::Retain,
            confirmation: None,
        };
        let fixture = Self {
            db,
            route,
            configuration: ConfigurationBinding {
                routes: vec![],
                policies: vec![],
                limits: Default::default(),
                zip_rules: None,
                source_node: None,
            },
            workspace: Workspace::create().unwrap(),
        };
        fixture
            .sql(
                "INSERT INTO pin_provider_routes VALUES('selected',$1,'selected',0)",
                vec![serde_json::to_string(&fixture.route).unwrap().into()],
            )
            .await;
        fixture.sql("INSERT INTO objects(id,bucket,key,cid,size,etag,encrypted,multipart,is_latest,created_at) VALUES('owner','bucket','key',$1,50,'etag',0,0,1,CURRENT_TIMESTAMP)", vec![CID.into()]).await;
        fixture.sql("INSERT INTO pin_leases(id,owner_object_id,source,policy_id,provider_mode,content_mode,created_at,last_touched_at,expires_at,generation,state) VALUES('lease','owner','automatic','policy','all','object',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'2099-01-01 00:00:00+00:00',1,'active')", vec![]).await;
        fixture.sql("INSERT INTO pin_lease_targets(id,lease_id,cid,logical_size,provider,state,created_at,last_touched_at) VALUES('target','lease',$1,50,'selected','pinned',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", vec![CID.into()]).await;
        fixture
            .remote("selected", CID, "resource", &fixture.route)
            .await;
        fixture.cleanup_job("selected").await;
        fixture
            .history("selected", "resource", &fixture.route, 0)
            .await;
        fixture.sql("INSERT INTO zip_batches(id,owner,source,token,fingerprint,bucket,archive_key,input_identity,captured_options,created_at,updated_at) VALUES('batch','owner','direct','token','fingerprint','bucket','archive','input','{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", vec![]).await;
        fixture.sql("INSERT INTO zip_root_builds(batch_id,revision,epoch,worker,lease_until,status,created_at,updated_at) VALUES('batch',1,1,'worker',CURRENT_TIMESTAMP,'verified',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", vec![]).await;
        fixture.sql("INSERT INTO zip_root_references(batch_id,revision,epoch,node_identity,tier,cid,state,created_at,updated_at) VALUES('batch',1,1,'node','hot',$1,'retained',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", vec![CID.into()]).await;
        fixture
    }

    async fn sql(&self, sql: &str, values: Vec<sea_orm::Value>) {
        self.db
            .execute(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                sql,
                values,
            ))
            .await
            .unwrap();
    }

    pub async fn remote(
        &self,
        allocation: &str,
        cid: &str,
        resource: &str,
        route: &ProviderRouteSnapshot,
    ) {
        self.sql("INSERT INTO remote_pins(provider,cid,request_id,cid_size,status,epoch,failure_attempts,last_touched_at) VALUES($1,$2,$3,50,'pinned',1,0,CURRENT_TIMESTAMP)", vec![allocation.into(), cid.into(), resource.into()]).await;
        self.sql("INSERT INTO remote_pin_ledger(provider,cid,route,ownership,effect) VALUES($1,$2,$3,'application_created','retained')", vec![allocation.into(), cid.into(), serde_json::to_string(route).unwrap().into()]).await;
    }

    async fn cleanup_job(&self, allocation: &str) {
        self.sql("INSERT INTO pin_jobs(id,operation,provider,cid,expected_remote_epoch,submit_phase,state,attempts,next_attempt_at,locked_until,created_at,updated_at) VALUES($1,'unpin',$1,$2,1,NULL,'running',0,CURRENT_TIMESTAMP,'2099-01-01 00:00:00+00:00',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", vec![allocation.into(), CID.into()]).await;
    }

    pub async fn history_without_live_target_or_route(&mut self) {
        self.sql("DELETE FROM pin_lease_targets", vec![]).await;
        let mut old_route = self.route.clone();
        old_route.credential_revision = 99;
        self.remote("historical-alias", CID, "older-resource", &old_route)
            .await;
        self.cleanup_job("historical-alias").await;
        self.history("historical-alias", "older-resource", &old_route, 10)
            .await;
        self.configuration.routes.push(self.route.clone());
    }

    async fn history(
        &self,
        allocation: &str,
        resource: &str,
        route: &ProviderRouteSnapshot,
        epoch: i64,
    ) {
        let ledger = serde_json::json!({
            "provider":allocation, "cid":CID, "route":serde_json::to_string(route).unwrap(),
            "ownership":"application_created", "effect":"retained", "first_observed_at":null,
            "last_observed_at":null, "remote_pinned_at":null, "gateway_verified_at":null,
            "content_verified_at":null, "first_error":null, "last_error":null,
        });
        let history =
            serde_json::json!({"ledger":ledger, "request_id":resource, "confirmed_absent":false});
        self.sql(
            "INSERT INTO pin_resource_history VALUES($1,$2,$3,$4)",
            vec![
                allocation.into(),
                CID.into(),
                epoch.into(),
                history.to_string().into(),
            ],
        )
        .await;
    }

    pub async fn inventory(&mut self) -> InventorySpool {
        let tx = self.db.begin().await.unwrap();
        let checkpoint =
            SourceCheckpoint::start(&tx, RouteIdentity::from_original(self.route.clone()))
                .await
                .unwrap();
        tx.rollback().await.unwrap();
        let (path, reservation) = self.workspace.reserve().await.unwrap();
        InventorySpool::create(&path, reservation, &checkpoint)
            .await
            .unwrap()
    }

    pub async fn scan(&self, spool: &InventorySpool) {
        while spool.scan(&self.db, &RunLimits::default()).await.unwrap() == Progress::Pending {}
    }

    pub async fn reduce(&self, spool: &InventorySpool) {
        self.scan(spool).await;
        loop {
            let mut limits = RunLimits::default();
            limits.pages = 1;
            if spool.reduce(&self.configuration, &limits).await.unwrap() == Progress::Reduced {
                break;
            }
        }
    }

    pub async fn seal(&self, review: &RetirementReview<'_>) -> Result<()> {
        let tx = self
            .db
            .begin()
            .await
            .map_err(|_| ManagementError::Database)?;
        let result = review.seal(&tx, &RunLimits::default()).await;
        tx.rollback().await.map_err(|_| ManagementError::Database)?;
        result
    }

    pub async fn plan(&mut self, spool: &InventorySpool) -> DryRunPlan {
        let review = spool.frozen_review().await.unwrap();
        let request = PlanRequest {
            plan_id: "maintenance-1".into(),
            items: vec![ItemRequest {
                item_id: "file-1".into(),
                action: Action::Retire,
                subject: Subject::Provider {
                    route: review.checkpoint().route.clone(),
                },
            }],
        };
        let local = Snapshot {
            observed_at: review.checkpoint().observed_at.timestamp(),
            database_now: Some(review.checkpoint().observed_at),
            configuration: Some(self.configuration.clone()),
            ..Default::default()
        };
        let (path, reservation) = self.workspace.reserve().await.unwrap();
        let union = PlanUnion::create(&path, reservation).await.unwrap();
        let plan = loop {
            if let Some(plan) = dry_run_retirement(
                &request,
                &local,
                std::slice::from_ref(&review),
                &[],
                &union,
                &RunLimits::default(),
            )
            .await
            .unwrap()
            {
                break plan;
            }
        };
        union.close().await.unwrap();
        plan
    }

    pub async fn close(self) {
        self.db.close().await.unwrap();
        self.workspace.cleanup().unwrap();
    }
}
