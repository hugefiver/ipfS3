//! Valid unrelated management/legacy history in the gateway's own database.

use std::collections::BTreeMap;

use ipfs_s3_gateway::{
    config::Config,
    pinning::management::{
        Action, ItemRequest, PlanRequest, PlanStatus, RouteIdentity, Subject, db_snapshot,
        local_config::{self, ManagementConfig},
        retirement::PreparedPlanning,
    },
    state::AppState,
    store::pinning::{
        legacy_migration,
        management::{actions, records},
        quota,
    },
};

const OLD_PROVIDER: &str = "stage75-unrelated-legacy";

pub struct History {
    config: ManagementConfig,
    auth: records::LocalAuthorization,
    binding: records::ConfirmedBinding,
    applied: actions::ApplyResult,
    retirement: records::RetirementRecord,
    legacy_receipt: String,
    allocation: String,
}

pub fn add_route(raw: &mut Config) {
    let unrelated = crate::history_base::config(&raw.kubo.rpc_url);
    raw.pinning.providers.extend(unrelated.pinning.providers);
    raw.pinning.policies.extend(unrelated.pinning.policies);
    raw.pinning_identity
        .providers
        .extend(unrelated.pinning_identity.providers);
}

impl History {
    /// Runs before serving requests or starting workers, matching the legacy
    /// migration's explicit maintenance precondition. No authority rows are forged.
    pub async fn prepare(state: &AppState, raw: &Config) -> Self {
        let db = state.store.db();
        let config = local_config::compile(raw, |_| None)
            .unwrap()
            .with_source_node(crate::history_base::NODE.into())
            .unwrap();
        let provider = config
            .pinning
            .providers
            .iter()
            .find(|p| p.identity.provider_id == "fixture-provider")
            .unwrap();
        let route = provider.route_snapshot();
        let allocation = provider.identity.allocation_key();
        // This public accounting operation creates ordinary source state; the
        // official migration creates its receipt and original-key relationship.
        quota::ensure_usage_row(db, OLD_PROVIDER).await.unwrap();
        let declaration: legacy_migration::Declaration =
            serde_json::from_value(serde_json::json!({
                "version": 1,
                "database_scope": "stage75-pixivbot-history",
                "ttl_seconds": 300,
                "maintenance": {"all_writers_stopped": true, "all_workers_drained": true},
                "namespaces": [{
                    "source": OLD_PROVIDER,
                    "target": allocation,
                    "backend": route.backend,
                    "scope": route.scope,
                    "resource_type": route.resource_type(),
                    "binding_only": false,
                    "target_route": route,
                    "bindings": [{
                        "table": "pin_provider_usage",
                        "key": {"provider": OLD_PROVIDER},
                        "provider": OLD_PROVIDER,
                        "historical_route": null
                    }]
                }]
            }))
            .unwrap();
        let inventory = legacy_migration::inventory(db, &declaration).await.unwrap();
        let legacy = legacy_migration::apply(db, &declaration, &inventory)
            .await
            .unwrap();
        let auth = records::authorize_local(raw, db).await.unwrap();
        let request = PlanRequest {
            plan_id: "stage75-unrelated-retain".into(),
            items: vec![ItemRequest {
                item_id: "unrelated-provider".into(),
                action: Action::Retire,
                subject: Subject::Provider {
                    route: RouteIdentity::from_original(provider.route_snapshot()),
                },
            }],
        };
        let snapshot = db_snapshot::collect_with_config(db, &config, &request)
            .await
            .unwrap()
            .planning_snapshot()
            .unwrap();
        let prepared = PreparedPlanning::collect(
            db,
            &request,
            &snapshot,
            records::SharedDecodeBudget::default(),
        )
        .await
        .unwrap();
        let plan = &prepared.plan;
        assert_eq!(plan.items()[0].status, PlanStatus::Ready);
        let binding = records::confirm(
            db,
            &config,
            &auth,
            (&request, &snapshot, plan),
            vec![],
            chrono::Duration::minutes(5),
        )
        .await
        .unwrap();
        prepared.close().await.unwrap();
        let applied = actions::apply(db, &config, &auth, &binding).await.unwrap();
        assert!(!applied.replayed);
        assert_eq!(applied.items.len(), 1);
        assert!(
            applied.items[0].jobs.is_empty(),
            "unrelated history cannot seed worker IO"
        );
        let retirement = records::read_retirement(db, &auth, &allocation)
            .await
            .unwrap()
            .unwrap();
        let history = Self {
            config,
            auth,
            binding,
            applied,
            retirement,
            legacy_receipt: legacy.id,
            allocation,
        };
        history.assert_intact(state).await;
        history
    }

    pub async fn assert_intact(&self, state: &AppState) {
        let db = state.store.db();
        let replay = actions::apply(db, &self.config, &self.auth, &self.binding)
            .await
            .unwrap();
        assert!(
            replay.replayed,
            "a real applied receipt must predate Pixivbot publication"
        );
        assert!(replay.items == self.applied.items);
        let retirement = records::read_retirement(db, &self.auth, &self.allocation)
            .await
            .unwrap()
            .unwrap();
        assert!(retirement == self.retirement);
        let key = BTreeMap::from([("provider".into(), OLD_PROVIDER.into())]);
        let legacy =
            legacy_migration::resolve_legacy_binding(db, "pin_provider_usage", &key, OLD_PROVIDER)
                .await
                .unwrap()
                .expect("committed legacy original-key relationship remains readable");
        assert_eq!(legacy.receipt_id, self.legacy_receipt);
        assert_eq!(legacy.operational_key, self.allocation);
    }
}
