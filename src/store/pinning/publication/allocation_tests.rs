use super::*;
use crate::{
    pinning::{
        config::{LeaseDuration, ProviderLimits},
        identity::ProviderIdentity,
        zip_policy::{ZipBoundOutput, ZipIntentSelection, ZipSelectedLease, ZipSelectedProvider},
    },
    store::{
        Store,
        entities::{pin_job, remote_pin_ledger},
        pinning::management::records::{DecodeWork, SharedDecodeBudget},
    },
};
use sea_orm::{ConnectOptions, Database, PaginatorTrait};

const ALIAS: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";

async fn fixture(profile: &str) -> (Store, ProviderIdentity) {
    let mut options = ConnectOptions::new("sqlite::memory:");
    options.max_connections(1);
    let db = Database::connect(options).await.unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    crate::store::bucket::create(&db, "bucket", None)
        .await
        .unwrap();
    let identity = ProviderIdentity::legacy("pinata", profile, None, profile, "cid");
    ledger::register_route(&db, "pinata", &identity)
        .await
        .unwrap();
    let store = Store::new(db);
    store
        .qualify_runtime_schema(DecodeWork::shared(&SharedDecodeBudget::default()))
        .await
        .unwrap();
    (store, identity)
}

fn request(cid: &str) -> PublicationRequest {
    PublicationRequest {
        object: PublicationObject::from_put(
            "object".into(),
            "bucket",
            "key",
            cid.into(),
            7,
            None,
            None,
            false,
            None,
            None,
            Utc::now(),
        ),
        tags: vec![],
        policy: PublicationPolicy {
            tags: vec![],
            leases: vec![LeaseIntent {
                source: LeaseSource::Automatic,
                policy_id: "policy:test".into(),
                provider_mode: ProviderMode::All,
                content_mode: ContentMode::Object,
                providers: vec!["pinata".into()],
                duration: LeaseDuration::parse("1h").unwrap(),
            }],
        },
        object_target: PinTargetSpec {
            cid: cid.into(),
            logical_size: 7,
        },
    }
}

fn limits() -> ProviderLimitMap {
    [(
        "pinata".into(),
        ProviderLimits {
            priority: 1,
            max_bytes: 100,
            max_pins: 10,
            enabled: true,
        },
    )]
    .into()
}

async fn history(db: &DatabaseConnection, cid: &str) {
    remote_pin_ledger::Entity::insert(remote_pin_ledger::ActiveModel {
        provider: Set("pinata".into()),
        cid: Set(cid.into()),
        route: Set(None),
        ownership: Set("unknown".into()),
        effect: Set("absent".into()),
        ..Default::default()
    })
    .exec(db)
    .await
    .unwrap();
}

fn selected(request: &PublicationRequest, identity: &ProviderIdentity) -> ZipIntentSelection {
    let output = ZipBoundOutput {
        object_id: request.object.id.clone(),
        bucket: "bucket".into(),
        key: "key".into(),
        version_id: "candidate:object".into(),
        cid: request.object.cid.clone(),
        logical_size: 7,
    };
    ZipIntentSelection {
        leases: vec![ZipSelectedLease {
            owner: output.clone(),
            intent: request.policy.leases[0].clone(),
            providers: vec![ZipSelectedProvider {
                config_name: "pinata".into(),
                route: identity.route_snapshot(),
                enabled: true,
            }],
            targets: vec![output],
        }],
        source_decision: None,
        output_decisions: vec![],
        warnings: vec![],
    }
}

#[tokio::test]
async fn bounded_allocation_rejects_ledger_only_alias_without_a_remote() {
    for profile in ["kubo", "cluster-rest"] {
        let (store, _) = fixture(profile).await;
        history(store.db(), ALIAS).await;
        let session = SharedDecodeBudget::default();
        let work = DecodeWork::shared(&session);
        let tx = store.begin_runtime(work).await.unwrap();
        let canonical = crate::pinning::provider::canonical_resource_cid(ALIAS).unwrap();
        assert!(matches!(
            ledger::allocation_cid_with_work(&tx.catalog(work), "pinata", &canonical, work).await,
            Err(AppError::InvalidPinningRequest(_))
        ));
        tx.rollback().await.unwrap();
        assert_eq!(
            remote_pin::Entity::find().count(store.db()).await.unwrap(),
            0
        );
        assert_eq!(pin_job::Entity::find().count(store.db()).await.unwrap(), 0);
    }
}

#[tokio::test]
async fn object_allocation_frontier_uses_remaining_publication_work() {
    frontier_remaining_work(false).await;
}

#[tokio::test]
async fn zip_allocation_frontier_uses_remaining_publication_work() {
    frontier_remaining_work(true).await;
}

async fn frontier_remaining_work(zip: bool) {
    for profile in ["kubo", "cluster-rest"] {
        let (store, identity) = fixture(profile).await;
        let request = request(ALIAS);
        let selection = selected(&request, &identity);
        {
            let session = SharedDecodeBudget::default();
            let work = DecodeWork::shared(&session);
            let tx = store.begin_runtime(work).await.unwrap();
            let owner = PublicationTransaction::Runtime(tx, session.clone());
            work.with(|session| {
                session.consume(0, crate::pinning::management::wire::MAX_WIRE_BYTES - 1)
            })
            .unwrap();
            let result = if zip {
                let mut maintenance =
                    crate::store::zip::publication_lease::PublicationLease::start(
                        owner.db(),
                        None,
                        "frontier",
                    )
                    .await
                    .unwrap();
                super::zip::allocation::attachment_pairs(
                    &owner,
                    &selection,
                    &limits(),
                    &mut maintenance,
                    work,
                )
                .await
            } else {
                publication_attachment_pairs(&owner, &request, &[], &limits(), work).await
            };
            assert!(
                matches!(result, Err(AppError::ManagementBudget)),
                "zip={zip}: {result:?}"
            );
            owner.into_db().rollback().await.unwrap();
        }
        assert_eq!(object::Entity::find().count(store.db()).await.unwrap(), 0);
        assert_eq!(
            remote_pin::Entity::find().count(store.db()).await.unwrap(),
            0
        );
    }
}

#[tokio::test]
async fn object_and_zip_frontiers_preserve_canonical_keys_and_reject_both_alias_histories() {
    for profile in ["kubo", "cluster-rest"] {
        for history_kind in ["none", "canonical", "ledger_alias", "remote_alias"] {
            let (store, identity) = fixture(profile).await;
            let canonical = crate::pinning::provider::canonical_resource_cid(ALIAS).unwrap();
            match history_kind {
                "canonical" => history(store.db(), &canonical).await,
                "ledger_alias" => history(store.db(), ALIAS).await,
                "remote_alias" => {
                    remote_pin::Entity::insert(remote_pin::ActiveModel {
                        provider: Set("pinata".into()),
                        cid: Set(ALIAS.into()),
                        cid_size: Set(7),
                        epoch: Set(1),
                        status: Set("reserved".into()),
                        failure_attempts: Set(0),
                        last_touched_at: Set(Utc::now()),
                        ..Default::default()
                    })
                    .exec(store.db())
                    .await
                    .unwrap();
                }
                _ => {}
            }
            let request = request(ALIAS);
            let selection = selected(&request, &identity);
            let session = SharedDecodeBudget::default();
            let work = DecodeWork::shared(&session);
            let tx = store.begin_runtime(work).await.unwrap();
            let owner = PublicationTransaction::Runtime(tx, session.clone());
            let object_pairs =
                publication_attachment_pairs(&owner, &request, &[], &limits(), work).await;
            let mut maintenance = crate::store::zip::publication_lease::PublicationLease::start(
                owner.db(),
                None,
                "frontier",
            )
            .await
            .unwrap();
            let zip_pairs = super::zip::allocation::attachment_pairs(
                &owner,
                &selection,
                &limits(),
                &mut maintenance,
                work,
            )
            .await;
            for result in [object_pairs, zip_pairs] {
                if matches!(history_kind, "none" | "canonical") {
                    assert_eq!(result.unwrap(), vec![("pinata".into(), canonical.clone())]);
                } else {
                    assert!(matches!(result, Err(AppError::InvalidPinningRequest(_))));
                }
            }
            owner.into_db().rollback().await.unwrap();
        }
    }
}

#[tokio::test]
async fn ledger_history_frontier_fails_closed_with_only_remaining_work_and_rolls_back() {
    use crate::store::pinning::management::{
        native::meter::Budget, records::ReadSession, transaction::CancellationToken,
    };
    for zip in [false, true] {
        let (store, identity) = fixture("kubo").await;
        let session = std::sync::Arc::new(std::sync::Mutex::new(ReadSession::for_task(
            Budget::default(),
            u64::MAX,
            64 * 1024,
            CancellationToken::new(),
        )));
        let work = DecodeWork::shared(&session);
        let tx = store.begin_runtime(work).await.unwrap();
        let owner = PublicationTransaction::Runtime(tx, session.clone());
        work.with(|session| session.consume(0, 48 * 1024)).unwrap();
        let request = request(ALIAS);
        // The remaining allowance admits the ordinary route and canonical key.
        publication_attachment_pairs(&owner, &request, &[], &limits(), work)
            .await
            .unwrap();
        let original = cid::Cid::try_from(ALIAS).unwrap().to_bytes();
        let rows = (0..=255)
            .map(|index| {
                let mut bytes = original.clone();
                let last = bytes.len() - 1;
                bytes[last] = index;
                let cid = cid::Cid::try_from(bytes.as_slice()).unwrap();
                remote_pin_ledger::ActiveModel {
                    provider: Set("pinata".into()),
                    cid: Set(cid.to_string()),
                    route: Set(None),
                    ownership: Set("unknown".into()),
                    effect: Set("absent".into()),
                    ..Default::default()
                }
            })
            .collect::<Vec<_>>();
        remote_pin_ledger::Entity::insert_many(rows)
            .exec(owner.db())
            .await
            .unwrap();
        let result = if zip {
            let mut maintenance = crate::store::zip::publication_lease::PublicationLease::start(
                owner.db(),
                None,
                "bounded-history",
            )
            .await
            .unwrap();
            super::zip::allocation::attachment_pairs(
                &owner,
                &selected(&request, &identity),
                &limits(),
                &mut maintenance,
                work,
            )
            .await
        } else {
            publication_attachment_pairs(&owner, &request, &[], &limits(), work).await
        };
        assert!(
            matches!(result, Err(AppError::ManagementBudget)),
            "zip={zip}: {result:?}"
        );
        owner.into_db().rollback().await.unwrap();
        assert_eq!(
            remote_pin_ledger::Entity::find()
                .count(store.db())
                .await
                .unwrap(),
            0
        );
        assert_eq!(object::Entity::find().count(store.db()).await.unwrap(), 0);
    }
}

#[tokio::test]
async fn warm_rpc_and_cluster_publication_reject_ledger_only_alias_before_any_birth() {
    for profile in ["kubo", "cluster-rest"] {
        let (store, _) = fixture(profile).await;
        history(store.db(), ALIAS).await;
        assert!(matches!(
            application::publish_object(&store, request(ALIAS), &limits()).await,
            Err(AppError::InvalidPinningRequest(_))
        ));
        assert_eq!(object::Entity::find().count(store.db()).await.unwrap(), 0);
        assert_eq!(
            object_version::Entity::find()
                .count(store.db())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            pin_lease::Entity::find().count(store.db()).await.unwrap(),
            0
        );
        assert_eq!(
            pin_lease_target::Entity::find()
                .count(store.db())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            remote_pin::Entity::find().count(store.db()).await.unwrap(),
            0
        );
        assert_eq!(pin_job::Entity::find().count(store.db()).await.unwrap(), 0);
    }
}
