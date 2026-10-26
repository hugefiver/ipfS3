use super::*;

#[tokio::test]
async fn quota_mutation_cannot_reuse_readiness_for_target_publication() {
    for mode in [ProviderMode::All, ProviderMode::One] {
        let db = setup().await;
        db.execute_unprepared("CREATE TRIGGER quota_readiness_change AFTER INSERT ON remote_pins BEGIN UPDATE pin_management_readiness SET state='backfill' WHERE id=1; END").await.unwrap();
        let publication = request(
            object("window-object", "key", "bafy-window", 7),
            vec![],
            vec![intent(
                LeaseSource::Automatic,
                mode,
                &["pinata", "filebase"],
                ContentMode::Object,
            )],
        );
        assert!(publish_object(&db, publication, &limits()).await.is_err());
        assert_eq!(object::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(pin_lease::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(
            pin_lease_target::Entity::find().count(&db).await.unwrap(),
            0
        );
        assert_eq!(remote_pin::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(pin_job::Entity::find().count(&db).await.unwrap(), 0);
        assert!(
            pin_provider_usage::Entity::find()
                .all(&db)
                .await
                .unwrap()
                .iter()
                .all(|usage| usage.reserved_bytes == 0 && usage.reserved_pins == 0)
        );
        crate::store::pinning::management::schema::validate(&db)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn one_mode_unavailable_candidate_does_not_duplicate_quota_or_targets() {
    let db = setup().await;
    let mut capped = limits();
    capped.get_mut("pinata").unwrap().max_bytes = 1;
    let publication = request(
        object("one-window", "key", "bafy-one-window", 7),
        vec![],
        vec![intent(
            LeaseSource::Automatic,
            ProviderMode::One,
            &["pinata", "filebase"],
            ContentMode::Object,
        )],
    );
    publish_object(&db, publication, &capped).await.unwrap();
    let targets = pin_lease_target::Entity::find().all(&db).await.unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].provider, "filebase");
    let usages = pin_provider_usage::Entity::find().all(&db).await.unwrap();
    assert_eq!(
        usages.iter().map(|usage| usage.reserved_bytes).sum::<i64>(),
        7
    );
    assert_eq!(
        usages.iter().map(|usage| usage.reserved_pins).sum::<i64>(),
        1
    );
}

async fn all_unavailable_fixture() -> (DatabaseConnection, ProviderLimitMap) {
    let db = setup().await;
    let mut capped = limits();
    for provider in ["pinata", "filebase"] {
        let identity = crate::pinning::identity::ProviderIdentity::legacy(
            provider, "noop", None, "noop", "cid",
        );
        ledger::register_route(&db, provider, &identity)
            .await
            .unwrap();
        capped.get_mut(provider).unwrap().max_bytes = 1;
    }
    (db, capped)
}

async fn assert_no_reserved_capacity(db: &DatabaseConnection) {
    let usages = pin_provider_usage::Entity::find().all(db).await.unwrap();
    assert!(
        usages
            .iter()
            .all(|usage| usage.reserved_bytes == 0 && usage.reserved_pins == 0)
    );
    assert_eq!(remote_pin::Entity::find().count(db).await.unwrap(), 0);
    assert_eq!(pin_job::Entity::find().count(db).await.unwrap(), 0);
}

#[tokio::test]
async fn one_mode_all_unavailable_publishes_only_first_candidate_without_capacity() {
    let (db, capped) = all_unavailable_fixture().await;
    let publication = request(
        object("all-unavailable", "key", "bafy-all-unavailable", 7),
        vec![],
        vec![intent(
            LeaseSource::Automatic,
            ProviderMode::One,
            &["filebase", "pinata"],
            ContentMode::Object,
        )],
    );

    publish_object(&db, publication, &capped).await.unwrap();

    assert_eq!(
        object::Entity::find().one(&db).await.unwrap().unwrap().id,
        "all-unavailable"
    );
    let lease = pin_lease::Entity::find().one(&db).await.unwrap().unwrap();
    assert_eq!(lease.owner_object_id, "all-unavailable");
    assert_eq!(lease.provider_mode, "one");
    let targets = pin_lease_target::Entity::find().all(&db).await.unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].lease_id, lease.id);
    assert_eq!(targets[0].provider, "pinata");
    assert_eq!(targets[0].cid, "bafy-all-unavailable");
    assert_eq!(targets[0].state, TARGET_QUOTA_BLOCKED);
    assert_no_reserved_capacity(&db).await;
}

#[tokio::test]
async fn unpublished_route_retirement_rejects_publication_atomically() {
    use crate::store::entities::pin_provider_route;

    // A direct route mutation leaves unpublished authority. Both request shapes
    // must fail closed and roll back the mutation with the publication.
    for include_first_candidate in [false, true] {
        let (db, capped) = all_unavailable_fixture().await;
        assert_no_reserved_capacity(&db).await;
        // Frontier prelocking precedes lease creation. Only filebase's own
        // reservation attempt can execute this mutation after a lease exists.
        db.execute_unprepared(
            "CREATE TRIGGER retire_first_candidate BEFORE INSERT ON pin_provider_usage
             WHEN NEW.provider='filebase'
               AND EXISTS(SELECT 1 FROM pin_leases WHERE owner_object_id='retirement-window')
             BEGIN
               UPDATE pin_provider_routes SET retired=TRUE WHERE provider='pinata';
             END",
        )
        .await
        .unwrap();
        let providers: &[&str] = if include_first_candidate {
            &["pinata", "filebase"]
        } else {
            &["filebase"]
        };
        let publication = request(
            object("retirement-window", "key", "bafy-retirement-window", 7),
            vec![],
            vec![intent(
                LeaseSource::Automatic,
                ProviderMode::One,
                providers,
                ContentMode::Object,
            )],
        );

        let result = publish_object(&db, publication, &capped).await;

        assert!(matches!(result, Err(AppError::InvalidPinningRequest(_))));
        assert_eq!(object::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(object_version::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(pin_lease::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(
            pin_lease_target::Entity::find().count(&db).await.unwrap(),
            0
        );
        let first = pin_provider_route::Entity::find_by_id("pinata")
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert!(!first.retired);
        assert!(
            !pin_provider_route::Entity::find_by_id("filebase")
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .retired
        );
        assert_no_reserved_capacity(&db).await;
        for provider in ["pinata", "filebase"] {
            crate::store::pinning::management::admission::check_provider_open(&db, provider)
                .await
                .unwrap();
        }
    }
}
