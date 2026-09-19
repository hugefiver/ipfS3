use std::collections::HashMap;

use ipfs_s3_gateway::store::entities::{
    object, object_version, physical_residency, residency_reference, version_residency,
};
use sea_orm::{DatabaseConnection, EntityTrait};

/// Asserts the publication/deletion residency invariant through durable state.
pub async fn assert_hot_standard_residency_invariant(db: &DatabaseConnection) {
    let objects = object::Entity::find()
        .all(db)
        .await
        .expect("load objects for residency invariant");
    let versions = object_version::Entity::find()
        .all(db)
        .await
        .expect("load object versions for residency invariant");
    let residencies = version_residency::Entity::find()
        .all(db)
        .await
        .expect("load version residencies for invariant");
    let references = residency_reference::Entity::find()
        .all(db)
        .await
        .expect("load residency references for invariant");
    let physical = physical_residency::Entity::find()
        .all(db)
        .await
        .expect("load physical residencies for invariant");

    let objects_by_id = objects
        .iter()
        .map(|object| (object.id.as_str(), object))
        .collect::<HashMap<_, _>>();
    let versions_by_id = versions
        .iter()
        .map(|version| (version.id.as_str(), version))
        .collect::<HashMap<_, _>>();

    for version in &versions {
        if version.kind == "delete_marker" {
            assert!(
                residencies
                    .iter()
                    .all(|residency| residency.version_row_id != version.id),
                "delete marker {} must not have version residency",
                version.id
            );
            assert!(
                references
                    .iter()
                    .all(|reference| reference.version_row_id != version.id),
                "delete marker {} must not have residency references",
                version.id
            );
            continue;
        }
        if version.kind != "object" {
            continue;
        }

        let object_id = version
            .object_id
            .as_deref()
            .unwrap_or_else(|| panic!("content version {} must reference an object", version.id));
        let object = objects_by_id.get(object_id).unwrap_or_else(|| {
            panic!(
                "content version {} references missing object {object_id}",
                version.id
            )
        });

        let owned_residencies = residencies
            .iter()
            .filter(|residency| residency.version_row_id == version.id)
            .collect::<Vec<_>>();
        assert_eq!(
            owned_residencies.len(),
            1,
            "content version {} must have exactly one version residency",
            version.id
        );
        let residency = owned_residencies[0];
        assert_eq!(residency.object_id, object.id, "version residency object");
        assert_eq!(residency.cid, object.cid, "version residency CID");
        assert_eq!(residency.primary_tier, "hot", "version residency tier");
        assert_eq!(
            residency.storage_class, "STANDARD",
            "version residency storage class"
        );

        let retained = references
            .iter()
            .filter(|reference| {
                reference.owner_kind == "version"
                    && reference.owner_id == version.id
                    && reference.reason == "retained_version"
            })
            .collect::<Vec<_>>();
        assert_eq!(
            retained.len(),
            1,
            "content version {} must have exactly one retained-version reference",
            version.id
        );
        let retained = retained[0];
        assert_eq!(retained.version_row_id, version.id, "retained version row");
        assert_eq!(retained.object_id, object.id, "retained version object");
        assert_eq!(retained.tier, "hot", "retained version tier");
        assert_eq!(retained.cid, object.cid, "retained version CID");

        let physical_rows = physical
            .iter()
            .filter(|row| row.tier == "hot" && row.cid == object.cid)
            .collect::<Vec<_>>();
        assert_eq!(
            physical_rows.len(),
            1,
            "content version {} must resolve to one hot physical residency",
            version.id
        );
        assert_eq!(
            physical_rows[0].verification_state, "pending",
            "newly published hot physical residency must remain pending in HTTP acceptance fixtures"
        );
    }

    for residency in &residencies {
        let owner = versions_by_id
            .get(residency.version_row_id.as_str())
            .unwrap_or_else(|| {
                panic!(
                    "version residency {} must not outlive its owner",
                    residency.version_row_id
                )
            });
        assert_eq!(
            owner.kind, "object",
            "only content versions may own version residency"
        );
    }

    for reference in references
        .iter()
        .filter(|reference| reference.reason == "retained_version")
    {
        let owner = versions_by_id
            .get(reference.owner_id.as_str())
            .unwrap_or_else(|| {
                panic!(
                    "retained-version reference {} must not outlive its owner",
                    reference.owner_id
                )
            });
        assert_eq!(owner.kind, "object", "retained owner must be content");
        assert_eq!(
            reference.version_row_id, owner.id,
            "retained owner must be its exact version row"
        );
    }

    // Physical rows are intentionally allowed to remain after their last reference is released.
}
