use super::*;
use crate::pinning::identity::{CleanupMode, ProviderRouteSnapshot};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

fn route() -> RouteIdentity {
    RouteIdentity::from_original(ProviderRouteSnapshot {
        provider_id: "provider".into(),
        backend: "noop".into(),
        scope: "scope".into(),
        storage_domain: "remote".into(),
        credential_revision: 1,
        endpoint_revision: 1,
        secret_ref: None,
        api_profile: "noop".into(),
        strategy: "cid".into(),
        cleanup: CleanupMode::Retain,
        confirmation: None,
    })
}

fn selections(count: usize) -> [RetirementSelection; 3] {
    [
        RetirementSelection::ReleaseRootReferences {
            references: (0..count)
                .map(|index| NativeKey {
                    table: "zip_root_references".into(),
                    values: vec![
                        format!("batch-{index}"),
                        "1".into(),
                        "1".into(),
                        "source".into(),
                        "directory".into(),
                        CID.into(),
                    ],
                })
                .collect(),
        },
        RetirementSelection::Migrate {
            targets: (0..count)
                .map(|index| RetirementMigration {
                    plan_id: "original-migrations".into(),
                    item_id: format!("migration-{index}"),
                    lease_id: format!("lease-{index}"),
                    source_target_id: format!("target-{index}"),
                    cid: CID.into(),
                    destination: route(),
                })
                .collect(),
        },
        RetirementSelection::ControlledCleanup {
            resources: (0..count)
                .map(|index| RetirementResource {
                    key: route().remote_key(CID),
                    route: route(),
                    epoch: i64::try_from(index).unwrap() + 1,
                    resource_id: format!("resource-{index}"),
                })
                .collect(),
        },
    ]
}

fn request(selection: RetirementSelection) -> PlanRequest {
    PlanRequest {
        plan_id: "original-retirement".into(),
        items: vec![ItemRequest {
            item_id: "retire".into(),
            action: Action::RetireWith { selection },
            subject: Subject::Provider { route: route() },
        }],
    }
}

#[test]
fn historical_retirement_preserves_larger_original_selections() {
    for selection in selections(MAX_ITEMS + 1) {
        let request = request(selection);
        assert_eq!(validate_request(&request), Err(SafeError::LimitExceeded));
        assert_eq!(validate_historical_request(&request), Ok(()));
    }
}

#[test]
fn historical_retirement_request_round_trips_as_v3_data_without_new_execution_permission() {
    use crate::pinning::management::wire::v3::{
        self, AppliedTrust, Event, Frame, ItemFact, Projection,
    };
    use crate::store::pinning::management::native::meter::Budget as WireBudget;
    for selection in selections(MAX_ITEMS + 1) {
        let request = request(selection);
        let mut writer =
            v3::Writer::new(Vec::new(), Projection::Review, WireBudget::default()).unwrap();
        writer
            .frame(&Frame::Header {
                schema_version: v3::SCHEMA_VERSION,
                projection: Projection::Review,
                database_scope: "original-scope".into(),
                database_identity: "original-database".into(),
                principal: "local_configuration".into(),
                applied_trust: AppliedTrust::UntrustedClaimsOnly,
                catalog: vec!["original-catalog".into()],
                created_at: "2026-01-01T00:00:00Z".into(),
                expires_at: Some("2026-01-01T00:05:00Z".into()),
            })
            .unwrap();
        writer
            .frame(&Frame::Request {
                request: Some(request.clone()),
            })
            .unwrap();
        writer
            .frame(&Frame::Configuration {
                configuration: None,
            })
            .unwrap();
        writer
            .frame(&Frame::Acknowledgements {
                item_ids: Some(vec!["retire".into()]),
            })
            .unwrap();
        writer
            .frame(&Frame::Item {
                entry: Box::new(ItemFact {
                    id: IdempotencyId("original-item".into()),
                    request: request.items[0].clone(),
                    status: PlanStatus::Candidate,
                    step: None,
                }),
            })
            .unwrap();
        writer
            .frame(&Frame::Summary {
                summary: Summary {
                    items: 1,
                    candidates: 1,
                    ..Default::default()
                },
            })
            .unwrap();
        let bytes = writer.finish().unwrap();
        let mut reader =
            v3::Reader::new(bytes.as_slice(), Projection::Review, WireBudget::default()).unwrap();
        let mut restored = None;
        while let Some(event) = reader.next_frame().unwrap() {
            if let Event::Frame(Frame::Request { request }) = event {
                restored = request;
            }
        }
        assert!(reader.verified());
        let restored = restored.unwrap();
        assert!(restored == request);
        assert_eq!(validate_request(&restored), Err(SafeError::LimitExceeded));
    }
}

#[test]
fn historical_retirement_retains_uniqueness_and_identity_checks() {
    for mut selection in selections(MAX_ITEMS + 1) {
        match &mut selection {
            RetirementSelection::ReleaseRootReferences { references } => {
                references.push(references[0].clone());
            }
            RetirementSelection::Migrate { targets } => targets.push(targets[0].clone()),
            RetirementSelection::ControlledCleanup { resources } => {
                resources.push(resources[0].clone());
            }
            RetirementSelection::Retain => unreachable!(),
        }
        assert_eq!(
            validate_historical_request(&request(selection)),
            Err(SafeError::DuplicateIdentity)
        );
    }
    let mut invalid = request(RetirementSelection::Retain);
    let Subject::Provider { route } = &mut invalid.items[0].subject else {
        unreachable!()
    };
    route.credential_revision = 0;
    assert_eq!(
        validate_historical_request(&invalid),
        Err(SafeError::InvalidInput)
    );
}

#[test]
fn historical_retirement_keeps_cumulative_and_plan_item_bounds() {
    for selection in selections(MAX_RECORDS) {
        assert_eq!(
            validate_historical_request(&request(selection)),
            Err(SafeError::LimitExceeded)
        );
    }
    let mut invalid = request(RetirementSelection::Retain);
    invalid.items = (0..=MAX_ITEMS)
        .map(|index| ItemRequest {
            item_id: format!("retire-{index}"),
            ..invalid.items[0].clone()
        })
        .collect();
    assert_eq!(
        validate_historical_request(&invalid),
        Err(SafeError::LimitExceeded)
    );
}

#[test]
fn historical_retirement_keeps_cumulative_text_bounds() {
    let [mut selection, _, _] = selections(3000);
    let RetirementSelection::ReleaseRootReferences { references } = &mut selection else {
        unreachable!()
    };
    for (index, key) in references.iter_mut().enumerate() {
        key.values[0] = format!("{index:0512}");
        key.values[3] = "s".repeat(512);
        key.values[4] = "d".repeat(512);
    }
    assert_eq!(
        validate_historical_request(&request(selection)),
        Err(SafeError::LimitExceeded)
    );
}
