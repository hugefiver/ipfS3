use super::*;

#[derive(Deserialize)]
struct Rules {
    policy: Vec<Rule>,
}

fn compile(config: &str) -> anyhow::Result<Policies> {
    Policies::new(&toml::from_str::<Rules>(config)?.policy)
}

fn context(action: Action, resource: Resource<'_>) -> Context<'_> {
    Context {
        action,
        resource,
        version: None,
        request_tags: None,
        existing_tags: None,
    }
}

#[test]
fn user_rules_restrict_all_buckets_bucket_rules_restrict_every_user_in_that_bucket() {
    let policies = compile(
        r#"
        [[policy]]
        effect = "allow"
        user = "reader"
        bucket = "photos"
        actions = ["GetObject"]
        prefix = "public/"

        [[policy]]
        effect = "deny"
        bucket = "photos"
        actions = ["GetObject"]
        prefix = "public/hidden/"
        "#,
    )
    .unwrap();

    let public = context(Action::GetObject, Resource::Object("public/image.jpg"));
    let hidden = context(Action::GetObject, Resource::Object("public/hidden/file"));
    let photos = policies.scope("reader", Some("photos"));
    assert!(photos.allows(&public));
    assert!(!photos.allows(&hidden));
    assert!(!policies.scope("reader", Some("other")).allows(&public));
    assert!(!policies.scope("other", Some("photos")).allows(&public));
    assert!(
        !policies
            .scope("reader", None)
            .allows(&context(Action::ListBuckets, Resource::Bucket))
    );
    assert!(
        policies
            .scope("other", None)
            .allows(&context(Action::ListBuckets, Resource::Bucket))
    );
    assert!(policies.has_scope("reader", Some("other")));
    assert!(policies.has_scope("other", Some("photos")));
    assert!(!policies.has_scope("other", Some("other")));
    let unrelated = policies.scope("other", Some("other"));
    assert!(unrelated.is_unrestricted());
    assert!(unrelated.allows(&hidden));
    assert!(!unrelated.needs_existing_tags(&hidden));
}

#[test]
fn whole_prefix_needs_covering_allow_and_rejects_overlapping_deny() {
    let policies = compile(
        r#"
        [[policy]]
        effect = "deny"
        bucket = "photos"
        actions = ["ListObjectsV2", "DecompressZip", "GetDecompressZipStatus"]
        prefix = "public/private/"

        [[policy]]
        effect = "allow"
        bucket = "photos"
        actions = ["ListObjectsV2", "DecompressZip", "GetDecompressZipStatus"]
        prefix = "public/"
        "#,
    )
    .unwrap();
    let scope = policies.scope("reader", Some("photos"));
    for action in [
        Action::ListObjectsV2,
        Action::DecompressZip,
        Action::GetDecompressZipStatus,
    ] {
        assert!(!scope.allows(&context(action, Resource::Prefix(""))));
        assert!(!scope.allows(&context(action, Resource::Prefix("public/"))));
        assert!(!scope.allows(&context(
            action,
            Resource::Prefix("public/private/incoming/")
        )));
        assert!(scope.allows(&context(action, Resource::Prefix("public/releases/"))));
    }
    assert!(scope.allows(&context(
        Action::DecompressZip,
        Resource::Object("public/archive.zip")
    )));
}

#[test]
fn explicit_version_and_all_tag_equalities_must_match() {
    let policies = compile(
        r#"
        [[policy]]
        effect = "allow"
        user = "editor"
        actions = ["PutObjectTagging"]
        version = "null"
        request_tags = { owner = "team", state = "ready" }
        existing_tags = { owner = "team" }
        "#,
    )
    .unwrap();
    let scope = policies.scope("editor", Some("photos"));
    let requested = [
        ObjectTag::new("owner", "team"),
        ObjectTag::new("state", "ready"),
    ];
    let existing = [ObjectTag::new("owner", "team")];
    let wrong_owner = [ObjectTag::new("owner", "other")];
    let request = context(Action::PutObjectTagging, Resource::Object("image.jpg"));
    let request = Context {
        request_tags: Some(&requested),
        ..request
    };
    assert!(!scope.needs_existing_tags(&request));
    let request = Context {
        version: Some("null"),
        ..request
    };
    assert!(!scope.needs_existing_tags(&Context {
        request_tags: Some(&requested[..1]),
        ..request
    }));
    assert!(scope.needs_existing_tags(&request));
    assert!(!scope.allows(&request));
    assert!(!scope.allows(&Context {
        existing_tags: Some(&wrong_owner),
        ..request
    }));
    assert!(!scope.allows(&Context {
        existing_tags: Some(&[]),
        ..request
    }));
    assert!(scope.allows(&Context {
        existing_tags: Some(&existing),
        ..request
    }));
}

#[test]
fn existing_tags_are_loaded_only_if_they_can_change_the_decision() {
    let policies = compile(
        r#"
        [[policy]]
        effect = "allow"
        user = "reader"
        actions = ["GetObject"]
        prefix = "public/"

        [[policy]]
        effect = "allow"
        user = "reader"
        actions = ["GetObject"]
        prefix = "public/"
        existing_tags = { reviewed = "yes" }

        [[policy]]
        effect = "deny"
        user = "reader"
        actions = ["GetObject"]
        prefix = "public/docs/"
        existing_tags = { private = "yes" }

        [[policy]]
        effect = "deny"
        user = "reader"
        actions = ["GetObject"]
        prefix = "public/docs/blocked/"

        [[policy]]
        effect = "deny"
        user = "reader"
        actions = ["GetObject"]
        prefix = "other/"
        existing_tags = { private = "yes" }
        "#,
    )
    .unwrap();
    let scope = policies.scope("reader", Some("photos"));
    let public = context(Action::GetObject, Resource::Object("public/file"));
    assert!(scope.allows(&public));
    assert!(!scope.needs_existing_tags(&public));

    for key in ["public/docs/blocked/file", "other/file"] {
        let blocked = context(Action::GetObject, Resource::Object(key));
        assert!(!scope.allows(&blocked));
        assert!(!scope.needs_existing_tags(&blocked));
    }

    let docs = context(Action::GetObject, Resource::Object("public/docs/file"));
    assert!(!scope.allows(&docs));
    assert!(scope.needs_existing_tags(&docs));
    let private = [ObjectTag::new("private", "yes")];
    assert!(!scope.allows(&Context {
        existing_tags: Some(&private),
        ..docs
    }));
    assert!(scope.allows(&Context {
        existing_tags: Some(&[]),
        ..docs
    }));
}

#[test]
fn public_grant_respects_version_tag_denies_and_never_delegates_bucket_rules_to_anonymous() {
    let policies = compile(
        r#"
        [[policy]]
        effect = "allow"
        bucket = "photos"
        actions = ["DeleteObject", "PutBucketAcl"]

        [[policy]]
        effect = "deny"
        bucket = "photos"
        actions = ["GetObject"]
        version = "null"
        existing_tags = { hidden = "yes" }

        [[policy]]
        effect = "deny"
        user = "owner"
        actions = ["GetBucketAcl"]
    "#,
    )
    .unwrap();
    let public = |user| policies.bucket_scope(user, "photos", Some("owner"), Some("public-read"));
    let get = context(Action::GetObject, Resource::Object("photo"));
    for user in [None, Some("owner"), Some("reader")] {
        let scope = public(user);
        assert!(scope.allows(&get));
        let version = Context {
            version: Some("null"),
            ..get
        };
        assert!(scope.needs_existing_tags(&version));
        assert!(!scope.allows(&Context {
            existing_tags: Some(&[ObjectTag::new("hidden", "yes")]),
            ..version
        }));
        assert!(scope.allows(&Context {
            existing_tags: Some(&[]),
            ..version
        }));
    }
    let delete = context(Action::DeleteObject, Resource::Object("photo"));
    assert!(!public(None).allows(&delete));
    assert!(public(Some("reader")).allows(&delete));
    let set_acl = context(Action::PutBucketAcl, Resource::Bucket);
    assert!(!public(Some("reader")).allows(&set_acl));
    assert!(public(Some("owner")).allows(&set_acl));
    assert!(!public(Some("owner")).allows(&context(Action::GetBucketAcl, Resource::Bucket)));
}

#[test]
fn canned_grants_distinguish_signature_and_ordinary_writes_without_legacy_fallback() {
    let policies = Policies::default();
    let scope = |user, acl| policies.bucket_scope(user, "bucket", Some("owner"), acl);
    let list = context(Action::ListObjectsV2, Resource::Prefix(""));
    assert!(!scope(None, None).allows(&list));
    assert!(scope(Some("reader"), None).allows(&list));
    assert!(!scope(Some("reader"), Some("private")).allows(&list));
    assert!(scope(Some("owner"), Some("private")).allows(&list));
    assert!(!scope(None, Some("authenticated-read")).allows(&list));
    assert!(scope(Some("reader"), Some("authenticated-read")).allows(&list));

    let write = context(Action::CreateMultipartUpload, Resource::Object("photo"));
    assert!(!scope(None, Some("public-read")).allows(&write));
    let public = scope(None, Some("public-read-write"));
    assert!(public.allows(&write));
    assert!(!public.allows(&Context {
        resource: Resource::Prefix("extract/"),
        ..write
    }));
    assert!(!public.allows(&Context {
        request_tags: Some(&[ObjectTag::new("ipfs-s3:pin", "true")]),
        ..write
    }));
    assert!(!public.without_public_grants().allows(&write));
}

#[test]
fn invalid_scope_and_unbindable_conditions_are_rejected_at_startup() {
    for config in [
        r#"actions = ["GetObject"]"#,
        "user = 'reader'\nactions = []",
        "bucket = 'photos'\nactions = ['ListBuckets']",
        "user = 'reader'\nactions = ['ListBuckets']\nprefix = 'photos/'",
        "user = 'reader'\nactions = ['GetObject', 'PutObject']\nversion = 'null'",
        "user = 'reader'\nactions = ['GetObject']\nversion = 'latest'",
        "user = 'reader'\nactions = ['ListObjectVersions']\nexisting_tags = { owner = 'team' }",
        "user = 'reader'\nactions = ['CopyObject']\nexisting_tags = { owner = 'team' }",
        "user = 'reader'\nactions = ['UploadPart']\nrequest_tags = { owner = 'team' }",
        "user = 'reader'\nactions = ['PutObject']\nrequest_tag = { owner = 'team' }",
        "user = 'reader'\nactions = ['PutObject']\nrequest_tags = {}",
        "user = 'reader'\nactions = ['GetObject']\nexisting_tags = { '' = 'team' }",
        "user = 'reader'\nactions = ['GetDecompressZipStatus']\nrequest_tags = { owner = 'team' }",
    ] {
        assert!(
            compile(&format!("[[policy]]\neffect = 'allow'\n{config}")).is_err(),
            "{config}"
        );
    }
}
