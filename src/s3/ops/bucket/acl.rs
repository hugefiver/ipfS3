use std::{collections::BTreeSet, sync::Arc};

use s3s::{S3Request, S3Response, S3Result, dto::*, s3_error};

use crate::{
    iam::{Action, Context, Resource},
    state::AppState,
    store::{bucket, entities},
};

const ALL_USERS: &str = "http://acs.amazonaws.com/groups/global/AllUsers";
const AUTHENTICATED_USERS: &str = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";

type Right<'a> = (Option<&'a str>, &'a str);
const OWNER: Right<'_> = (None, "FULL_CONTROL");
const PUBLIC_READ: Right<'_> = (Some(ALL_USERS), "READ");
const PUBLIC_WRITE: Right<'_> = (Some(ALL_USERS), "WRITE");
const ACLS: &[(&str, &[Right<'_>])] = &[
    ("private", &[OWNER]),
    ("public-read", &[OWNER, PUBLIC_READ]),
    ("public-read-write", &[OWNER, PUBLIC_READ, PUBLIC_WRITE]),
    (
        "authenticated-read",
        &[OWNER, (Some(AUTHENTICATED_USERS), "READ")],
    ),
];

pub(super) fn canned(acl: &BucketCannedACL) -> S3Result<&'static str> {
    ACLS.iter()
        .find(|(name, _)| *name == acl.as_str())
        .map(|(name, _)| *name)
        .ok_or_else(|| s3_error!(InvalidArgument, "unsupported canned bucket ACL"))
}

fn allows(state: &AppState, user: &str, model: &entities::bucket::Model, action: Action) -> bool {
    crate::access::model_scope(state, Some(user), model).allows(&Context {
        action,
        resource: Resource::Bucket,
        version: None,
        request_tags: None,
        existing_tags: None,
    })
}

async fn authorized_bucket(
    state: &AppState,
    bucket_name: &str,
    user: &str,
    expected: Option<&str>,
    action: Action,
) -> S3Result<entities::bucket::Model> {
    let model = bucket::get(state.store.db(), bucket_name).await?;
    if expected.is_some_and(|id| model.owner.as_deref() != Some(id))
        || !allows(state, user, &model, action)
    {
        return Err(s3_error!(AccessDenied));
    }
    Ok(model)
}

fn xml_acl(policy: &AccessControlPolicy, owner: &str) -> S3Result<&'static str> {
    let invalid = || {
        s3_error!(
            InvalidArgument,
            "ACL must match a supported bucket grant set"
        )
    };
    if policy.owner.as_ref().and_then(|value| value.id.as_deref()) != Some(owner) {
        return Err(invalid());
    }
    let mut rights = BTreeSet::new();
    for grant in policy.grants.iter().flatten() {
        let grantee = grant.grantee.as_ref().ok_or_else(invalid)?;
        let permission = grant.permission.as_ref().ok_or_else(invalid)?.as_str();
        if grantee.email_address.is_some() {
            return Err(invalid());
        }
        let group = match (
            grantee.type_.as_str(),
            grantee.id.as_deref(),
            grantee.uri.as_deref(),
        ) {
            ("CanonicalUser", Some(id), None) if id == owner => None,
            ("Group", None, Some(uri)) => Some(uri),
            _ => return Err(invalid()),
        };
        rights.insert((group, permission));
    }
    ACLS.iter()
        .find(|(_, expected)| {
            rights.len() == expected.len() && expected.iter().all(|pair| rights.contains(pair))
        })
        .map(|(name, _)| *name)
        .ok_or_else(invalid)
}

pub async fn get_bucket_acl(
    state: &Arc<AppState>,
    req: S3Request<GetBucketAclInput>,
) -> S3Result<S3Response<GetBucketAclOutput>> {
    let user = super::signed_subject(state, &req)?;
    let model = authorized_bucket(
        state,
        &req.input.bucket,
        user,
        req.input.expected_bucket_owner.as_deref(),
        Action::GetBucketAcl,
    )
    .await?;
    let stored = model.canned_acl.ok_or_else(|| {
        s3_error!(
            InvalidBucketState,
            "Choose a bucket ACL with PutBucketAcl first"
        )
    })?;
    let (_, rights) = ACLS
        .iter()
        .find(|(name, _)| *name == stored)
        .ok_or_else(|| s3_error!(InternalError))?;
    let grants = rights
        .iter()
        .map(|&(group, permission)| Grant {
            grantee: Some(Grantee {
                type_: Type::from_static(if group.is_some() {
                    "Group"
                } else {
                    "CanonicalUser"
                }),
                id: group.is_none().then(|| user.to_owned()),
                uri: group.map(str::to_owned),
                display_name: None,
                email_address: None,
            }),
            permission: Some(Permission::from_static(permission)),
        })
        .collect();
    Ok(S3Response::new(GetBucketAclOutput {
        owner: Some(Owner {
            id: model.owner,
            display_name: None,
        }),
        grants: Some(grants),
    }))
}

pub async fn put_bucket_acl(
    state: &Arc<AppState>,
    req: S3Request<PutBucketAclInput>,
) -> S3Result<S3Response<PutBucketAclOutput>> {
    let user = super::signed_subject(state, &req)?;
    let input = &req.input;
    authorized_bucket(
        state,
        &input.bucket,
        user,
        input.expected_bucket_owner.as_deref(),
        Action::PutBucketAcl,
    )
    .await?;
    if input.grant_full_control.is_some()
        || input.grant_read.is_some()
        || input.grant_write.is_some()
        || input.grant_read_acp.is_some()
        || input.grant_write_acp.is_some()
    {
        return Err(s3_error!(
            InvalidArgument,
            "explicit grant headers are not supported"
        ));
    }
    let acl = match (&input.acl, &input.access_control_policy) {
        (Some(acl), None) => canned(acl)?,
        (None, Some(policy)) => xml_acl(policy, user)?,
        _ => {
            return Err(s3_error!(
                InvalidArgument,
                "supply either a canned ACL or an access control policy"
            ));
        }
    };
    bucket::set_acl_for_owner(state.store.db(), &input.bucket, user, acl).await?;
    Ok(S3Response::new(PutBucketAclOutput::default()))
}
