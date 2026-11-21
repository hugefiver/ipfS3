use super::*;
use crate::kubo::directory::{
    DirectoryBuildError, DirectoryFile, MAX_FILES, build_unpinned_directory,
};
use crate::s3::ops::storage_class::{ObjectIdentity, classes_in_snapshot};
use crate::store::entities::object_tag;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect};

#[derive(sea_orm::FromQueryResult)]
struct DirectoryObject {
    #[sea_orm(nested)]
    listing: ListingObject,
    encrypted: bool,
}

pub(super) fn requested(req: &S3Request<ListObjectsV2Input>) -> S3Result<bool> {
    const FLAG: &str = "ipfs3-directory-cid";
    let mut flags = url::form_urlencoded::parse(req.uri.query().unwrap_or_default().as_bytes())
        .filter(|(name, _)| name == FLAG);
    let Some((_, value)) = flags.next() else {
        return Ok(false);
    };
    // s3s supplies a credential region only for SigV4. SigV2 does not bind custom query fields.
    if req.credentials.is_none() || req.region.is_none() {
        return Err(s3s::s3_error!(AccessDenied, "Directory CID requires SigV4"));
    }
    if value != "true" || flags.next().is_some() {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "expected ipfs3-directory-cid=true exactly once"
        ));
    }
    if !req
        .input
        .prefix
        .as_deref()
        .is_some_and(|prefix| prefix.ends_with('/'))
    {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "directory prefix must end in /"
        ));
    }
    if req.input.continuation_token.is_some() || req.input.start_after.is_some() {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "Directory CID requires the first page"
        ));
    }
    Ok(true)
}

pub(super) async fn snapshot(
    db: &impl ConnectionTrait,
    request: ListingRequest<'_>,
    scope: &Scope<'_>,
) -> S3Result<(
    ListingPage,
    Vec<DirectoryFile>,
    HashMap<String, crate::residency::StorageClass>,
)> {
    let rows = crate::store::object::listing_query(
        db.get_database_backend(),
        request.bucket,
        Some(request.prefix),
        None,
        MAX_FILES as u64 + 1,
    )
    .column(object::Column::Encrypted)
    .into_model::<DirectoryObject>()
    .all(db)
    .await
    .map_err(AppError::from)?;
    if rows.is_empty() {
        return Err(s3s::s3_error!(
            NoSuchKey,
            "directory prefix has no current objects"
        ));
    }
    if rows.len() > MAX_FILES {
        return Err(s3s::s3_error!(
            InvalidRequest,
            "directory file limit exceeded"
        ));
    }
    authorize_files(db, scope, &rows).await?;
    if rows.iter().any(|row| row.encrypted) {
        return Err(s3s::s3_error!(
            InvalidRequest,
            "directory contains encrypted content"
        ));
    }
    let objects = rows
        .iter()
        .map(|row| ObjectIdentity {
            id: &row.listing.id,
            bucket: &row.listing.bucket,
            key: &row.listing.key,
            cid: &row.listing.cid,
        })
        .collect::<Vec<_>>();
    let classes = classes_in_snapshot(db, &objects).await?;
    if classes
        .values()
        .any(|class| *class != crate::residency::StorageClass::Standard)
    {
        return Err(s3s::s3_error!(
            InvalidObjectState,
            "directory contains cold content"
        ));
    }
    let mut builder = ListingPageBuilder::new(request.prefix, request.delimiter, request.max_keys);
    let mut files = Vec::with_capacity(rows.len());
    for DirectoryObject { listing: row, .. } in rows {
        files.push(DirectoryFile {
            path: row.key[request.prefix.len()..].to_owned(),
            cid: row.cid.clone(),
        });
        if !builder.is_truncated {
            builder.push_row(row);
        }
    }
    Ok((builder.finish(), files, classes))
}

async fn authorize_files(
    db: &impl ConnectionTrait,
    scope: &Scope<'_>,
    rows: &[DirectoryObject],
) -> S3Result<()> {
    let mut pending = Vec::new();
    for DirectoryObject { listing: row, .. } in rows {
        let context = object_access_context(Action::GetObject, &row.key, None);
        match scope.decision(&context) {
            Decision::Allow => (),
            Decision::Deny => return Err(object_policy_denied().into()),
            Decision::NeedsExistingTags => pending.push((&row.id, context)),
        }
    }
    for chunk in pending.chunks(250) {
        let ids = chunk.iter().map(|(id, _)| id.as_str());
        let tags = object_tag::Entity::find()
            .filter(object_tag::Column::ObjectId.is_in(ids))
            .all(db)
            .await
            .map_err(AppError::from)?;
        let mut by_object = HashMap::<String, Vec<ObjectTag>>::new();
        for tag in tags {
            by_object
                .entry(tag.object_id)
                .or_default()
                .push(ObjectTag::new(tag.key, tag.value));
        }
        for (id, context) in chunk {
            let tags = by_object.get(*id).map(Vec::as_slice).unwrap_or_default();
            check_object_policy(scope, context, None, Some(tags))?;
        }
    }
    Ok(())
}

pub(super) async fn build_header(
    kubo: &crate::kubo::KuboClient,
    files: &[DirectoryFile],
) -> S3Result<http::HeaderValue> {
    let root = build_unpinned_directory(kubo, files)
        .await
        .map_err(|err| match err {
            DirectoryBuildError::PathConflict
            | DirectoryBuildError::InvalidManifest
            | DirectoryBuildError::BlockTooLarge
            | DirectoryBuildError::HashCollision => s3s::s3_error!(
                InvalidRequest,
                "prefix cannot be represented as a bounded UnixFS directory"
            ),
            DirectoryBuildError::Kubo(err) => err.into(),
            _ => s3s::s3_error!(ServiceUnavailable, "directory build did not complete"),
        })?;
    Ok(http::HeaderValue::from_str(&root.to_string()).expect("CID is an ASCII header value"))
}
