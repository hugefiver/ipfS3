use super::super::super::ZipCompletedReplay;
use super::super::{CapturedZipPublication, invalid};
use crate::{
    error::AppResult,
    lifecycle::model::MultipartUploadTargetIdentity,
    store::{
        entities::{zip_batch, zip_root_reference},
        object_version::PublicationResult,
    },
};
use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter};
use std::collections::BTreeMap;

pub(super) async fn seal(
    tx: &DatabaseTransaction,
    request: &CapturedZipPublication,
    target: &MultipartUploadTargetIdentity,
    result: &PublicationResult,
    replay: &ZipCompletedReplay,
) -> AppResult<()> {
    let source = request.source.as_ref().ok_or_else(invalid)?;
    let sealed = zip_batch::Entity::find_by_id(&request.batch_id)
        .one(tx)
        .await?
        .ok_or_else(invalid)?;
    if sealed.state != "published" || sealed.source != "mpu" || sealed.owner != replay.owner {
        return Err(invalid());
    }
    let mut headers = BTreeMap::from([
        ("content-type".into(), "application/xml".into()),
        ("etag".into(), format!("\"{}\"", source.cid)),
        ("x-ipfs-s3-zip-batch-id".into(), request.batch_id.clone()),
        (
            "x-ipfs-s3-zip-root-status".into(),
            sealed.root_status.clone(),
        ),
    ]);
    if let Some(version) = &result.version_id {
        headers.insert("x-amz-version-id".into(), version.clone());
    }
    if let Some(sse) = &replay.server_side_encryption {
        headers.insert("x-amz-server-side-encryption".into(), sse.clone());
    }
    if let Some(warning) = &replay.pin_warning {
        headers.insert("x-ipfs3-pin-warning".into(), warning.clone());
    }
    if let Some(warning) = &sealed.root_error_code {
        headers.insert("x-ipfs-s3-zip-root-warning".into(), warning.clone());
    }
    if matches!(sealed.root_status.as_str(), "complete" | "partial") {
        let cid = sealed.root_cid.as_deref().ok_or_else(invalid)?;
        let adopted = zip_root_reference::Entity::find()
            .filter(zip_root_reference::Column::BatchId.eq(&request.batch_id))
            .filter(zip_root_reference::Column::Cid.eq(cid))
            .filter(zip_root_reference::Column::State.eq("adopted"))
            .filter(zip_root_reference::Column::VerificationReceipt.is_not_null())
            .one(tx)
            .await?;
        if adopted.is_none() {
            return Err(invalid());
        }
        headers.insert("x-ipfs-s3-zip-root-cid".into(), cid.into());
    }
    crate::store::zip::BatchAdmission::completed_upload_result(
        tx,
        &replay.owner,
        &source.bucket,
        &source.key,
        &target.upload_id,
        &replay.parts,
        &source.id,
        &source.cid,
        source.logical_size,
        result.version_id.as_deref(),
        replay.server_side_encryption.as_deref(),
        &replay.response_xml,
        headers,
    )
    .await
}
