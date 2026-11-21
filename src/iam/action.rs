use serde::Deserialize;

use super::Resource;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum Action {
    ListBuckets,
    CreateBucket,
    DeleteBucket,
    GetBucketAcl,
    PutBucketAcl,
    HeadBucket,
    GetBucketLocation,
    GetBucketVersioning,
    PutBucketVersioning,
    DeleteBucketLifecycle,
    GetBucketLifecycleConfiguration,
    PutBucketLifecycleConfiguration,
    GetBucketCors,
    PutBucketCors,
    DeleteBucketCors,
    PutObject,
    GetObject,
    HeadObject,
    DeleteObject,
    GetObjectTagging,
    PutObjectTagging,
    DeleteObjectTagging,
    CopyObject,
    ListObjects,
    ListObjectsV2,
    ListObjectVersions,
    CreateMultipartUpload,
    UploadPart,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    ListParts,
    GetPinningStatus,
    ImportObject,
    GetImportStatus,
    DecompressZip,
    GetDecompressZipStatus,
}

impl Action {
    pub(super) fn accepts_resource(self, resource: Resource<'_>) -> bool {
        use Action::*;

        match self {
            ListBuckets
            | CreateBucket
            | DeleteBucket
            | GetBucketAcl
            | PutBucketAcl
            | HeadBucket
            | GetBucketLocation
            | GetBucketVersioning
            | PutBucketVersioning
            | DeleteBucketLifecycle
            | GetBucketLifecycleConfiguration
            | PutBucketLifecycleConfiguration
            | GetBucketCors
            | PutBucketCors
            | DeleteBucketCors => matches!(resource, Resource::Bucket),
            ListObjects | ListObjectsV2 | ListObjectVersions => {
                matches!(resource, Resource::Prefix(_))
            }
            ImportObject
            | GetImportStatus
            | DecompressZip
            | GetDecompressZipStatus
            | CreateMultipartUpload
            | CompleteMultipartUpload => {
                matches!(resource, Resource::Object(_) | Resource::Prefix(_))
            }
            PutObject | GetObject | HeadObject | DeleteObject | GetObjectTagging
            | PutObjectTagging | DeleteObjectTagging | CopyObject | UploadPart
            | AbortMultipartUpload | ListParts | GetPinningStatus => {
                matches!(resource, Resource::Object(_))
            }
        }
    }

    pub(super) fn has_version(self) -> bool {
        matches!(
            self,
            Self::GetObject
                | Self::HeadObject
                | Self::DeleteObject
                | Self::GetObjectTagging
                | Self::PutObjectTagging
                | Self::DeleteObjectTagging
                | Self::GetPinningStatus
        )
    }

    pub(super) fn has_existing_tags(self) -> bool {
        matches!(
            self,
            Self::GetObject
                | Self::HeadObject
                | Self::DeleteObject
                | Self::GetObjectTagging
                | Self::PutObjectTagging
                | Self::DeleteObjectTagging
                | Self::GetPinningStatus
        )
    }

    pub(super) fn has_request_tags(self) -> bool {
        matches!(
            self,
            Self::PutObject
                | Self::PutObjectTagging
                | Self::CopyObject
                | Self::CreateMultipartUpload
                | Self::CompleteMultipartUpload
                | Self::ImportObject
                | Self::DecompressZip
        )
    }
}
