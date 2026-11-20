pub mod decompress_zip;
pub mod gateway;
pub mod import_object;

use s3s::S3Result;

use crate::{
    iam::{Action, Context, Resource, Scope},
    pinning::tags::ObjectTag,
    zip::options::ZipV2Options,
};

pub(crate) fn authorize_zip(
    scope: &Scope<'_>,
    action: Action,
    key: &str,
    prefix: Option<&str>,
    v2: Option<&ZipV2Options>,
    tags: Option<&[ObjectTag]>,
) -> S3Result<()> {
    if scope.is_unrestricted() {
        return Ok(());
    }
    let resources = [
        v2.is_none_or(|options| options.publish_source)
            .then_some(Resource::Object(key)),
        prefix
            .filter(|_| v2.is_none_or(|options| options.publish_extracted))
            .map(Resource::Prefix),
    ];
    for resource in resources.into_iter().flatten() {
        if !scope.allows(&Context {
            action,
            resource,
            version: None,
            request_tags: tags,
            existing_tags: None,
        }) {
            return Err(s3s::s3_error!(AccessDenied));
        }
    }
    Ok(())
}

fn authorize_zip_status(
    scope: &Scope<'_>,
    action: Action,
    key: &str,
    capture: &str,
    v2: bool,
) -> S3Result<()> {
    if scope.is_unrestricted() {
        return Ok(());
    }
    let invalid = || s3s::s3_error!(InternalError, "invalid captured ZIP publication scope");
    let options = v2
        .then(|| crate::zip::options::decode_historical_zip_options(capture, None))
        .transpose()
        .map_err(|_| invalid())?
        .map(|capture| capture.options);
    let captured: serde_json::Value = serde_json::from_str(capture).map_err(|_| invalid())?;
    let prefix = if options
        .as_ref()
        .is_none_or(|options| options.publish_extracted)
    {
        Some(captured["target_prefix"].as_str().ok_or_else(invalid)?)
    } else {
        None
    };
    authorize_zip(scope, action, key, prefix, options.as_ref(), None)
}
