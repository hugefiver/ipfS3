//! Cluster v1.1.6 write transport.
mod add_response;
mod car;
mod cbor;
mod dagpb;
mod http;
mod options;
mod remove;
mod response;
mod stream;
mod submit;

pub use super::error::{ClusterFailureCode, ClusterWriteFailure};
pub(crate) use car::{Car as CarValidator, DirectoryCarLimits, DirectoryRootKind};
pub use options::{
    ClusterProtocol, ClusterProviderOptions, ClusterSource, ClusterStrategy, ClusterTransferLimits,
    ClusterUnixfsOptions,
};

use super::{ClusterNetworkPolicy, ClusterResourceKey};
use ClusterFailureCode as Code;
use ClusterWriteFailure as Failure;
use cid::Cid;
use http::Http;

#[derive(Debug)]
pub struct ClusterWriteReceipt {
    pub resource: ClusterResourceKey,
    pub request_id: String,
}

pub struct ClusterProvider {
    options: ClusterProviderOptions,
    target: Http,
    source: Option<Http>,
}

impl ClusterProvider {
    pub fn new(options: ClusterProviderOptions) -> Result<Self, ClusterWriteFailure> {
        let (rest, proxy, source) = options.validate()?;
        let policy =
            ClusterNetworkPolicy::from_allow_private_network(options.allow_private_network);
        let (target, auth) = match proxy {
            Some(proxy) => (proxy, options.proxy_auth.as_ref()),
            None => (rest, options.rest_auth.as_ref()),
        };
        let target = Http::new(target, auth, policy, options.transfer_limits)?;
        let source = source
            .zip(options.source.as_ref())
            .map(|(url, source)| {
                Http::new(url, source.auth.as_ref(), policy, options.transfer_limits)
            })
            .transpose()?;
        Ok(Self {
            options,
            target,
            source,
        })
    }

    pub fn resource_key(&self, cid: &str) -> Result<ClusterResourceKey, Failure> {
        let cid = cid.parse().map_err(|_| Failure::new(Code::InvalidCid))?;
        Ok(ClusterResourceKey {
            pinset: self.options.pinset.clone(),
            cid: canonical_cid(&cid).to_string(),
        })
    }
}

fn canonical_cid(cid: &Cid) -> Cid {
    Cid::new_v1(cid.codec(), *cid.hash())
}
