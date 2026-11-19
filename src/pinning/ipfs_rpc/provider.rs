use super::{IpfsRpcProvider, RpcProfile, RpcStrategy, cid, error, transport::ControlResponse};
use crate::pinning::provider::{
    PinningProvider, ProviderError, RemotePin, RemotePinStatus, SubmitPin,
};
use ::cid::Cid;
use serde::Deserialize;

#[async_trait::async_trait]
impl PinningProvider for IpfsRpcProvider {
    fn name(&self) -> &str {
        &self.name
    }

    async fn submit(&self, request: SubmitPin) -> Result<RemotePin, ProviderError> {
        let requested = cid::parse(&request.cid)?;
        let canonical = cid::canonical(&requested);
        let pinned = match self.strategy {
            RpcStrategy::Cid => self.pin_cid(&canonical).await?,
            RpcStrategy::Upload => self.upload(&requested).await?,
            RpcStrategy::Car => self.import_car(&canonical).await?,
        };
        Ok(RemotePin {
            // Kubo's pin index distinguishes CIDv0 and CIDv1 for the same block.
            request_id: format!("rpc-pin:{}:{pinned}", self.profile.id()),
            cid: canonical.to_string(),
            status: RemotePinStatus::Pinned,
            raw_status: "pinned".into(),
            failure_reason: None,
        })
    }

    /// The caller selects the provider and authorizes removal of this handle.
    async fn unpin(&self, request_id: &str) -> Result<(), ProviderError> {
        let cid = cid::decode(self.profile, request_id)?;
        let arg = cid.to_string();
        let query = [("arg", arg.as_str()), ("recursive", "true")];
        let response = self
            .target
            .control(
                "pin/rm",
                if self.profile == RpcProfile::Filebase {
                    &query[..1]
                } else {
                    &query
                },
                true,
            )
            .await?;
        if pins_ack(response)? != cid {
            return Err(error::mismatch());
        }
        Ok(())
    }
}

impl IpfsRpcProvider {
    async fn pin_cid(&self, requested: &Cid) -> Result<Cid, ProviderError> {
        let arg = requested.to_string();
        let response = self
            .target
            .control(
                "pin/add",
                &[("arg", &arg), ("recursive", "true"), ("progress", "false")],
                true,
            )
            .await?;
        let pinned = pins_ack(response)?;
        if !cid::equivalent(&pinned, requested) {
            return Err(error::mismatch());
        }
        Ok(pinned)
    }
}

#[derive(Deserialize)]
struct Pins {
    #[serde(rename = "Pins")]
    pins: Vec<String>,
}

fn pins_ack(response: ControlResponse) -> Result<Cid, ProviderError> {
    response.check(true)?;
    let pins: Pins = response.json()?;
    let [pin] = pins.pins.as_slice() else {
        return Err(error::protocol("RPC pin response did not contain one root"));
    };
    cid::parse_ack(pin)
}
