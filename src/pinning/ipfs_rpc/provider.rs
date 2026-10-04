use super::{IpfsRpcProvider, Pins, RpcProfile, cid, error};
use crate::pinning::provider::{
    FindPin, PinningProvider, ProviderError, ProviderErrorClass, QueryObservation, RemotePin,
    SubmitObservation, SubmitPin,
};

#[async_trait::async_trait]
impl PinningProvider for IpfsRpcProvider {
    fn name(&self) -> &str {
        &self.name
    }
    fn invocation_route(&self) -> (&'static str, &'static str) {
        (self.profile.api_profile(), self.strategy.as_str())
    }

    async fn submit(&self, request: SubmitPin) -> Result<RemotePin, ProviderError> {
        self.submit_observed(request).await.result
    }

    async fn submit_observed(&self, request: SubmitPin) -> SubmitObservation {
        IpfsRpcProvider::submit_observed(self, request).await.into()
    }

    async fn get(&self, request_id: &str) -> Result<RemotePin, ProviderError> {
        let cid = cid::decode(self.profile, request_id)?;
        self.find_verified(&cid)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| {
                error::error(
                    ProviderErrorClass::NotFound,
                    "RPC pin is authoritatively absent",
                )
            })
    }

    async fn find(&self, query: FindPin) -> Result<Vec<RemotePin>, ProviderError> {
        self.find_verified(&query.cid).await
    }

    /// Only invoke AFTER the outer ledger authorizes this exact resource. There
    /// is intentionally no automatic cleanup, bulk unpin or garbage collection.
    async fn unpin(&self, request_id: &str) -> Result<(), ProviderError> {
        let cid = cid::decode(self.profile, request_id)?;
        let filebase = [("arg", cid.as_str())];
        let kubo = [("arg", cid.as_str()), ("recursive", "true")];
        let query = if self.profile == RpcProfile::Filebase {
            &filebase[..]
        } else {
            &kubo[..]
        };
        let response = self.target.control("pin/rm", query, true).await?;
        response.check(true)?;
        let pins: Pins = response.json()?;
        if pins.pins.len() != 1 || !cid::equivalent(&pins.pins[0], &cid).unwrap_or(false) {
            return Err(error::protocol("RPC pin/rm did not confirm the exact root"));
        }
        Ok(())
    }

    async fn find_historical(
        &self,
        query: FindPin,
        api: &str,
        strategy: &str,
    ) -> Result<Vec<RemotePin>, ProviderError> {
        self.historical_route(api, strategy)?;
        self.find(query).await
    }

    async fn observe_historical(
        &self,
        query: FindPin,
        api: &str,
        strategy: &str,
    ) -> Result<QueryObservation, ProviderError> {
        self.historical_route(api, strategy)?;
        Ok(self.observe(query).await)
    }

    async fn get_historical(
        &self,
        id: &str,
        api: &str,
        strategy: &str,
    ) -> Result<RemotePin, ProviderError> {
        self.historical_route(api, strategy)?;
        self.get(id).await
    }

    async fn unpin_historical(
        &self,
        id: &str,
        api: &str,
        strategy: &str,
    ) -> Result<(), ProviderError> {
        self.historical_route(api, strategy)?;
        self.unpin(id).await
    }
}
