//! Same Config validation/normalization as service startup; no provider factory.
use crate::{config::Config, pinning::{config::ValidatedPinningConfig, coordinator::normalize_validated_config, policy::PinPolicyEvaluator, zip_policy::ValidatedZipOutputRules}};
use super::SafeError;

pub struct ManagementConfig {
    pub pinning: ValidatedPinningConfig,
    pub policy: PinPolicyEvaluator,
    pub zip_rules: ValidatedZipOutputRules,
    pub(crate) source_endpoint: String,
    source_node: Option<String>,
}
impl ManagementConfig {
    /// Explicit local claim; preparation independently reads and verifies this
    /// node before/after. No claim alone establishes readability or capability.
    pub fn with_source_node(mut self,node:String)->Result<Self,SafeError> {
        if node.is_empty() || node.len()>256 || node.chars().any(char::is_control) { return Err(SafeError::InvalidInput); }
        self.source_node=Some(node); Ok(self)
    }
    pub fn source_node(&self)->Option<&str> { self.source_node.as_deref() }
}
pub fn compile(config: &Config, get_env: impl Fn(&str) -> Option<String>) -> Result<ManagementConfig, SafeError> {
    config.validate_kubo().map_err(|_| SafeError::SafetyProhibition)?;
    let pinning = normalize_validated_config(ValidatedPinningConfig::from_config(config,get_env).map_err(|_| SafeError::SafetyProhibition)?).map_err(|_| SafeError::SafetyProhibition)?;
    let policy = PinPolicyEvaluator::with_mode(&pinning,config.pinning_control.unavailable);
    let zip_rules = ValidatedZipOutputRules::from_config(&config.decompress_zip,&pinning).map_err(|_| SafeError::SafetyProhibition)?;
    Ok(ManagementConfig { pinning, policy, zip_rules, source_endpoint: config.kubo.rpc_url.clone(),source_node:None })
}
