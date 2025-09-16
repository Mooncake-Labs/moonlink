/// Cloud vendor security config.
///
/// AWS security config.
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub struct AwsSecurityConfig {
    #[serde(rename = "access_key_id")]
    #[serde(default)]
    pub access_key_id: String,

    #[serde(rename = "security_access_key")]
    #[serde(default)]
    pub security_access_key: String,

    #[serde(rename = "region")]
    #[serde(default)]
    pub region: String,
}

impl std::fmt::Debug for AwsSecurityConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsSecurityConfig")
            .field("access_key_id", &"xxxxx")
            .field("security_access_key", &"xxxx")
            .field("region", &self.region)
            .finish()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum CloudSecurityConfig {
    Aws(AwsSecurityConfig),
}

impl CloudSecurityConfig {
    /// Get AWS security config.
    pub fn get_aws_security_config(&self) -> Option<&AwsSecurityConfig> {
        match self {
            CloudSecurityConfig::Aws(config) => Some(config),
        }
    }
}
