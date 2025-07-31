use crate::MoonlinkTableSecret;
use crate::StorageConfig;

/// This module contains a few filesystem wrappers, including timeout, retry, etc.
use more_asserts as ma;
use serde::{Deserialize, Serialize};

/// ========================
/// Retry config
/// ========================
///
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct RetryConfig {
    #[serde(default = "RetryConfig::default_max_count")]
    pub max_count: usize,

    #[serde(default = "RetryConfig::default_min_delay")]
    pub min_delay: std::time::Duration,

    #[serde(default = "RetryConfig::default_max_delay")]
    pub max_delay: std::time::Duration,

    #[serde(default = "RetryConfig::default_delay_factor")]
    pub delay_factor: f32,
}

impl RetryConfig {
    const DEFAULT_MIN_DELAY: std::time::Duration = std::time::Duration::from_millis(100);
    const DEFAULT_MAX_DELAY: std::time::Duration = std::time::Duration::from_secs(5);
    const DEFAULT_DELAY_FACTOR: f32 = 1.5;
    const DEFAULT_MAX_COUNT: usize = 5;

    // Util functions for serde defaults.
    fn default_max_count() -> usize {
        Self::DEFAULT_MAX_COUNT
    }
    fn default_min_delay() -> std::time::Duration {
        Self::DEFAULT_MIN_DELAY
    }
    fn default_max_delay() -> std::time::Duration {
        Self::DEFAULT_MAX_DELAY
    }
    fn default_delay_factor() -> f32 {
        Self::DEFAULT_DELAY_FACTOR
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_count: Self::DEFAULT_MAX_COUNT,
            min_delay: Self::DEFAULT_MIN_DELAY,
            max_delay: Self::DEFAULT_MAX_DELAY,
            delay_factor: Self::DEFAULT_DELAY_FACTOR,
        }
    }
}

/// ========================
/// Chaos config
/// ========================
///
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ChaosConfig {
    /// Min and max latency introduced to all operation access, both inclusive.
    pub min_latency: std::time::Duration,
    pub max_latency: std::time::Duration,

    /// Probability ranges from [0, err_prob]; if not 0, will return retriable opendal error randomly.
    pub err_prob: usize,
}

impl ChaosConfig {
    /// Validate whether the given option is valid.
    pub fn validate(&self) {
        ma::assert_le!(self.min_latency, self.max_latency);
        ma::assert_le!(self.err_prob, 100);
    }
}

/// ========================
/// Timeout config
/// ========================
///
/// TODO(hjiang): Allow finer-granularity timeout control.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct TimeoutConfig {
    /// Timeout for all attempts for an IO operations, including retry.
    #[serde(default = "TimeoutConfig::default_timeout")]
    pub timeout: std::time::Duration,
}

impl TimeoutConfig {
    /// Default timeout for all IO operations.
    const DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

    fn default_timeout() -> std::time::Duration {
        Self::DEFAULT_TIMEOUT
    }
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            timeout: Self::DEFAULT_TIMEOUT,
        }
    }
}

/// ========================
/// Accessor config
/// ========================
///
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct AccessorConfig {
    /// Internal storage config.
    pub storage_config: StorageConfig,
    /// Retry config.
    #[serde(default)]
    pub retry_config: RetryConfig,
    /// Timeout config.
    #[serde(default)]
    pub timeout_config: TimeoutConfig,
    /// Chaos config.
    #[serde(default)]
    pub chaos_config: Option<ChaosConfig>,
}

impl AccessorConfig {
    pub fn new_with_storage_config(storage_config: StorageConfig) -> Self {
        Self {
            storage_config,
            retry_config: RetryConfig::default(),
            timeout_config: TimeoutConfig::default(),
            chaos_config: None,
        }
    }

    pub fn get_root_path(&self) -> String {
        self.storage_config.get_root_path()
    }

    pub fn extract_security_metadata_entry(&self) -> Option<MoonlinkTableSecret> {
        self.storage_config.extract_security_metadata_entry()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StorageConfig;

    use serde_json::json;

    /// Testing scenario: deserialize accessor config with only storage config populated.
    #[test]
    fn test_deserialize_accessor_config_with_only_storage_config() {
        let input = json!({
            "storage_config": {
                "FileSystem": {
                    "root_directory": "/tmp"
                }
            }
        });

        let config: AccessorConfig = serde_json::from_value(input).unwrap();
        assert_eq!(
            config,
            AccessorConfig {
                storage_config: StorageConfig::FileSystem {
                    root_directory: "/tmp".to_string()
                },
                retry_config: RetryConfig::default(),
                timeout_config: TimeoutConfig::default(),
                chaos_config: None,
            }
        );
    }

    /// Testing scenario: only one field is specified for retry config.
    #[test]
    fn test_deserialize_retry_config() {
        let input = json!({
            "storage_config": {
                "FileSystem": {
                    "root_directory": "/tmp"
                },
            },
            "retry_config": {
                "delay_factor": 2
            }
        });

        let config: AccessorConfig = serde_json::from_value(input).unwrap();
        assert_eq!(
            config,
            AccessorConfig {
                storage_config: StorageConfig::FileSystem {
                    root_directory: "/tmp".to_string()
                },
                retry_config: RetryConfig {
                    delay_factor: 2.0,
                    max_count: RetryConfig::default_max_count(),
                    min_delay: RetryConfig::default_min_delay(),
                    max_delay: RetryConfig::default_max_delay(),
                },
                timeout_config: TimeoutConfig::default(),
                chaos_config: None,
            }
        );
    }
}
