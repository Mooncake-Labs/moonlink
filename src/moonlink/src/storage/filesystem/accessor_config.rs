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
    /// Random seed; if unassigned, use current timestamp as random seed.
    pub random_seed: Option<u64>,

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
/// Throttle config
/// ========================
///
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ThrottleConfig {
    /// Bandwidth in bytes per second
    #[serde(default = "ThrottleConfig::default_bandwidth")]
    pub bandwidth: u32,

    /// Burst size in bytes
    #[serde(default = "ThrottleConfig::default_burst")]
    pub burst: u32,
}

impl ThrottleConfig {
    const DEFAULT_BANDWIDTH: u32 = 10 * 1024 * 1024; // 10MB/s
    const DEFAULT_BURST: u32 = 100 * 1024 * 1024; // 100MB

    fn default_bandwidth() -> u32 {
        Self::DEFAULT_BANDWIDTH
    }

    fn default_burst() -> u32 {
        Self::DEFAULT_BURST
    }
}

impl Default for ThrottleConfig {
    fn default() -> Self {
        Self {
            bandwidth: Self::DEFAULT_BANDWIDTH,
            burst: Self::DEFAULT_BURST,
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
    /// Throttle config.
    #[serde(default)]
    pub throttle_config: Option<ThrottleConfig>,
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
            throttle_config: None,
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
                "fs": {
                    "root_directory": "/tmp"
                }
            }
        });

        let config: AccessorConfig = serde_json::from_value(input).unwrap();
        assert_eq!(
            config,
            AccessorConfig {
                storage_config: StorageConfig::FileSystem {
                    root_directory: "/tmp".to_string(),
                    atomic_write_dir: None,
                },
                retry_config: RetryConfig::default(),
                timeout_config: TimeoutConfig::default(),
                throttle_config: None,
                chaos_config: None,
            }
        );
    }

    /// Testing scenario: only one field is specified for retry config.
    #[test]
    fn test_deserialize_retry_config() {
        let input = json!({
            "storage_config": {
                "fs": {
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
                    root_directory: "/tmp".to_string(),
                    atomic_write_dir: None,
                },
                retry_config: RetryConfig {
                    delay_factor: 2.0,
                    max_count: RetryConfig::default_max_count(),
                    min_delay: RetryConfig::default_min_delay(),
                    max_delay: RetryConfig::default_max_delay(),
                },
                timeout_config: TimeoutConfig::default(),
                throttle_config: None,
                chaos_config: None,
            }
        );
    }

    /// Testing scenario: deserialize throttle config with custom values.
    #[test]
    fn test_deserialize_throttle_config() {
        let input = json!({
            "storage_config": {
                "fs": {
                    "root_directory": "/tmp"
                }
            },
            "throttle_config": {
                "bandwidth": 5242880,  // 5MB/s
                "burst": 52428800      // 50MB
            }
        });

        let config: AccessorConfig = serde_json::from_value(input).unwrap();
        assert_eq!(
            config,
            AccessorConfig {
                storage_config: StorageConfig::FileSystem {
                    root_directory: "/tmp".to_string(),
                    atomic_write_dir: None,
                },
                retry_config: RetryConfig::default(),
                timeout_config: TimeoutConfig::default(),
                throttle_config: Some(ThrottleConfig {
                    bandwidth: 5242880, // 5MB/s
                    burst: 52428800,    // 50MB
                }),
                chaos_config: None,
            }
        );
    }
    /// Testing scenario: throttle config with default values.
    #[test]
    fn test_throttle_config_defaults() {
        let config = ThrottleConfig::default();
        assert_eq!(config.bandwidth, 10 * 1024 * 1024); // 10MB/s
        assert_eq!(config.burst, 100 * 1024 * 1024); // 100MB
    }

    /// Testing scenario: deserialize accessor config with throttle_config as null.
    #[test]
    fn test_deserialize_accessor_config_throttle_none() {
        let input = json!({
            "storage_config": {
                "fs": {
                    "root_directory": "/tmp"
                }
            },
            "throttle_config": null
        });

        let config: AccessorConfig = serde_json::from_value(input).unwrap();
        assert_eq!(config.throttle_config, None);
    }
}
