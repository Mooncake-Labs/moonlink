use opendal::Result;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
/// A chaos generator, which creates delay and error status based on config and random generator.
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

use crate::storage::filesystem::accessor_config::ChaosConfig;

#[derive(Clone, Debug)]
pub(crate) struct ChaosGenerator {
    /// Randomness.
    rng: Arc<Mutex<StdRng>>,
    /// Chao layer option.
    option: ChaosConfig,
}

impl ChaosGenerator {
    pub(crate) fn new(option: ChaosConfig) -> Self {
        option.validate();
        let random_seed = if let Some(random_seed) = option.random_seed {
            random_seed
        } else {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            nanos as u64
        };
        let rng = Arc::new(Mutex::new(StdRng::seed_from_u64(random_seed)));

        Self { rng, option }
    }

    /// Get random latency.
    async fn get_random_duration(&self) -> std::time::Duration {
        let mut rng = self.rng.lock().await;
        let min_ns = self.option.min_latency.as_nanos();
        let max_ns = self.option.max_latency.as_nanos();
        let sampled_ns = rng.random_range(min_ns..=max_ns);
        std::time::Duration::from_nanos(sampled_ns as u64)
    }

    /// Get random error.
    async fn get_random_error(&self) -> Result<()> {
        if self.option.err_prob == 0 {
            return Ok(());
        }

        let mut rng = self.rng.lock().await;
        let rand_val: usize = rng.random_range(0..=100);
        if rand_val <= self.option.err_prob {
            let err = opendal::Error::new(opendal::ErrorKind::Unexpected, "Injected error")
                .set_temporary();
            return Err(err);
        }

        Ok(())
    }

    /// Attempt injected delay and error.
    pub(crate) async fn perform_wrapper_function(&self) -> Result<()> {
        // Introduce latency for IO operations.
        let latency = self.get_random_duration().await;
        tokio::time::sleep(latency).await;

        // Get injected error status.
        self.get_random_error().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_no_delay_no_error() {
        let option = ChaosConfig {
            random_seed: None,
            min_latency: std::time::Duration::from_millis(0),
            max_latency: std::time::Duration::from_millis(0),
            err_prob: 0,
        };
        let generator = ChaosGenerator::new(option);
        generator.perform_wrapper_function().await.unwrap();
    }

    #[tokio::test]
    async fn test_delay_no_error() {
        let option = ChaosConfig {
            random_seed: None,
            min_latency: std::time::Duration::from_millis(100),
            max_latency: std::time::Duration::from_millis(200),
            err_prob: 0,
        };
        let generator = ChaosGenerator::new(option);
        generator.perform_wrapper_function().await.unwrap();
    }

    #[tokio::test]
    async fn test_always_error_no_delay() {
        const ATTEMPT_COUNT: usize = 10;

        let option = ChaosConfig {
            random_seed: None,
            min_latency: std::time::Duration::from_millis(0),
            max_latency: std::time::Duration::from_millis(0),
            err_prob: 100,
        };
        let generator = ChaosGenerator::new(option);
        for _ in 0..ATTEMPT_COUNT {
            let res = generator.perform_wrapper_function().await;
            assert!(res.is_err())
        }
    }
}
