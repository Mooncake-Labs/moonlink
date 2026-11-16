pub(crate) mod base_cache;
pub mod cache_config;
pub(crate) mod cache_handle;
pub mod object_storage_cache;

#[cfg(test)]
mod state_tests;

#[cfg(test)]
mod local_file_optimization_state_tests;

#[cfg(test)]
mod budget_overflow_tests;

#[cfg(test)]
pub(crate) mod test_utils;
