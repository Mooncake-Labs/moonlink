use super::MokaCache;

#[cfg(test)]
impl<K, V> MokaCache<K, V>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub async fn initialize_for_test(&self, entries: Vec<(K, V)>) {
        for (k, v) in entries {
            self.cache.insert(k, v).await;
        }
    }

    pub async fn dump_all_for_test(&self) -> Vec<(K, V)> {
        self.cache
            .iter()
            .map(|(k_arc, v)| ((*k_arc).clone(), v.clone()))
            .collect()
    }

    pub async fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks().await;
    }
}
