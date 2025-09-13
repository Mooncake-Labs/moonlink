pub(crate) struct TestGuard {
    dir: String,
}

impl TestGuard {
    pub(crate) fn new(dir: &str) -> Self {
        Self::cleanup_sync(dir);
        Self {
            dir: dir.to_string(),
        }
    }

    fn cleanup_sync(dir: &str) {
        let dir = std::path::Path::new(dir);
        if dir.exists() {
            for entry in std::fs::read_dir(dir).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                if path.is_dir() {
                    Self::cleanup_sync(path.to_str().unwrap());
                    std::fs::remove_dir_all(path).unwrap();
                } else {
                    std::fs::remove_file(path).unwrap();
                }
            }
        }
    }
}

impl Drop for TestGuard {
    fn drop(&mut self) {
        Self::cleanup_sync(&self.dir);
    }
}
