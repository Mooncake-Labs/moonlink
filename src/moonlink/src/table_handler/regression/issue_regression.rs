use crate::table_handler::chaos_replay;

#[tokio::test]
async fn test_issue_1793() {
    let replay_event_filepath = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/table_handler/regression/issue_1793_replay_events.json");
    chaos_replay::replay(replay_event_filepath.as_path().to_str().unwrap()).await;
}

#[tokio::test]
async fn test_issue_1834() {
    let replay_event_filepath = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/table_handler/regression/issue_1834_replay_events.json");
    chaos_replay::replay(replay_event_filepath.as_path().to_str().unwrap()).await;
}

#[tokio::test]
async fn test_issue_2049() {
    let replay_event_filepath = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/table_handler/regression/issue_2049_replay_events.json");
    chaos_replay::replay(replay_event_filepath.as_path().to_str().unwrap()).await;
}
