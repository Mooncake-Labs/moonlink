/// This module interacts with iceberg snapshot status.
use tokio::sync::mpsc;

/// At most one outstanding snapshot request is allowed.
pub struct IcebergSnapshotStateManager {
    /// Used to initiate an iceberg snapshot event.
    snapshot_initiation_sender: mpsc::Sender<()>,
    /// Used to subscribe to the initiation for an iceberg snapshot event.
    snapshot_initiation_receiver: Option<mpsc::Receiver<()>>,
    /// Used to notify the completion of an iceberg snapshot.
    snapshot_completion_sender: mpsc::Sender<()>,
    /// Used to synchronize on the completion of an iceberg snapshot.
    snapshot_completion_receiver: mpsc::Receiver<()>,
}

impl Default for IcebergSnapshotStateManager {
    fn default() -> Self {
        Self::new()
    }
}

impl IcebergSnapshotStateManager {
    pub fn new() -> Self {
        let (snapshot_completion_sender, snapshot_completion_receiver) = mpsc::channel(1);
        let (snapshot_initiation_sender, snapshot_initiation_receiver) = mpsc::channel(1);
        Self {
            snapshot_initiation_sender,
            snapshot_initiation_receiver: Some(snapshot_initiation_receiver),
            snapshot_completion_sender,
            snapshot_completion_receiver,
        }
    }

    /// Get snapshot completion event sender.
    pub fn get_snapshot_completion_sender(&self) -> mpsc::Sender<()> {
        self.snapshot_completion_sender.clone()
    }

    /// Synchronize on iceberg snapshot completion.
    pub async fn sync_snapshot_completion(&mut self) {
        self.snapshot_completion_receiver.recv().await.unwrap()
    }

    /// Initiate an iceberg snapshot event.
    pub async fn initiate_snapshot(&mut self) {
        self.snapshot_initiation_sender.send(()).await.unwrap()
    }

    /// Take the ownership for snapshot initiate receiver.
    pub fn take_snapshot_initiation_receiver(&mut self) -> Option<mpsc::Receiver<()>> {
        self.snapshot_initiation_receiver.take()
    }
}
