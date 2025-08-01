/// A stream writer, which doesn't maintain a buffer inside.
use crate::storage::filesystem::accessor::base_unbuffered_stream_writer::BaseUnbufferedStreamWriter;
use crate::Result;

use async_trait::async_trait;
use opendal::Operator;
use tokio::sync::mpsc::Sender;

/// Max number of outstanding multipart writes.
const MAX_CONCURRENT_WRITRS: usize = 32;
/// Channel size for foreground/background communication.
const CHANNEL_SIZE: usize = 32;

pub struct UnbufferedStreamWriter {
    request_tx: Sender<Vec<u8>>,
    background_task: tokio::task::JoinHandle<Result<()>>,
}

impl UnbufferedStreamWriter {
    /// # Arguments
    ///
    /// * object_filepath: filepath relative to operator root path.
    pub fn new(operator: Operator, object_filepath: String) -> Result<Self> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(CHANNEL_SIZE);
        let background_task = tokio::spawn(async move {
            let mut writer = operator
                .writer_with(&object_filepath)
                .concurrent(MAX_CONCURRENT_WRITRS)
                .await?;
            while let Some(buf) = rx.recv().await {
                writer.write(buf).await?;
            }
            writer.close().await?;
            Ok(())
        });

        Ok(Self {
            request_tx: tx,
            background_task,
        })
    }
}

#[async_trait]
impl BaseUnbufferedStreamWriter for UnbufferedStreamWriter {
    async fn append_non_blocking(&mut self, data: Vec<u8>) -> Result<()> {
        self.request_tx.send(data).await.unwrap();
        Ok(())
    }

    async fn finalize(self: Box<Self>) -> Result<()> {
        drop(self.request_tx);
        self.background_task.await??;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::filesystem::accessor::operator_utils;
    use crate::storage::filesystem::accessor_config::AccessorConfig;
    use crate::storage::filesystem::test_utils::writer_test_utils::*;
    use crate::storage::StorageConfig;

    #[tokio::test]
    async fn test_unbuffered_stream_writer() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root_directory = temp_dir.path().to_str().unwrap().to_string();
        let dst_filename = "dst".to_string();

        // Create an operator.
        let storage_config = StorageConfig::FileSystem { root_directory };
        let accessor_config = AccessorConfig::new_with_storage_config(storage_config.clone());
        let operator = operator_utils::create_opendal_operator(&accessor_config)
            .await
            .unwrap();

        // Create writer and append in blocks.
        let writer =
            Box::new(UnbufferedStreamWriter::new(operator.clone(), dst_filename.clone()).unwrap());
        test_unbuffered_stream_writer_impl(writer, dst_filename, accessor_config).await;
    }
}
