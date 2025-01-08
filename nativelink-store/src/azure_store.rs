use std::borrow::Cow;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use azure_core::auth::TokenCredential;
use azure_core::prelude::*;
use azure_identity::DefaultAzureCredential;
use azure_storage::blob::prelude::*;
use azure_storage::core::prelude::*;
use bytes::Bytes;
use futures::stream::StreamExt;
use nativelink_config::stores::AzureSpec;
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{make_buf_channel_pair, DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::health_utils::{default_health_status_indicator, HealthStatusIndicator};
use nativelink_util::store_trait::{StoreDriver, StoreKey, UploadSizeInfo};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{event, Level};

#[derive(MetricsComponent)]
pub struct AzureStore {
    container_client: Arc<ContainerClient>,
    credential: Arc<dyn TokenCredential>,
    #[metric(help = "The container name for the Azure store")]
    container_name: String,
    #[metric(help = "The key prefix for the Azure store")]
    key_prefix: String,
    #[metric(help = "The number of bytes to buffer for retrying requests")]
    max_retry_buffer_per_request: usize,
}

impl AzureStore {
    pub async fn new(spec: &AzureSpec) -> Result<Arc<Self>, Error> {
        let credential = Arc::new(DefaultAzureCredential::default());
        let container_client = Arc::new(
            StorageAccountClient::new_access_key(
                &spec.account_name,
                &spec.account_key,
            )
            .as_container_client(&spec.container_name),
        );

        Ok(Arc::new(Self {
            container_client,
            credential,
            container_name: spec.container_name.clone(),
            key_prefix: spec.key_prefix.as_ref().unwrap_or(&String::new()).clone(),
            max_retry_buffer_per_request: spec
                .max_retry_buffer_per_request
                .unwrap_or(5 * 1024 * 1024), // 5MB
        }))
    }

    fn make_azure_path(&self, key: &StoreKey<'_>) -> String {
        format!("{}{}", self.key_prefix, key.as_str())
    }

    async fn has(self: Pin<&Self>, digest: &StoreKey<'_>) -> Result<Option<u64>, Error> {
        let blob_client = self
            .container_client
            .as_blob_client(&self.make_azure_path(digest));

        match blob_client.get_properties().await {
            Ok(properties) => Ok(Some(properties.blob.properties.content_length)),
            Err(e) => match e.kind() {
                azure_core::error::ErrorKind::ResourceNotFound => Ok(None),
                _ => Err(make_err!(Code::Unavailable, "Azure error: {e:?}")),
            },
        }
    }
}

#[async_trait]
impl StoreDriver for AzureStore {
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        keys.iter()
            .zip(results.iter_mut())
            .map(|(key, result)| async move {
                *result = self.has(key).await?;
                Ok::<_, Error>(())
            })
            .collect::<futures::stream::FuturesUnordered<_>>()
            .try_collect()
            .await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        let blob_client = self
            .container_client
            .as_blob_client(&self.make_azure_path(&key));

        let mut block_list = Vec::new();
        let mut block_id = 0;

        loop {
            let chunk = reader
                .consume(Some(self.max_retry_buffer_per_request))
                .await
                .err_tip(|| "Failed to read chunk in azure_store")?;
            if chunk.is_empty() {
                break; // EOF
            }

            let block_id_str = format!("{:032}", block_id);
            block_list.push(BlockId::new(block_id_str.clone()));

            blob_client
                .put_block(block_id_str, chunk.clone())
                .await
                .err_tip(|| "Failed to upload block to Azure Blob Storage")?;

            block_id += 1;
        }

        blob_client
            .put_block_list(block_list)
            .await
            .err_tip(|| "Failed to commit block list to Azure Blob Storage")?;

        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let blob_client = self
            .container_client
            .as_blob_client(&self.make_azure_path(&key));

        let mut stream = blob_client
            .get()
            .range(offset..length.map_or(u64::MAX, |l| offset + l))
            .stream();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.err_tip(|| "Failed to read chunk from Azure Blob Storage")?;
            writer
                .send(Bytes::from(chunk))
                .await
                .err_tip(|| "Failed to send chunk to writer in azure_store")?;
        }

        writer
            .send_eof()
            .err_tip(|| "Failed to send EOF in azure_store get_part")?;

        Ok(())
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any(&self) -> &(dyn std::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Sync + Send + 'static> {
        self
    }
}

default_health_status_indicator!(AzureStore);
