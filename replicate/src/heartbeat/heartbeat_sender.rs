use std::sync::Arc;

use async_trait::async_trait;

use crate::net::connect::service_client::ServiceResponseError;

#[async_trait]
pub trait HeartbeatSender: Send + Sync {
    async fn send_heartbeat(&self) -> Result<(), ServiceResponseError>;
}

pub type HeartbeatSenderType = Arc<dyn HeartbeatSender>;