use std::sync::Arc;

use async_trait::async_trait;
use crate::net::connect::host_and_port::HostAndPort;

use crate::net::connect::service_client::ServiceResponseError;

#[async_trait]
pub trait HeartbeatSender: Send + Sync {
    async fn send(&self, source_address: HostAndPort) -> Result<(), ServiceResponseError>;
}

pub type HeartbeatSenderType = Arc<dyn HeartbeatSender>;