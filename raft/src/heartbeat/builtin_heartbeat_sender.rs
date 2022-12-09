use std::sync::Arc;
use async_trait::async_trait;

use crate::heartbeat::heartbeat_sender::HeartbeatSender;
use crate::net::connect::async_network::AsyncNetwork;
use crate::net::connect::correlation_id::CorrelationIdGenerator;
use crate::net::connect::host_and_port::HostAndPort;
use crate::net::connect::service::heartbeat::service_request::HeartbeatServiceRequest;
use crate::net::connect::service_client::ServiceResponseError;

pub struct BuiltinHeartbeatSender {
    address: Arc<HostAndPort>,
}

#[async_trait]
impl HeartbeatSender for BuiltinHeartbeatSender {
    async fn send(&self) -> Result<(), ServiceResponseError> {
        let node_id = "mark";
        let service_server_request = HeartbeatServiceRequest::new(
            node_id.to_string(),
            CorrelationIdGenerator::new()
        );
        let address = self.address.clone();
        let result = AsyncNetwork::send(service_server_request, address.clone()).await;
        if result.is_err() {
            eprintln!("Could not send heartbeat to {}", address.as_string());
        }
        return result;
    }
}

impl BuiltinHeartbeatSender {
    pub fn new(address: Arc<HostAndPort>) -> BuiltinHeartbeatSender {
        return BuiltinHeartbeatSender { address };
    }
}