use async_trait::async_trait;

use crate::heartbeat::heartbeat_sender::HeartbeatSender;
use crate::net::connect::async_network::AsyncNetwork;
use crate::net::connect::host_and_port::HostAndPort;
use crate::net::connect::random_correlation_id_generator::RandomCorrelationIdGenerator;
use crate::net::connect::service::heartbeat::service_request::HeartbeatServiceRequest;
use crate::net::connect::service_client::ServiceResponseError;

pub struct BuiltinHeartbeatSender {
    target_address: HostAndPort,
}

#[async_trait]
impl HeartbeatSender for BuiltinHeartbeatSender {
    async fn send(&self) -> Result<(), ServiceResponseError> {
        let node_id = "mark";
        let service_server_request = HeartbeatServiceRequest::new(
            node_id.to_string(),
            &RandomCorrelationIdGenerator::new()
        );
        let target_address = self.target_address.clone();
        let result = AsyncNetwork::send_without_source_footprint(service_server_request, target_address.clone()).await;
        if result.is_err() {
            eprintln!("Could not send heartbeat to {}", target_address.as_string());
        }
        return result;
    }
}

impl BuiltinHeartbeatSender {
    pub fn new(target_address: HostAndPort) -> BuiltinHeartbeatSender {
        return BuiltinHeartbeatSender { target_address };
    }
}