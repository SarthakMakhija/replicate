use crate::net::connect::service::heartbeat::client::HeartbeatServiceClient;
use crate::net::connect::service::heartbeat::service::grpc::HeartbeatRequest;
use crate::net::connect::service_client::ServiceRequest;

pub struct HeartbeatServiceRequest {}

impl HeartbeatServiceRequest {
    pub fn new(node_id: String) -> ServiceRequest<HeartbeatRequest, ()> {
        return ServiceRequest::new(
            HeartbeatRequest { node_id },
            Box::new(HeartbeatServiceClient {}),
            100,
        );
    }
}