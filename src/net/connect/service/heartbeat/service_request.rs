use crate::net::connect::service::heartbeat::service::grpc::HeartbeatRequest;
use crate::net::connect::service::heartbeat::client::HeartbeatServiceClient;
use crate::net::connect::service_server_callable::ServiceRequest;

pub(crate) struct HeartbeatServiceRequest {}

impl HeartbeatServiceRequest {
    pub(crate) fn new(node_id: String) -> ServiceRequest<HeartbeatRequest, ()> {
        return ServiceRequest {
            payload: HeartbeatRequest { node_id },
            service_client: Box::new(HeartbeatServiceClient {}),
        };
    }
}