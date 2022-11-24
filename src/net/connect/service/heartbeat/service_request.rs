use crate::net::connect::service::heartbeat::service::grpc::HeartbeatRequest;
use crate::net::connect::service::heartbeat::client::HeartbeatServiceClient;
use crate::net::connect::service_server_callable::ServiceServerRequest;

pub(crate) struct HeartbeatServiceRequest {}

impl HeartbeatServiceRequest {
    pub(crate) fn new(node_id: String) -> ServiceServerRequest<HeartbeatRequest, ()> {
        return ServiceServerRequest {
            payload: HeartbeatRequest { node_id },
            callable: Box::new(HeartbeatServiceClient {}),
        };
    }
}