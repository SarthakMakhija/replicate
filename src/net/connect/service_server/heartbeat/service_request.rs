use crate::net::connect::service_server::heartbeat::service::grpc::HeartbeatRequest;
use crate::net::connect::service_server::heartbeat::client::HeartbeatServiceClient;
use crate::net::connect::service_server_callable::ServiceServerRequest;

pub(crate) struct HeartbeatServiceServerRequest {}

impl HeartbeatServiceServerRequest {
    pub(crate) fn new(node_id: String) -> ServiceServerRequest<HeartbeatRequest, ()> {
        return ServiceServerRequest {
            payload: HeartbeatRequest { node_id },
            callable: Box::new(HeartbeatServiceClient {}),
        };
    }
}