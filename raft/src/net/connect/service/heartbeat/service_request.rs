use crate::net::connect::correlation_id::{CorrelationIdGenerator, DefaultCorrelationIdType};
use crate::net::connect::service::heartbeat::client::HeartbeatServiceClient;
use crate::net::connect::service::heartbeat::service::grpc::HeartbeatRequest;
use crate::net::connect::service_client::ServiceRequest;

pub struct HeartbeatServiceRequest {}

impl HeartbeatServiceRequest {
    pub fn new(
        node_id: String,
        mut correlation_id_generator: CorrelationIdGenerator,
    ) -> ServiceRequest<HeartbeatRequest, (), DefaultCorrelationIdType> {
        let correlation_id = correlation_id_generator.generate::<DefaultCorrelationIdType>();
        return ServiceRequest::new(
            HeartbeatRequest { node_id, correlation_id },
            Box::new(HeartbeatServiceClient {}),
            correlation_id,
        );
    }
}