use raft::net::connect::correlation_id::CorrelationIdGenerator;
use raft::net::connect::random_correlation_id_generator::RandomCorrelationIdGenerator;
use raft::net::connect::service_client::ServiceRequest;

use crate::quorum::client_provider::{CorrelatingGetValueByKeyRequestClient, VersionedPutKeyValueRequestClient};
use crate::quorum::rpc::grpc::CorrelatingGetValueByKeyRequest;
use crate::quorum::rpc::grpc::VersionedPutKeyValueRequest;

pub(crate) struct ServiceRequestFactory {}

impl ServiceRequestFactory {
    pub(crate) fn correlating_get_value_by_key_request(key: String) -> ServiceRequest<CorrelatingGetValueByKeyRequest, ()> {
        let correlation_id_generator = RandomCorrelationIdGenerator::new();
        let correlation_id = correlation_id_generator.generate();
        return ServiceRequest::new(
            CorrelatingGetValueByKeyRequest {
                key,
                correlation_id,
            },
            Box::new(CorrelatingGetValueByKeyRequestClient {}),
            correlation_id,
        );
    }

    pub(crate) fn versioned_put_key_value_request(timestamp: u64, key: String, value: String) -> ServiceRequest<VersionedPutKeyValueRequest, ()> {
        let correlation_id_generator = RandomCorrelationIdGenerator::new();
        let correlation_id = correlation_id_generator.generate();
        return ServiceRequest::new(
            VersionedPutKeyValueRequest {
                key,
                value,
                timestamp,
                correlation_id,
            },
            Box::new(VersionedPutKeyValueRequestClient {}),
            correlation_id,
        );
    }
}