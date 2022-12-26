use replicate::net::connect::correlation_id::{CorrelationId, CorrelationIdGenerator};
use replicate::net::connect::random_correlation_id_generator::RandomCorrelationIdGenerator;
use replicate::net::connect::service_client::ServiceRequest;

use crate::quorum::factory::client_provider::CorrelatingGetValueByKeyRequestClient;
use crate::quorum::factory::client_provider::GetValueByKeyResponseClient;
use crate::quorum::factory::client_provider::PutKeyValueResponseClient;
use crate::quorum::factory::client_provider::VersionedPutKeyValueRequestClient;

use crate::quorum::rpc::grpc::CorrelatingGetValueByKeyRequest;
use crate::quorum::rpc::grpc::GetValueByKeyResponse;
use crate::quorum::rpc::grpc::VersionedPutKeyValueRequest;
use crate::quorum::rpc::grpc::PutKeyValueResponse;

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

    pub(crate) fn get_value_by_key_response(correlation_id: CorrelationId, response: GetValueByKeyResponse) -> ServiceRequest<GetValueByKeyResponse, ()> {
        return ServiceRequest::new(
            response,
            Box::new(GetValueByKeyResponseClient {}),
            correlation_id,
        );
    }

    pub(crate) fn put_key_value_response(correlation_id: CorrelationId) -> ServiceRequest<PutKeyValueResponse, ()> {
        return ServiceRequest::new(
            PutKeyValueResponse { was_put: true, correlation_id },
            Box::new(PutKeyValueResponseClient {}),
            correlation_id,
        );
    }
}