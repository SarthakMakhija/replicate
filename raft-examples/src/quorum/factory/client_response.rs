use raft::net::connect::correlation_id::CorrelationId;

use crate::quorum::rpc::grpc::GetValueByKeyResponse;
use crate::quorum::rpc::grpc::PutKeyValueResponse;
use crate::quorum::store::value::Value;

pub(crate) struct ClientResponse {}

impl ClientResponse {
    pub(crate) fn empty_get_value_by_key_response() -> GetValueByKeyResponse {
        return GetValueByKeyResponse {
            key: "".to_string(),
            value: "".to_string(),
            correlation_id: 0,
            timestamp: 0,
        };
    }

    pub(crate) fn get_value_by_key_response(response: &GetValueByKeyResponse) -> GetValueByKeyResponse {
        return GetValueByKeyResponse {
            key: response.key.clone(),
            value: response.value.clone(),
            correlation_id: response.correlation_id,
            timestamp: response.timestamp,
        };
    }

    pub(crate) fn get_value_by_key_response_using_value_ref(key: String, value: &Value, correlation_id: CorrelationId) -> GetValueByKeyResponse {
        return GetValueByKeyResponse {
            key,
            value: value.get_value(),
            correlation_id,
            timestamp: value.get_timestamp(),
        };
    }

    pub(crate) fn get_value_by_key_response_using_empty_value(key: String, correlation_id: CorrelationId) -> GetValueByKeyResponse {
        return GetValueByKeyResponse {
            key,
            value: "".to_string(),
            correlation_id,
            timestamp: 0,
        };
    }

    pub(crate) fn put_key_value_response(done: bool, correlation_id: CorrelationId) -> PutKeyValueResponse {
        return PutKeyValueResponse {
            was_put: done,
            correlation_id,
        };
    }
}