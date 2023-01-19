use replicate::net::connect::correlation_id::CorrelationId;
use replicate::net::replica::ReplicaId;

use crate::net::rpc::grpc::AppendEntries;
use crate::net::rpc::grpc::AppendEntriesResponse;

pub(crate) struct HeartbeatRequestBuilder {}

pub(crate) struct HeartbeatResponseBuilder {}

impl HeartbeatRequestBuilder {
    pub(crate) fn heartbeat_request(term: u64, leader_id: ReplicaId, correlation_id: CorrelationId) -> AppendEntries {
        return AppendEntries {
            term,
            leader_id,
            correlation_id,
            entry: None,
            previous_log_index: None,
            previous_log_term: None,
            leader_commit_index: None,
        };
    }
}

impl HeartbeatResponseBuilder {
    pub(crate) fn success_response(term: u64, correlation_id: CorrelationId) -> AppendEntriesResponse {
        return AppendEntriesResponse {
            success: true,
            term,
            correlation_id,
            log_entry_index: None,
        };
    }

    pub(crate) fn failure_response(term: u64, correlation_id: CorrelationId) -> AppendEntriesResponse {
        return AppendEntriesResponse {
            success: false,
            term,
            correlation_id,
            log_entry_index: None,
        };
    }
}