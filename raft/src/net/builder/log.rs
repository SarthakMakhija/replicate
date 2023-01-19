use replicate::net::connect::correlation_id::CorrelationId;
use replicate::net::replica::ReplicaId;

use crate::net::rpc::grpc::AppendEntries;
use crate::net::rpc::grpc::AppendEntriesResponse;
use crate::net::rpc::grpc::Entry;

pub(crate) struct ReplicateLogRequestBuilder {}
pub(crate) struct ReplicateLogResponseBuilder {}

impl ReplicateLogRequestBuilder {
    pub(crate) fn replicate_log_request_with_no_log_reference(
        term: u64,
        leader_id: ReplicaId,
        correlation_id: CorrelationId) -> AppendEntries {
        return ReplicateLogRequestBuilder::replicate_log_request(
            term, leader_id, correlation_id, None, None, None, None,
        );
    }

    pub(crate) fn replicate_log_request_with_no_previous_log_reference(
        term: u64,
        leader_id: ReplicaId,
        correlation_id: CorrelationId,
        entry: Option<Entry>) -> AppendEntries {
        return ReplicateLogRequestBuilder::replicate_log_request(
            term, leader_id, correlation_id, None, None, None, entry,
        );
    }

    pub(crate) fn replicate_log_request(
        term: u64,
        leader_id: ReplicaId,
        correlation_id: CorrelationId,
        previous_log_index: Option<u64>,
        previous_log_term: Option<u64>,
        leader_commit_index: Option<u64>,
        entry: Option<Entry>,
    ) -> AppendEntries {
        return AppendEntries {
            term,
            leader_id,
            correlation_id,
            entry,
            previous_log_index,
            previous_log_term,
            leader_commit_index,
        };
    }
}

impl ReplicateLogResponseBuilder {
    pub(crate) fn success_response(term: u64, correlation_id: CorrelationId, log_entry_index: u64) -> AppendEntriesResponse {
        AppendEntriesResponse {
            term,
            success: true,
            log_entry_index: Some(log_entry_index),
            correlation_id,
        }
    }

    pub(crate) fn failure_response(term: u64, correlation_id: CorrelationId) -> AppendEntriesResponse {
        AppendEntriesResponse {
            term,
            success: false,
            log_entry_index: None,
            correlation_id,
        }
    }
}