use replicate::net::connect::correlation_id::CorrelationId;
use replicate::net::replica::ReplicaId;

use crate::net::rpc::grpc::RequestVote;
use crate::net::rpc::grpc::RequestVoteResponse;

pub(crate) struct RequestVoteBuilder {}

pub(crate) struct RequestVoteResponseBuilder {}

impl RequestVoteBuilder {
    pub(crate) fn request_vote(replica_id: ReplicaId,
                               term: u64,
                               correlation_id: CorrelationId,
    ) -> RequestVote {
        return RequestVoteBuilder::request_vote_with_log(
            replica_id,
            term,
            correlation_id,
            None,
            None,
        );
    }

    pub(crate) fn request_vote_with_log(replica_id: ReplicaId,
                                        term: u64,
                                        correlation_id: CorrelationId,
                                        last_log_index: Option<u64>,
                                        last_log_term: Option<u64>,
    ) -> RequestVote {
        return RequestVote {
            replica_id,
            term,
            correlation_id,
            last_log_index,
            last_log_term,
        };
    }
}

impl RequestVoteResponseBuilder {
    pub(crate) fn voted_response(term: u64, correlation_id: CorrelationId) -> RequestVoteResponse {
        return RequestVoteResponse {
            term,
            correlation_id,
            voted: true,
        };
    }

    pub(crate) fn not_voted_response(term: u64, correlation_id: CorrelationId) -> RequestVoteResponse {
        return RequestVoteResponse {
            term,
            correlation_id,
            voted: false,
        };
    }
}