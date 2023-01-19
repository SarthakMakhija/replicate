use replicate::net::connect::correlation_id::CorrelationId;
use replicate::net::replica::ReplicaId;

use crate::net::rpc::grpc::RequestVote;

pub(crate) struct RequestVoteBuilder {}

impl RequestVoteBuilder {
    pub(crate) fn new() -> Self {
        return RequestVoteBuilder {};
    }

    pub(crate) fn request_vote(&self,
                               replica_id: ReplicaId,
                               term: u64,
                               correlation_id: CorrelationId,
    ) -> RequestVote {
        return self.request_vote_with_log(
            replica_id,
            term,
            correlation_id,
            None,
            None,
        );
    }

    pub(crate) fn request_vote_with_log(&self,
                                        replica_id: ReplicaId,
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