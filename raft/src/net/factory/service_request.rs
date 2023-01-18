use replicate::net::connect::correlation_id::CorrelationIdGenerator;
use replicate::net::connect::random_correlation_id_generator::RandomCorrelationIdGenerator;
use replicate::net::connect::service_client::ServiceRequest;
use replicate::net::replica::ReplicaId;

use crate::net::factory::client_provider::{HeartbeatClient, ReplicateLogClient, RequestVoteClient};
use crate::net::rpc::grpc::AppendEntries;
use crate::net::rpc::grpc::AppendEntriesResponse;
use crate::net::rpc::grpc::Entry;
use crate::net::rpc::grpc::RequestVote;
use crate::net::rpc::grpc::RequestVoteResponse;

pub(crate) trait ServiceRequestFactory: Send + Sync {
    fn request_vote(&self,
                    replica_id: ReplicaId,
                    term: u64,
                    last_log_index: Option<u64>,
                    last_log_term: Option<u64>,
    ) -> ServiceRequest<RequestVote, RequestVoteResponse> {
        let correlation_id_generator = RandomCorrelationIdGenerator::new();
        let correlation_id = correlation_id_generator.generate();
        return ServiceRequest::new(
            RequestVote {
                replica_id,
                term,
                correlation_id,
            },
            Box::new(RequestVoteClient {}),
            correlation_id,
        );
    }

    fn heartbeat(&self, term: u64, leader_id: ReplicaId) -> ServiceRequest<AppendEntries, AppendEntriesResponse> {
        let correlation_id_generator = RandomCorrelationIdGenerator::new();
        let correlation_id = correlation_id_generator.generate();

        return ServiceRequest::new(
            AppendEntries {
                term,
                leader_id,
                correlation_id,
                entry: None,
                previous_log_index: None,
                previous_log_term: None,
                leader_commit_index: None,
            },
            Box::new(HeartbeatClient {}),
            correlation_id,
        );
    }

    fn replicate_log(&self,
                     term: u64,
                     leader_id: ReplicaId,
                     previous_log_index: Option<u64>,
                     previous_log_term: Option<u64>,
                     leader_commit_index: Option<u64>,
                     entry: Option<Entry>,
    ) -> ServiceRequest<AppendEntries, AppendEntriesResponse> {
        let correlation_id_generator = RandomCorrelationIdGenerator::new();
        let correlation_id = correlation_id_generator.generate();

        return ServiceRequest::new(
            AppendEntries {
                term,
                leader_id,
                correlation_id,
                entry,
                previous_log_index,
                previous_log_term,
                leader_commit_index,
            },
            Box::new(ReplicateLogClient {}),
            correlation_id,
        );
    }
}

pub(crate) struct BuiltInServiceRequestFactory {}

impl ServiceRequestFactory for BuiltInServiceRequestFactory {}

impl BuiltInServiceRequestFactory {
    pub(crate) fn new() -> Self {
        return BuiltInServiceRequestFactory {};
    }
}