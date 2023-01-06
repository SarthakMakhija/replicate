use replicate::net::connect::correlation_id::{CorrelationId, CorrelationIdGenerator};
use replicate::net::connect::random_correlation_id_generator::RandomCorrelationIdGenerator;
use replicate::net::connect::service_client::ServiceRequest;
use replicate::net::replica::ReplicaId;

use crate::net::factory::client_provider::{HeartbeatServiceClient, ReplicateLogClient, ReplicateLogResponseClient, RequestVoteClient, RequestVoteResponseClient};
use crate::net::rpc::grpc::AppendEntries;
use crate::net::rpc::grpc::AppendEntriesResponse;
use crate::net::rpc::grpc::Entry;
use crate::net::rpc::grpc::RequestVote;
use crate::net::rpc::grpc::RequestVoteResponse;

pub(crate) trait ServiceRequestFactory: Send + Sync {
    fn request_vote(&self, replica_id: ReplicaId, term: u64) -> ServiceRequest<RequestVote, ()> {
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

    fn request_vote_response(&self, term: u64, voted: bool, correlation_id: CorrelationId) -> ServiceRequest<RequestVoteResponse, ()> {
        return ServiceRequest::new(
            RequestVoteResponse {
                term,
                voted,
                correlation_id,
            },
            Box::new(RequestVoteResponseClient {}),
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
            },
            Box::new(HeartbeatServiceClient {}),
            correlation_id,
        );
    }

    fn replicate_log(&self,
                     term: u64,
                     leader_id: ReplicaId,
                     previous_log_index: Option<u64>,
                     previous_log_term: Option<u64>,
                     entry: Option<Entry>,
    ) -> ServiceRequest<AppendEntries, ()> {
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
            },
            Box::new(ReplicateLogClient {}),
            correlation_id,
        );
    }

    fn replicate_log_response(&self, term: u64, success: bool, correlation_id: CorrelationId) -> ServiceRequest<AppendEntriesResponse, ()> {
        return ServiceRequest::new(
            AppendEntriesResponse {
                term,
                success,
                correlation_id
            },
            Box::new(ReplicateLogResponseClient {}),
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