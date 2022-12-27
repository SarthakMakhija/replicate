use replicate::net::connect::correlation_id::{CorrelationId, CorrelationIdGenerator};
use replicate::net::connect::random_correlation_id_generator::RandomCorrelationIdGenerator;
use replicate::net::connect::service_client::ServiceRequest;
use replicate::net::replica::ReplicaId;

use crate::net::rpc::grpc::RequestVote;
use crate::net::rpc::grpc::RequestVoteResponse;
use crate::net::factory::client_provider::{RequestVoteClient, RequestVoteResponseClient};

pub(crate) struct ServiceRequestFactory {}

impl ServiceRequestFactory {
    pub(crate) fn request_vote(replica_id: ReplicaId, term: u64) -> ServiceRequest<RequestVote, ()> {
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

    pub(crate) fn request_vote_response(term: u64, voted: bool, correlation_id: CorrelationId) -> ServiceRequest<RequestVoteResponse, ()> {
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
}