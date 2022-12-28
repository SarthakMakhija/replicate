use std::sync::Arc;

use tonic::{Request, Response};

use replicate::net::connect::async_network::AsyncNetwork;
use replicate::net::connect::host_port_extractor::HostAndPortExtractor;

use crate::net::rpc::grpc::{RequestVote, RequestVoteResponse, AppendEntries};
use crate::net::rpc::grpc::raft_server::Raft;
use crate::net::factory::service_request::ServiceRequestFactory;
use crate::state::State;

pub struct RaftService {
    state: Arc<State>,
}

impl RaftService {
    pub fn new(state: Arc<State>) -> Self {
        return RaftService { state };
    }
}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn acknowledge_request_vote(&self, request: Request<RequestVote>) -> Result<Response<()>, tonic::Status> {
        let originating_host_port = request.try_referral_host_port().unwrap();

        let state = self.state.clone();
        let request = request.into_inner();
        let correlation_id = request.correlation_id;
        let replica = self.state.get_replica();
        let source_address = replica.get_self_address();

        println!("received RequestVote with term {}", request.term);
        let handler = async move {
            let term = state.get_term();
            //TODO: Validate from raft paper
            let voted: bool = if request.term > term {
                true
            } else {
                false
            };
            AsyncNetwork::send_with_source_footprint(
                ServiceRequestFactory::request_vote_response(term, voted, correlation_id),
                source_address,
                originating_host_port,
            ).await.unwrap();
        };
        let _ = replica.submit_to_queue(handler);
        return Ok(Response::new(()));
    }

    async fn finish_request_vote(&self, request: Request<RequestVoteResponse>) -> Result<Response<()>, tonic::Status> {
        let originating_host_port = request.try_referral_host_port().unwrap();
        let response = request.into_inner();
        println!("received RequestVoteResponse with voted? {}", response.voted);

        let _ = &self.state.get_replica_reference().register_response(response.correlation_id, originating_host_port, Ok(Box::new(response)));
        return Ok(Response::new(()));
    }

    async fn acknowledge_heartbeat(&self, _: Request<AppendEntries>) -> Result<Response<()>, tonic::Status> {
        println!("received heartbeat on {:?}", self.state.get_replica_reference().get_self_address());
        return Ok(Response::new(()));
    }
}