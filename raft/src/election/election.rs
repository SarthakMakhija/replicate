use std::sync::Arc;

use replicate::consensus::quorum::async_quorum_callback::AsyncQuorumCallback;
use replicate::net::connect::correlation_id::RESERVED_CORRELATION_ID;
use replicate::net::replica::Replica;
use replicate::net::request_waiting_list::response_callback::ResponseCallback;

use crate::net::factory::service_request::ServiceRequestFactory;
use crate::net::rpc::grpc::RequestVoteResponse;
use crate::state::State;

pub struct Election {
    state: Arc<State>,
    replica: Arc<Replica>,
}

impl Election {
    pub fn new(state: Arc<State>, replica: Arc<Replica>) -> Self {
        return Election {
            state,
            replica,
        };
    }

    pub fn start(&self) {
        let replica = self.replica.clone();
        let inner_replica = replica.clone();
        let state = self.state.clone();

        replica.add_to_queue(async move {
            let term = state.change_to_candidate(inner_replica.get_id());
            let service_request_constructor = || {
                ServiceRequestFactory::request_vote(
                    inner_replica.get_id(),
                    term,
                )
            };
            let success_condition = Box::new(|response: &RequestVoteResponse| response.voted);
            let expected_responses = inner_replica.total_peer_count();
            let async_quorum_callback = AsyncQuorumCallback::<RequestVoteResponse>::new_with_success_condition(
                expected_responses,
                success_condition,
            );
            let _ = inner_replica.send_one_way_to_replicas(
                service_request_constructor,
                async_quorum_callback.clone(),
            ).await;

            async_quorum_callback.on_response(inner_replica.get_self_address(), Ok(Box::new(RequestVoteResponse{
                term,
                voted: true,
                correlation_id: RESERVED_CORRELATION_ID
            })));

            let quorum_completion_response = async_quorum_callback.handle().await;
            if quorum_completion_response.is_success() { state.change_to_leader() }
        });
    }
}