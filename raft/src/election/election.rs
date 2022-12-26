use std::sync::Arc;

use replicate::consensus::quorum::async_quorum_callback::AsyncQuorumCallback;
use replicate::net::replica::Replica;

use crate::net::rpc::grpc::RequestVoteResponse;
use crate::net::factory::service_request::ServiceRequestFactory;
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
            let term = state.increment_term();
            let service_request_constructor = || {
                ServiceRequestFactory::request_vote(
                    inner_replica.get_name().to_string(),
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
            let _ = async_quorum_callback.handle().await;
        });
    }
}