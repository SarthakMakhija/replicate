use async_trait::async_trait;

use std::sync::Arc;

use replicate::heartbeat::heartbeat_sender::HeartbeatSender;
use replicate::net::connect::async_network::AsyncNetwork;
use replicate::net::connect::service_client::ServiceResponseError;
use replicate::net::replica::Replica;

use crate::net::factory::service_request::ServiceRequestFactory;
use crate::state::State;

pub struct RaftHeartbeatSender {
    state: Arc<State>,
    replica: Arc<Replica>,
}

#[async_trait]
impl HeartbeatSender for RaftHeartbeatSender {
    async fn send(&self) -> Result<(), ServiceResponseError> {
        let term = self.state.get_term();
        let leader_id = self.replica.get_id();
        let mut handles = Vec::new();

        let peers = self.replica.get_peers().clone();
        for peer_address in peers {
            let service_server_request = ServiceRequestFactory::heartbeat(
                term,
                leader_id,
            );
            handles.push(tokio::spawn(async move{
                return AsyncNetwork::send_without_source_footprint(
                    service_server_request,
                    peer_address,
                ).await;
            }));
        }

        let mut done = Ok(());
        for handle in handles {
            let result = handle.await.unwrap();
            if result.is_err() {
                eprintln!("Could not send heartbeat {}", result.as_ref().unwrap_err());
                done = result;
            }
        }
        return done;
    }
}

impl RaftHeartbeatSender {
    pub fn new(state: Arc<State>, replica: Arc<Replica>) -> RaftHeartbeatSender {
        return RaftHeartbeatSender { state, replica };
    }
}
