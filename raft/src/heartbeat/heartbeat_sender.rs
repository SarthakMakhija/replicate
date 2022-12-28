use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use async_trait::async_trait;

use replicate::heartbeat::heartbeat_sender::HeartbeatSender;
use replicate::net::connect::service_client::ServiceResponseError;
use replicate::net::replica::Replica;

use crate::net::factory::service_request::ServiceRequestFactory;
use crate::state::State;

pub struct RaftHeartbeatSender {
    state: Arc<State>,
    replica: Arc<Replica>,
}

#[derive(Debug)]
pub struct HeartbeatSendError {
    pub total_failed_sends: usize,
}

impl Display for HeartbeatSendError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let message = format!("Total failures in sending heartbeat {}", self.total_failed_sends);
        write!(formatter, "{}", message)
    }
}

impl Error for HeartbeatSendError {}

#[async_trait]
impl HeartbeatSender for RaftHeartbeatSender {
    async fn send(&self) -> Result<(), ServiceResponseError> {
        let term = self.state.get_term();
        let leader_id = self.replica.get_id();
        let service_request_constructor = || {
            ServiceRequestFactory::heartbeat(term, leader_id)
        };
        let total_failed_sends =
            self.replica.send_one_way_to_replicas_without_callback(service_request_constructor).await;

        return match total_failed_sends {
            0 => Ok(()),
            _ => Err(Box::new(HeartbeatSendError { total_failed_sends }))
        };
    }
}

impl RaftHeartbeatSender {
    pub fn new(state: Arc<State>, replica: Arc<Replica>) -> RaftHeartbeatSender {
        return RaftHeartbeatSender { state, replica };
    }
}
