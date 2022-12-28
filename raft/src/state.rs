use std::error::Error;
use async_trait::async_trait;

use std::fmt::{Display, Formatter};
use std::sync::{Arc, RwLock};
use replicate::heartbeat::heartbeat_sender::HeartbeatSender;
use replicate::net::connect::service_client::ServiceResponseError;

use replicate::net::replica::{Replica, ReplicaId};
use crate::net::factory::service_request::ServiceRequestFactory;

pub struct State {
    consensus_state: RwLock<ConsensusState>,
    replica: Arc<Replica>,
}

struct ConsensusState {
    term: u64,
    role: ReplicaRole,
    voted_for: Option<u64>,
}

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum ReplicaRole {
    Leader,
    Follower,
    Candidate,
}

impl State {
    pub fn new(replica: Arc<Replica>) -> State {
        return State {
            replica,
            consensus_state: RwLock::new(ConsensusState {
                term: 0,
                role: ReplicaRole::Follower,
                voted_for: None,
            }),
        };
    }

    pub(crate) fn change_to_candidate(&self) -> u64 {
        let mut write_guard = self.consensus_state.write().unwrap();
        let mut consensus_state = &mut *write_guard;
        consensus_state.term = consensus_state.term + 1;
        consensus_state.role = ReplicaRole::Candidate;
        consensus_state.voted_for = Some(self.replica.get_id());

        return consensus_state.term;
    }

    pub(crate) fn change_to_follower(&self, term: u64) {
        let mut write_guard = self.consensus_state.write().unwrap();
        let mut consensus_state = &mut *write_guard;
        consensus_state.role = ReplicaRole::Follower;
        consensus_state.term = term;
        consensus_state.voted_for = None;
    }

    pub(crate) fn change_to_leader(&self) {
        let mut write_guard = self.consensus_state.write().unwrap();
        let mut consensus_state = &mut *write_guard;
        consensus_state.role = ReplicaRole::Leader;
    }

    pub fn get_term(&self) -> u64 {
        let guard = self.consensus_state.read().unwrap();
        return (*guard).term;
    }

    pub fn get_role(&self) -> ReplicaRole {
        let guard = self.consensus_state.read().unwrap();
        return (*guard).role;
    }

    fn get_voted_for(&self) -> Option<ReplicaId> {
        let guard = self.consensus_state.read().unwrap();
        return (*guard).voted_for;
    }

    pub(crate) fn get_replica(&self) -> Arc<Replica> {
        return self.replica.clone();
    }
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
impl HeartbeatSender for State {
    async fn send(&self) -> Result<(), ServiceResponseError> {
        let term = self.get_term();
        let leader_id = self.replica.get_id();
        let service_request_constructor = || {
            ServiceRequestFactory::heartbeat(term, leader_id)
        };
        let total_failed_sends =
            self.replica.send_one_way_to_replicas_without_callback(service_request_constructor).await;

        println!("total failures {}", total_failed_sends);
        return match total_failed_sends {
            0 => Ok(()),
            _ => Err(Box::new(HeartbeatSendError { total_failed_sends }))
        };
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;
    use replicate::clock::clock::SystemClock;
    use replicate::net::connect::host_and_port::HostAndPort;
    use replicate::net::replica::Replica;
    use crate::state::{ReplicaRole, State};

    #[test]
    fn become_candidate() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let state = State::new(Arc::new(some_replica));
        state.change_to_candidate();

        assert_eq!(1, state.get_term());
        assert_eq!(ReplicaRole::Candidate, state.get_role());
        assert_eq!(Some(10), state.get_voted_for());
    }

    #[test]
    fn become_leader() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let state = State::new(Arc::new(some_replica));
        state.change_to_candidate();
        state.change_to_leader();

        assert_eq!(1, state.get_term());
        assert_eq!(ReplicaRole::Leader, state.get_role());
        assert_eq!(Some(10), state.get_voted_for());
    }

    #[test]
    fn become_follower() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let state = State::new(Arc::new(some_replica));
        state.change_to_candidate();
        state.change_to_follower(2);

        assert_eq!(2, state.get_term());
        assert_eq!(ReplicaRole::Follower, state.get_role());
        assert_eq!(None, state.get_voted_for());
    }
}