use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use replicate::clock::clock::Clock;
use replicate::net::connect::error::AnyError;
use replicate::net::replica::{Replica, ReplicaId};

use crate::net::factory::service_request::ServiceRequestFactory;

pub struct State {
    consensus_state: RwLock<ConsensusState>,
    replica: Arc<Replica>,
    clock: Arc<dyn Clock>,
}

struct ConsensusState {
    term: u64,
    role: ReplicaRole,
    voted_for: Option<u64>,
    heartbeat_received_time: Option<SystemTime>,
}

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum ReplicaRole {
    Leader,
    Follower,
    Candidate,
}

impl State {
    pub fn new(replica: Arc<Replica>, clock: Arc<dyn Clock>) -> Arc<State> {
        let state = State {
            replica,
            clock,
            consensus_state: RwLock::new(ConsensusState {
                term: 0,
                role: ReplicaRole::Follower,
                voted_for: None,
                heartbeat_received_time: None,
            }),
        };
        return Arc::new(state);
    }

    pub(crate) fn mark_heartbeat_received(&self) {
        let mut write_guard = self.consensus_state.write().unwrap();
        let mut consensus_state = &mut *write_guard;
        consensus_state.heartbeat_received_time = Some(self.clock.now());
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

    pub fn get_heartbeat_received_time(&self) -> Option<SystemTime> {
        let guard = self.consensus_state.read().unwrap();
        return (*guard).heartbeat_received_time;
    }

    pub fn get_heartbeat_sender(&self) -> impl Future<Output=Result<(), AnyError>> {
        let term = self.get_term();
        let leader_id = self.replica.get_id();
        let replica = self.replica.clone();

        return async move {
            let service_request_constructor = || {
                ServiceRequestFactory::heartbeat(term, leader_id)
            };
            let total_failed_sends =
                replica.send_to_replicas_without_callback(service_request_constructor).await;

            println!("total failures {}", total_failed_sends);
            return match total_failed_sends {
                0 => Ok(()),
                _ => {
                    let any_error: AnyError = Box::new(HeartbeatSendError { total_failed_sends });
                    Err(any_error)
                }
            };
        };
    }

    pub(crate) fn maybe_start_election<F>(self: Arc<Self>, election_timeout: &Duration, election_starter: F)
        where F: FnOnce(Arc<State>) -> () {
        let write_guard = self.consensus_state.write().unwrap();
        let consensus_state = &*write_guard;
        if let Some(last_heartbeat_time) = consensus_state.heartbeat_received_time {
            if self.clock.now().duration_since(last_heartbeat_time).unwrap().ge(election_timeout) {
                election_starter(self.clone());
            }
        }
    }

    pub(crate) fn get_replica(&self) -> Arc<Replica> {
        return self.replica.clone();
    }

    pub(crate) fn get_replica_reference(&self) -> &Arc<Replica> {
        return &self.replica;
    }

    fn get_voted_for(&self) -> Option<ReplicaId> {
        let guard = self.consensus_state.read().unwrap();
        return (*guard).voted_for;
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

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;
    use std::time::Duration;

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

        let state = State::new(Arc::new(some_replica), Arc::new(SystemClock::new()));
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

        let state = State::new(Arc::new(some_replica), Arc::new(SystemClock::new()));
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

        let state = State::new(Arc::new(some_replica), Arc::new(SystemClock::new()));
        state.change_to_candidate();
        state.change_to_follower(2);

        assert_eq!(2, state.get_term());
        assert_eq!(ReplicaRole::Follower, state.get_role());
        assert_eq!(None, state.get_voted_for());
    }

    #[test]
    fn maybe_start_election() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let state = State::new(Arc::new(some_replica), Arc::new(SystemClock::new()));
        state.mark_heartbeat_received();

        let election_timeout = Duration::from_millis(0);
        let mut count = 0;
        let election_starter = |_state| { count = count + 1; };

        state.maybe_start_election(&election_timeout, election_starter);
        assert_eq!(1, count);
    }

    #[test]
    fn maybe_not_start_election() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let state = State::new(Arc::new(some_replica), Arc::new(SystemClock::new()));
        state.mark_heartbeat_received();

        let election_timeout = Duration::from_secs(100);
        let mut count = 0;
        let election_starter = |_state| { count = count + 1; };

        state.maybe_start_election(&election_timeout, election_starter);

        assert_eq!(0, count);
    }
}