use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use replicate::clock::clock::Clock;
use replicate::heartbeat::heartbeat_scheduler::SingleThreadedHeartbeatScheduler;
use replicate::net::connect::error::AnyError;
use replicate::net::replica::{Replica, ReplicaId};

use crate::election::election::Election;
use crate::net::factory::service_request::ServiceRequestFactory;

pub struct State {
    consensus_state: RwLock<ConsensusState>,
    replica: Arc<Replica>,
    clock: Arc<dyn Clock>,
    heartbeat_send_scheduler: SingleThreadedHeartbeatScheduler,
    heartbeat_check_scheduler: SingleThreadedHeartbeatScheduler,
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
            consensus_state: RwLock::new(ConsensusState {
                term: 0,
                role: ReplicaRole::Follower,
                voted_for: None,
                heartbeat_received_time: None,
            }),
            replica,
            clock,
            heartbeat_send_scheduler: SingleThreadedHeartbeatScheduler::new(Duration::from_millis(10)),
            heartbeat_check_scheduler: SingleThreadedHeartbeatScheduler::new(Duration::from_millis(5)),
        };

        let state = Arc::new(state);
        state.clone().change_to_follower(0);
        return state;
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

        self.heartbeat_send_scheduler.stop();
        self.heartbeat_check_scheduler.stop();

        return consensus_state.term;
    }

    pub(crate) fn change_to_follower(self: Arc<State>, term: u64) {
        let mut write_guard = self.consensus_state.write().unwrap();
        let mut consensus_state = &mut *write_guard;
        consensus_state.role = ReplicaRole::Follower;
        consensus_state.term = term;
        consensus_state.voted_for = None;

        self.heartbeat_send_scheduler.stop();
        Self::restart_heartbeat_checker(self.clone(), &self.heartbeat_check_scheduler);
    }

    pub(crate) fn change_to_leader(self: Arc<State>) {
        let mut write_guard = self.consensus_state.write().unwrap();
        let mut consensus_state = &mut *write_guard;
        consensus_state.role = ReplicaRole::Leader;

        self.heartbeat_check_scheduler.stop();
        Self::restart_heartbeat_sender(self.clone(), &self.heartbeat_send_scheduler);
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

    pub(crate) fn get_heartbeat_checker<F>(self: Arc<State>, election_timeout: Duration, election_starter: F) -> impl Future<Output=Result<(), AnyError>>
        where F: FnOnce(Arc<State>) -> () {
        let inner_self = self.clone();
        let clock = self.clock.clone();

        return async move {
            let write_guard = inner_self.consensus_state.write().unwrap();
            let consensus_state = &*write_guard;
            if let Some(last_heartbeat_time) = consensus_state.heartbeat_received_time {
                if clock.now().duration_since(last_heartbeat_time).unwrap().ge(&election_timeout) {
                    election_starter(inner_self.clone());
                }
            }
            return Ok(());
        };
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

    fn restart_heartbeat_checker(state: Arc<State>, heartbeat_check_scheduler: &SingleThreadedHeartbeatScheduler) {
        heartbeat_check_scheduler.stop();
        heartbeat_check_scheduler.start_with(move ||
            state.clone().get_heartbeat_checker(Duration::from_millis(10), |state| Election::new(state).start())
        );
    }

    fn restart_heartbeat_sender(state: Arc<State>, heartbeat_send_scheduler: &SingleThreadedHeartbeatScheduler) {
        heartbeat_send_scheduler.stop();
        heartbeat_send_scheduler.start_with(move ||
            state.get_heartbeat_sender()
        );
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
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    use replicate::clock::clock::SystemClock;
    use replicate::net::connect::host_and_port::HostAndPort;
    use replicate::net::replica::Replica;

    use crate::state::{ReplicaRole, State};

    #[tokio::test(flavor = "multi_thread")]
    async fn change_to_candidate() {
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

    #[tokio::test(flavor = "multi_thread")]
    async fn change_to_leader() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let state = State::new(Arc::new(some_replica), Arc::new(SystemClock::new()));
        let clone = state.clone();
        clone.change_to_candidate();
        clone.change_to_leader();

        assert_eq!(1, state.get_term());
        assert_eq!(ReplicaRole::Leader, state.get_role());
        assert_eq!(Some(10), state.get_voted_for());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn change_to_follower() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let state = State::new(Arc::new(some_replica), Arc::new(SystemClock::new()));
        let clone = state.clone();
        clone.change_to_candidate();
        clone.change_to_follower(2);

        assert_eq!(2, state.get_term());
        assert_eq!(ReplicaRole::Follower, state.get_role());
        assert_eq!(None, state.get_voted_for());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn heartbeat_timeout() {
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
        let count = Arc::new(RwLock::new(0));
        let cloned = count.clone();

        let election_starter = move |_state| {
            let mut write_guard = cloned.write().unwrap();
            *write_guard = *write_guard + 1;
        };

        let handle = tokio::spawn(state.get_heartbeat_checker(election_timeout, election_starter));

        let _ = handle.await;
        assert_eq!(1, *(count.read().unwrap()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn heartbeat_does_not_timeout() {
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
        let count = Arc::new(RwLock::new(0));
        let cloned = count.clone();

        let election_starter = move |_state| {
            let mut write_guard = cloned.write().unwrap();
            *write_guard = *write_guard + 1;
        };

        let handle = tokio::spawn(state.get_heartbeat_checker(election_timeout, election_starter));
        let _ = handle.await;

        assert_eq!(0, *(count.read().unwrap()));
    }
}