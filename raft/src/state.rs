use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use replicate::clock::clock::Clock;
use replicate::heartbeat::heartbeat_scheduler::SingleThreadedHeartbeatScheduler;
use replicate::net::connect::error::{AnyError, ServiceResponseError};
use replicate::net::replica::{Replica, ReplicaId};

use crate::election::election::Election;
use crate::heartbeat_config::HeartbeatConfig;
use crate::log::LogEntry;
use crate::net::factory::service_request::{BuiltInServiceRequestFactory, ServiceRequestFactory};
use crate::net::rpc::grpc::{AppendEntriesResponse, Command};

pub struct State {
    consensus_state: RwLock<ConsensusState>,
    replica: Arc<Replica>,
    clock: Arc<dyn Clock>,
    heartbeat_config: HeartbeatConfig,
    heartbeat_send_scheduler: SingleThreadedHeartbeatScheduler,
    heartbeat_check_scheduler: SingleThreadedHeartbeatScheduler,
    service_request_factory: Arc<dyn ServiceRequestFactory>,
}

struct ConsensusState {
    term: u64,
    role: ReplicaRole,
    voted_for: Option<u64>,
    heartbeat_received_time: Option<SystemTime>,
    creation_time: SystemTime,
    log_entries: Vec<LogEntry>,
    next_index: u64,
}

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum ReplicaRole {
    Leader,
    Follower,
    Candidate,
}

impl State {
    pub fn new(replica: Arc<Replica>, heartbeat_config: HeartbeatConfig) -> Arc<State> {
        return Self::new_with(replica, heartbeat_config, Arc::new(BuiltInServiceRequestFactory::new()));
    }

    fn new_with(replica: Arc<Replica>, heartbeat_config: HeartbeatConfig, service_request_factory: Arc<dyn ServiceRequestFactory>) -> Arc<State> {
        let clock = replica.get_clock();
        let heartbeat_config = heartbeat_config;
        let heartbeat_interval = heartbeat_config.get_heartbeat_interval();
        let heartbeat_timeout = heartbeat_config.get_heartbeat_timeout();

        let state = State {
            consensus_state: RwLock::new(ConsensusState {
                term: 0,
                role: ReplicaRole::Follower,
                voted_for: None,
                heartbeat_received_time: None,
                creation_time: clock.now(),
                log_entries: Vec::new(),
                next_index: 1,
            }),
            replica,
            clock,
            heartbeat_config,
            heartbeat_send_scheduler: SingleThreadedHeartbeatScheduler::new(heartbeat_interval),
            heartbeat_check_scheduler: SingleThreadedHeartbeatScheduler::new(heartbeat_timeout),
            service_request_factory,
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

    pub fn get_heartbeat_sender(self: Arc<State>) -> impl Future<Output=Result<(), AnyError>> {
        let term = self.get_term();
        let leader_id = self.replica.get_id();
        let replica = self.replica.clone();
        let service_request_factory = self.service_request_factory.clone();

        return async move {
            let service_request_constructor = || {
                service_request_factory.heartbeat(term, leader_id)
            };

            let response_handler_generator =
                move |response: Result<AppendEntriesResponse, ServiceResponseError>| {
                    match response {
                        Ok(response) => Some(self.clone().get_heartbeat_response_handler(response)),
                        Err(_) => None
                    }
                };

            replica.send_to_replicas_without_callback(service_request_constructor, Arc::new(response_handler_generator)).await;
            return Ok(());
        };
    }

    pub(crate) fn get_heartbeat_response_handler(self: Arc<State>, append_entry_response: AppendEntriesResponse) -> impl Future<Output=()> {
        let inner_state = self.clone();
        return async move {
            if !append_entry_response.success {
                inner_state.change_to_follower(append_entry_response.term);
            }
        };
    }

    pub(crate) fn get_heartbeat_checker<F>(self: Arc<State>, heartbeat_timeout: Duration, election_starter: F) -> impl Future<Output=Result<(), AnyError>>
        where F: FnOnce(Arc<State>) -> () {
        let inner_self = self.clone();
        let clock = self.clock.clone();

        return async move {
            let write_guard = inner_self.consensus_state.write().unwrap();
            let consensus_state = &*write_guard;
            match consensus_state.heartbeat_received_time {
                Some(last_heartbeat_time) => {
                    if clock.duration_since(last_heartbeat_time).ge(&heartbeat_timeout) {
                        election_starter(inner_self.clone());
                    }
                }
                None => {
                    if clock.duration_since(consensus_state.creation_time).ge(&heartbeat_timeout) {
                        election_starter(inner_self.clone());
                    }
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

    pub(crate) fn voted_for(&self, replica_id: ReplicaId) {
        let mut write_guard = self.consensus_state.write().unwrap();
        let mut consensus_state = &mut *write_guard;
        consensus_state.voted_for = Some(replica_id);
    }

    pub(crate) fn has_not_voted_for_or_matches(&self, replica_id: ReplicaId) -> bool {
        if let Some(voted_for) = self.get_voted_for() {
            if voted_for == replica_id {
                return true;
            }
            return false;
        }
        return true;
    }

    pub(crate) fn get_voted_for(&self) -> Option<ReplicaId> {
        let guard = self.consensus_state.read().unwrap();
        return (*guard).voted_for;
    }

    pub fn append_command(&self, command: &Command) {
        let mut write_guard = self.consensus_state.write().unwrap();
        let consensus_state = &mut *write_guard;
        let log_entries_size = consensus_state.log_entries.len();

        let log_entry = LogEntry::new(consensus_state.term, log_entries_size as u64, command);
        consensus_state.log_entries.push(log_entry);
    }

    pub(crate) fn matches_log_entry_term_at(&self, index: usize, term: u64) -> bool {
        let guard = self.consensus_state.read().unwrap();
        return match (*guard).log_entries.get(index) {
            None => false,
            Some(log_entry) => log_entry.matches_term(term)
        };
    }

    pub fn total_log_entries(&self) -> usize {
        let guard = self.consensus_state.read().unwrap();
        return (*guard).log_entries.len();
    }

    pub(crate) fn get_previous_log_index(&self) -> Option<u64> {
        let guard = self.consensus_state.read().unwrap();
        let next_index = (*guard).next_index;
        if next_index >= 1 {
            return Some(next_index - 1);
        }
        return None;
    }

    pub(crate) fn reduce_next_index(&self) {
        let mut write_guard = self.consensus_state.write().unwrap();
        let mut consensus_state = &mut *write_guard;
        consensus_state.next_index = consensus_state.next_index - 1;
    }

    pub(crate) fn get_next_log_index(&self) -> u64 {
        let guard = self.consensus_state.read().unwrap();
        return (*guard).next_index;
    }

    pub(crate) fn get_log_term_at(&self, index: usize) -> Option<u64> {
        let guard = self.consensus_state.read().unwrap();
        return match (*guard).log_entries.get(index) {
            None => None,
            Some(log_entry) => Some(log_entry.get_term())
        };
    }

    pub fn get_log_entry_at(&self, index: usize) -> Option<LogEntry> {
        let guard = self.consensus_state.read().unwrap();
        return match (*guard).log_entries.get(index) {
            None => None,
            Some(entry) => Some(LogEntry::from(entry))
        };
    }

    fn restart_heartbeat_checker(state: Arc<State>, heartbeat_check_scheduler: &SingleThreadedHeartbeatScheduler) {
        heartbeat_check_scheduler.restart_with(move || {
            let inner_state = state.clone();
            let heartbeat_timeout = inner_state.heartbeat_config.get_heartbeat_timeout();

            inner_state.get_heartbeat_checker(
                heartbeat_timeout,
                |state| Election::new(state).start(),
            )
        });
    }

    fn restart_heartbeat_sender(state: Arc<State>, heartbeat_send_scheduler: &SingleThreadedHeartbeatScheduler) {
        heartbeat_send_scheduler.restart_with(move || {
            let inner_state = state.clone();
            inner_state.get_heartbeat_sender()
        });
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::{Arc, RwLock};
    use std::sync::atomic::AtomicU64;
    use std::thread;
    use std::time::Duration;

    use tokio::runtime::Builder;

    use replicate::clock::clock::SystemClock;
    use replicate::net::connect::host_and_port::HostAndPort;
    use replicate::net::replica::Replica;

    use crate::heartbeat_config::HeartbeatConfig;
    use crate::log::LogEntry;
    use crate::net::rpc::grpc::Command;
    use crate::state::{ReplicaRole, State};
    use crate::state::tests::setup::{HeartbeatResponseClientType, IncrementingCorrelationIdServiceRequestFactory};

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

        let state = State::new(Arc::new(some_replica), HeartbeatConfig::default());
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

        let state = State::new(Arc::new(some_replica), HeartbeatConfig::default());
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

        let state = State::new(Arc::new(some_replica), HeartbeatConfig::default());
        let clone = state.clone();
        clone.change_to_candidate();
        clone.change_to_follower(2);

        assert_eq!(2, state.get_term());
        assert_eq!(ReplicaRole::Follower, state.get_role());
        assert_eq!(None, state.get_voted_for());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_voted_for_none() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let state = State::new(Arc::new(some_replica), HeartbeatConfig::default());

        assert_eq!(None, state.get_voted_for());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn has_not_voted() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let state = State::new(Arc::new(some_replica), HeartbeatConfig::default());

        assert_eq!(true, state.has_not_voted_for_or_matches(10));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn has_voted_for_the_replica_id() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let state = State::new(Arc::new(some_replica), HeartbeatConfig::default());
        state.voted_for(15);

        assert_eq!(true, state.has_not_voted_for_or_matches(15));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn does_not_match_the_replica_id_voted_for() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let state = State::new(Arc::new(some_replica), HeartbeatConfig::default());
        state.voted_for(10);

        assert_eq!(false, state.has_not_voted_for_or_matches(15));
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

        let state = State::new(Arc::new(some_replica), HeartbeatConfig::default());
        state.mark_heartbeat_received();

        let heartbeat_timeout = Duration::from_millis(0);
        let count = Arc::new(RwLock::new(0));
        let cloned = count.clone();

        let election_starter = move |_state| {
            let mut write_guard = cloned.write().unwrap();
            *write_guard = *write_guard + 1;
        };

        let handle = tokio::spawn(state.get_heartbeat_checker(
            heartbeat_timeout,
            election_starter,
        ));

        let _ = handle.await;
        assert_eq!(1, *(count.read().unwrap()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn never_received_heartbeat_times_out() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let state = State::new(Arc::new(some_replica), HeartbeatConfig::default());
        let heartbeat_timeout = Duration::from_millis(5);
        let count = Arc::new(RwLock::new(0));
        let cloned = count.clone();

        let election_starter = move |_state| {
            let mut write_guard = cloned.write().unwrap();
            *write_guard = *write_guard + 1;
        };

        thread::sleep(Duration::from_millis(5));
        let handle = tokio::spawn(state.get_heartbeat_checker(
            heartbeat_timeout,
            election_starter,
        ));

        let _ = handle.await;
        assert_eq!(1, *(count.read().unwrap()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn never_received_heartbeat_but_yet_to_time_out() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let state = State::new(Arc::new(some_replica), HeartbeatConfig::default());
        let heartbeat_timeout = Duration::from_millis(5);
        let count = Arc::new(RwLock::new(0));
        let cloned = count.clone();

        let election_starter = move |_state| {
            let mut write_guard = cloned.write().unwrap();
            *write_guard = *write_guard + 1;
        };

        thread::sleep(Duration::from_millis(2));
        let handle = tokio::spawn(state.get_heartbeat_checker(
            heartbeat_timeout,
            election_starter,
        ));

        let _ = handle.await;
        assert_eq!(0, *(count.read().unwrap()));
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

        let state = State::new(Arc::new(some_replica), HeartbeatConfig::default());
        state.mark_heartbeat_received();

        let heartbeat_timeout = Duration::from_secs(100);
        let count = Arc::new(RwLock::new(0));
        let cloned = count.clone();

        let election_starter = move |_state| {
            let mut write_guard = cloned.write().unwrap();
            *write_guard = *write_guard + 1;
        };

        let handle = tokio::spawn(state.get_heartbeat_checker(
            heartbeat_timeout,
            election_starter,
        ));
        let _ = handle.await;

        assert_eq!(0, *(count.read().unwrap()));
    }

    #[test]
    fn send_heartbeat() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let some_replica = Arc::new(some_replica);
        let inner_replica = some_replica.clone();

        let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let state = blocking_runtime.block_on(async move {
            return State::new_with(
                inner_replica,
                HeartbeatConfig::default(),
                Arc::new(IncrementingCorrelationIdServiceRequestFactory {
                    base_correlation_id: RwLock::new(AtomicU64::new(0)),
                    heartbeat_response_client_type: HeartbeatResponseClientType::Success,
                }),
            );
        });

        let inner_state = state.clone();
        blocking_runtime.block_on(async move {
            let result = inner_state.get_heartbeat_sender().await;
            assert!(result.is_ok());
        });
    }

    #[test]
    fn do_not_switch_to_follower_on_heartbeat_response() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let some_replica = Arc::new(some_replica);
        let inner_replica = some_replica.clone();

        let blocking_runtime = Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        let state = blocking_runtime.block_on(async move {
            let state = State::new_with(
                inner_replica,
                HeartbeatConfig::default(),
                Arc::new(IncrementingCorrelationIdServiceRequestFactory {
                    base_correlation_id: RwLock::new(AtomicU64::new(0)),
                    heartbeat_response_client_type: HeartbeatResponseClientType::Success,
                }),
            );
            state.clone().change_to_leader();
            return state;
        });

        let inner_state = state.clone();
        blocking_runtime.block_on(async move {
            let cloned = inner_state.clone();
            let _ = inner_state.get_heartbeat_sender().await;

            thread::sleep(Duration::from_secs(1));
            assert_eq!(ReplicaRole::Leader, cloned.get_role());
        });
    }

    #[test]
    fn switch_to_follower_on_heartbeat_response() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let some_replica = Arc::new(some_replica);
        let inner_replica = some_replica.clone();

        let blocking_runtime = Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        let state = blocking_runtime.block_on(async move {
            let state = State::new_with(
                inner_replica,
                HeartbeatConfig::default(),
                Arc::new(IncrementingCorrelationIdServiceRequestFactory {
                    base_correlation_id: RwLock::new(AtomicU64::new(0)),
                    heartbeat_response_client_type: HeartbeatResponseClientType::Failure,
                }),
            );
            state.clone().change_to_leader();
            return state;
        });

        let inner_state = state.clone();
        blocking_runtime.block_on(async move {
            let cloned = inner_state.clone();
            let _ = inner_state.get_heartbeat_sender().await;
            thread::sleep(Duration::from_millis(3));

            assert_eq!(ReplicaRole::Follower, cloned.get_role());
            assert_eq!(5, cloned.get_term());
        });
    }

    #[tokio::test]
    async fn append_command() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let state = State::new(Arc::new(some_replica), HeartbeatConfig::default());
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };

        state.append_command(&command);

        let log_entry = state.get_log_entry_at(0).unwrap();
        assert_eq!(0, log_entry.get_term());
        assert_eq!(0, log_entry.get_index());
        assert_eq!(content.as_bytes().to_vec(), log_entry.get_bytes_as_vec());
    }

    #[tokio::test]
    async fn get_non_existing_previous_log_index() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let state = State::new(Arc::new(some_replica), HeartbeatConfig::default());
        {
            let mut guard = state.consensus_state.write().unwrap();
            let consensus_state = &mut *guard;
            consensus_state.next_index = consensus_state.next_index - 1;
        }

        assert_eq!(None, state.get_previous_log_index());
    }

    #[tokio::test]
    async fn get_previous_log_index() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let state = State::new(Arc::new(some_replica), HeartbeatConfig::default());
        assert_eq!(Some(0), state.get_previous_log_index());
    }

    #[tokio::test]
    async fn get_log_term_at_non_existing_index() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let state = State::new(Arc::new(some_replica), HeartbeatConfig::default());
        assert_eq!(None, state.get_log_term_at(99));
    }

    #[tokio::test]
    async fn get_log_term_at_an_existing_index() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let state = State::new(Arc::new(some_replica), HeartbeatConfig::default());
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };

        state.append_command(&command);
        assert_eq!(Some(0), state.get_log_term_at(0));
    }

    #[tokio::test]
    async fn get_log_entry_at_non_existing_index() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let state = State::new(Arc::new(some_replica), HeartbeatConfig::default());
        assert_eq!(None, state.get_log_entry_at(99));
    }

    #[tokio::test]
    async fn get_log_entry_at_an_existing_index() {
        let some_replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297),
            ],
            Arc::new(SystemClock::new()),
        );

        let state = State::new(Arc::new(some_replica), HeartbeatConfig::default());
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };
        state.append_command(&command);

        assert_eq!(
            Some(LogEntry::new(0, 0, &command)),
            state.get_log_entry_at(0)
        );
    }

    mod setup {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::RwLock;

        use async_trait::async_trait;
        use tonic::{Request, Response};

        use replicate::net::connect::correlation_id::CorrelationId;
        use replicate::net::connect::error::ServiceResponseError;
        use replicate::net::connect::host_and_port::HostAndPort;
        use replicate::net::connect::service_client::{ServiceClientProvider, ServiceRequest};
        use replicate::net::replica::ReplicaId;

        use crate::net::factory::service_request::ServiceRequestFactory;
        use crate::net::rpc::grpc::AppendEntries;
        use crate::net::rpc::grpc::AppendEntriesResponse;

        #[derive(PartialEq)]
        pub(crate) enum HeartbeatResponseClientType {
            Success,
            Failure,
        }

        pub(crate) struct IncrementingCorrelationIdServiceRequestFactory {
            pub(crate) base_correlation_id: RwLock<AtomicU64>,
            pub(crate) heartbeat_response_client_type: HeartbeatResponseClientType,
        }

        impl ServiceRequestFactory for IncrementingCorrelationIdServiceRequestFactory {
            fn heartbeat(&self, term: u64, leader_id: ReplicaId) -> ServiceRequest<AppendEntries, AppendEntriesResponse> {
                {
                    let write_guard = self.base_correlation_id.write().unwrap();
                    write_guard.fetch_add(1, Ordering::SeqCst);
                }

                let guard = self.base_correlation_id.read().unwrap();
                let correlation_id: CorrelationId = guard.load(Ordering::SeqCst);

                let client: Box<dyn ServiceClientProvider<AppendEntries, AppendEntriesResponse>> = if self.heartbeat_response_client_type == HeartbeatResponseClientType::Success {
                    Box::new(TestHeartbeatSuccessClient {})
                } else {
                    Box::new(TestHeartbeatFailureClient {})
                };

                return ServiceRequest::new(
                    AppendEntries {
                        term,
                        leader_id,
                        correlation_id,
                        entry: None,
                        previous_log_index: None,
                        previous_log_term: None,
                    },
                    client,
                    correlation_id,
                );
            }
        }

        struct TestHeartbeatSuccessClient {}

        #[async_trait]
        impl ServiceClientProvider<AppendEntries, AppendEntriesResponse> for TestHeartbeatSuccessClient {
            async fn call(&self, _: Request<AppendEntries>, _: HostAndPort) -> Result<Response<AppendEntriesResponse>, ServiceResponseError> {
                return Ok(
                    Response::new(AppendEntriesResponse { term: 1, success: true, correlation_id: 10 })
                );
            }
        }

        struct TestHeartbeatFailureClient {}

        #[async_trait]
        impl ServiceClientProvider<AppendEntries, AppendEntriesResponse> for TestHeartbeatFailureClient {
            async fn call(&self, _: Request<AppendEntries>, _: HostAndPort) -> Result<Response<AppendEntriesResponse>, ServiceResponseError> {
                return Ok(
                    Response::new(AppendEntriesResponse { term: 5, success: false, correlation_id: 20 })
                );
            }
        }
    }
}