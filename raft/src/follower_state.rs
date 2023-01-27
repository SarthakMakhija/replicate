use std::future::Future;
use std::sync::Arc;

use dashmap::DashMap;

use replicate::net::connect::error::ServiceResponseError;
use replicate::net::connect::host_and_port::HostAndPort;
use replicate::net::connect::service_client::ServiceRequest;
use replicate::net::peers::Peers;
use replicate::net::replica::Replica;

use crate::net::rpc::grpc::{AppendEntries, AppendEntriesResponse, Command, Entry};
use crate::state::{ReplicaRole, State};

type NextLogIndex = u64;

pub(crate) struct FollowerState {
    peers: Peers,
    next_log_index_by_peer: DashMap<HostAndPort, NextLogIndex>,
}

impl FollowerState {
    pub(crate) fn new(
        replica: &Replica,
    ) -> Self {
        let addresses = replica.get_peers();

        let next_log_index_by_peer = DashMap::new();
        addresses.iter().for_each(|host_and_port| {
            next_log_index_by_peer.insert(host_and_port.clone(), 0);
        });

        let follower_state = FollowerState {
            peers: Peers::new(addresses),
            next_log_index_by_peer,
        };
        return follower_state;
    }

    pub(crate) fn replicate_log_at(self: Arc<FollowerState>, state: Arc<State>, latest_log_entry_index: u64) {
        let term = state.get_term();

        for peer_address in &self.peers.get_peer_addresses() {
            let next_log_index_by_peer = self.next_log_index_by_peer_for(peer_address);
            if latest_log_entry_index >= next_log_index_by_peer.1 {
                (&self).replicate_to(state.clone(), peer_address, next_log_index_by_peer, term)
            }
        }
    }

    pub(crate) fn register(self: Arc<FollowerState>, state: Arc<State>, response: AppendEntriesResponse, from: HostAndPort) {
        if response.success {
            self.acknowledge_log_index(from);
            return;
        }
        self.retry_reducing_log_index(state,from);
    }

    pub(crate) fn reset_next_log_index_to(&self, latest_log_index: u64) {
        self.peers.get_peer_addresses().iter().for_each(|host_and_port| {
            self.next_log_index_by_peer.entry(host_and_port.clone())
                .and_modify(|next_log_index| *next_log_index = latest_log_index);
        });
    }

    pub(crate) fn get_next_log_index_for(&self, peer_address: &HostAndPort) -> NextLogIndex {
        return *(self.next_log_index_by_peer.get(peer_address).unwrap().value());
    }

    fn replicate_to(self: &Arc<FollowerState>, state: Arc<State>, peer_address: &HostAndPort, next_log_index_by_peer: (HostAndPort, NextLogIndex), term: u64) {
        println!("replicating log at log index {} for the peer {:?}", next_log_index_by_peer.1, peer_address);

        let follower_state = self.clone();
        let handler_hook_state = state.clone();
        let service_request_state = state.clone();
        let peers = Peers::new(vec![peer_address.clone()]);

        handler_hook_state.get_replica_reference().send_to_with_handler_hook(
            &peers,
            move || self.service_request(&service_request_state, next_log_index_by_peer.1, term),
            Arc::new(move |peer, response: Result<AppendEntriesResponse, ServiceResponseError>| {
                return Self::append_entries_response_handler(follower_state.clone(), state.clone(), peer, response);
            }),
            || None,
        )
    }

    fn acknowledge_log_index(&self, peer: HostAndPort) {
        self.next_log_index_by_peer.entry(peer)
            .and_modify(|next_log_index| *next_log_index = *next_log_index + 1);
    }

    fn retry_reducing_log_index(self: Arc<FollowerState>, state: Arc<State>, peer: HostAndPort) {
        let next_log_index_by_peer = self.next_log_index_by_peer_for(&peer);
        let (previous_log_index, _) = self.previous_log_index_term(&state,next_log_index_by_peer.1);

        if let Some(previous_log_index) = previous_log_index {
            {
                self.next_log_index_by_peer.entry(peer.clone())
                    .and_modify(|next_log_index| *next_log_index = previous_log_index);
            }

            let term = state.get_term();
            let next_log_index_by_peer = self.next_log_index_by_peer_for(&peer);

            println!("retrying log replication at log index {} for the peer {:?}", next_log_index_by_peer.1, peer);
            let follower_state = self.clone();
            let handler_hook_state = state.clone();
            let service_request_state = state.clone();
            let peers = Peers::new(vec![peer.clone()]);

            handler_hook_state.get_replica_reference().send_to_with_handler_hook(
                &peers,
                move || self.service_request(&service_request_state, next_log_index_by_peer.1, term),
                Arc::new(move |peer, response: Result<AppendEntriesResponse, ServiceResponseError>| {
                    return Self::append_entries_response_handler(follower_state.clone(), state.clone(), peer, response);
                }),
                || None,
            )
        }
    }

    fn service_request(&self, state: &Arc<State>, next_log_index: NextLogIndex, term: u64) -> ServiceRequest<AppendEntries, AppendEntriesResponse> {
        let (previous_log_index, previous_log_term) = self.previous_log_index_term(state, next_log_index);

        let entry = match state.get_replicated_log_reference().get_log_entry_at(next_log_index as usize) {
            None => None,
            Some(entry) => {
                Some(
                    Entry {
                        command: Some(Command { command: entry.get_bytes_as_vec() }),
                        term: entry.get_term(),
                        index: entry.get_index(),
                    }
                )
            }
        };
        return state.get_service_request_factory_reference().replicate_log(
            term,
            state.get_replica_reference().get_id(),
            previous_log_index,
            previous_log_term,
            state.get_replicated_log_reference().get_commit_index(),
            entry,
        );
    }

    fn previous_log_index_term(&self, state: &Arc<State>, next_log_index: NextLogIndex) -> (Option<u64>, Option<u64>) {
        let previous_log_index = if next_log_index >= 1 {
            Some(next_log_index - 1)
        } else {
            None
        };

        let previous_log_term = match previous_log_index {
            None => None,
            Some(index) => state.get_replicated_log_reference().get_log_term_at(index as usize)
        };
        return (previous_log_index, previous_log_term);
    }

    fn next_log_index_by_peer_for(&self, peer_address: &HostAndPort) -> (HostAndPort, NextLogIndex) {
        return {
            let next_log_index_by_peer = self.next_log_index_by_peer.get(peer_address).unwrap();
            (next_log_index_by_peer.key().clone(), *next_log_index_by_peer.value())
        };
    }

    fn append_entries_response_handler(follower_state: Arc<FollowerState>,
                                       state: Arc<State>,
                                       peer: HostAndPort,
                                       response: Result<AppendEntriesResponse, ServiceResponseError>,
    ) -> Option<impl Future<Output=()>> {
        let inner_follower_state = follower_state.clone();
        return Some(
            async move {
                match response {
                    Ok(append_entries_response) => {
                        let state = state.clone();
                        let term = state.get_term();
                        if append_entries_response.term > term {
                            state.clone().change_to_follower(append_entries_response.term);
                        }
                        let replica_role = state.get_role();
                        if replica_role == ReplicaRole::Leader {
                            if append_entries_response.success {
                                let replicated_log = state.get_replicated_log_reference();
                                let log_entry_index = append_entries_response.log_entry_index.unwrap() as usize;

                                replicated_log.acknowledge_log_entry_at(log_entry_index);
                                if replicated_log.is_entry_replicated(log_entry_index) {
                                    replicated_log.commit(|commit_index| {
                                        state.get_pending_committed_log_entries_reference().handle_response(
                                            commit_index,
                                            state.get_replica_reference().get_self_address(),
                                            Ok(Box::new(())),
                                        );
                                    });
                                }
                            }
                            inner_follower_state.register(state, append_entries_response, peer);
                        }
                    }
                    Err(_err) => {
                        eprintln!("received AppendEntriesResponse with an error from the host {:?}", peer);
                    }
                }
            }
        );
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;

    use tokio::runtime::Builder;

    use replicate::clock::clock::SystemClock;
    use replicate::net::connect::host_and_port::HostAndPort;
    use replicate::net::replica::Replica;

    use crate::follower_state::{FollowerState, NextLogIndex};
    use crate::heartbeat_config::HeartbeatConfig;
    use crate::net::builder::log::ReplicateLogResponseBuilder;
    use crate::net::rpc::grpc::Command;
    use crate::state::{ReplicaRole, State};

    #[test]
    fn service_request_with_term() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peer = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061);

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            vec![peer],
            Box::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            let state = State::new(replica, HeartbeatConfig::default());
            state.change_to_candidate();
            return state;
        });

        let follower_state = Arc::new(FollowerState::new(
            state.get_replica_reference(),
        ));

        let next_log_index: NextLogIndex = 1;
        let term = 1;

        let service_request = follower_state.service_request(
            &state,
            next_log_index,
            term,
        );

        let payload = service_request.get_payload();
        assert_eq!(1, payload.term);
    }

    #[test]
    fn service_request_with_no_leader_commit() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peer = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061);

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            vec![peer],
            Box::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            return State::new(replica, HeartbeatConfig::default());
        });

        let follower_state = Arc::new(FollowerState::new(
            state.get_replica_reference(),
        ));

        let next_log_index: NextLogIndex = 1;
        let term = 1;

        let service_request = follower_state.service_request(
            &state,
            next_log_index,
            term,
        );

        let payload = service_request.get_payload();
        assert_eq!(None, payload.leader_commit_index);
    }

    #[test]
    fn service_request_with_leader_commit_index() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peer = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061);

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            vec![peer],
            Box::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            let state = State::new(replica, HeartbeatConfig::default());
            let content = String::from("Content");
            let command = Command { command: content.as_bytes().to_vec() };
            state.get_replicated_log_reference().append(
                &command,
                1,
            );

            state.get_replicated_log_reference().acknowledge_log_entry_at(0);
            state.get_replicated_log_reference().acknowledge_log_entry_at(0);
            state.get_replicated_log_reference().commit(|_| {});
            return state;
        });

        let follower_state = Arc::new(FollowerState::new(
            state.get_replica_reference(),
        ));

        let next_log_index: NextLogIndex = 1;
        let term = 1;

        let service_request = follower_state.service_request(
            &state,
            next_log_index,
            term,
        );

        let payload = service_request.get_payload();
        assert_eq!(Some(0), payload.leader_commit_index);
    }

    #[test]
    fn service_request_with_no_previous_log_index() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peer = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061);

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            vec![peer],
            Box::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            return State::new(replica, HeartbeatConfig::default());
        });

        let follower_state = Arc::new(FollowerState::new(
            state.get_replica_reference(),
        ));

        let next_log_index: NextLogIndex = 0;
        let term = 1;

        let service_request = follower_state.service_request(
            &state,
            next_log_index,
            term,
        );

        let payload = service_request.get_payload();
        assert_eq!(None, payload.previous_log_index);
        assert_eq!(None, payload.previous_log_term);
    }

    #[test]
    fn service_request_with_previous_log_index() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peer = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061);

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            vec![peer],
            Box::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            return State::new(replica, HeartbeatConfig::default());
        });

        let follower_state = Arc::new(FollowerState::new(
            state.get_replica_reference(),
        ));

        let next_log_index: NextLogIndex = 1;
        let term = 1;

        let service_request = follower_state.service_request(
            &state,
            next_log_index,
            term,
        );

        let payload = service_request.get_payload();
        assert_eq!(Some(0), payload.previous_log_index);
        assert_eq!(None, payload.previous_log_term);
    }

    #[test]
    fn service_request_with_previous_log_index_and_previous_log_term() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peer = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061);

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            vec![peer],
            Box::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            let state = State::new(replica, HeartbeatConfig::default());
            let content = String::from("Content");
            let command = Command { command: content.as_bytes().to_vec() };
            state.get_replicated_log_reference().append(
                &command,
                1,
            );
            return state;
        });

        let follower_state = Arc::new(FollowerState::new(
            state.get_replica_reference(),
        ));

        let next_log_index: NextLogIndex = 1;
        let term = 1;

        let service_request = follower_state.service_request(
            &state,
            next_log_index,
            term,
        );

        let payload = service_request.get_payload();
        assert_eq!(Some(0), payload.previous_log_index);
        assert_eq!(Some(1), payload.previous_log_term);
    }

    #[test]
    fn service_request_with_no_entry() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peer = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061);

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            vec![peer],
            Box::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            return State::new(replica, HeartbeatConfig::default());
        });

        let follower_state = Arc::new(FollowerState::new(
            state.get_replica_reference(),
        ));

        let next_log_index: NextLogIndex = 1;
        let term = 1;

        let service_request = follower_state.service_request(
            &state,
            next_log_index,
            term,
        );

        let payload = service_request.get_payload();
        assert_eq!(None, payload.entry);
    }

    #[test]
    fn service_request_with_an_entry() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peer = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061);

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            vec![peer],
            Box::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            let state = State::new(replica, HeartbeatConfig::default());
            let content = String::from("Content");
            let command = Command { command: content.as_bytes().to_vec() };
            state.get_replicated_log_reference().append(
                &command,
                1,
            );
            return state;
        });

        let follower_state = Arc::new(FollowerState::new(
            state.get_replica_reference(),
        ));

        let next_log_index: NextLogIndex = 0;
        let term = 1;

        let service_request = follower_state.service_request(
            &state,
            next_log_index,
            term,
        );

        let payload = service_request.get_payload();
        let entry = payload.entry.as_ref().unwrap();
        assert_eq!(1, entry.term);
        assert_eq!(0, entry.index);
    }

    #[test]
    fn register_success_response_from_peer() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peer = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061);

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            vec![peer],
            Box::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            return State::new(replica, HeartbeatConfig::default());
        });

        let follower_state = Arc::new(FollowerState::new(
            state.get_replica_reference(),
        ));

        runtime.block_on(async {
            let response = ReplicateLogResponseBuilder::success_response(
                1, 10, 5,
            );
            follower_state.clone().register(state, response, peer.clone());
        });

        let next_log_index_by_peer = follower_state.next_log_index_by_peer.get(&peer).unwrap();
        assert_eq!(1, *(next_log_index_by_peer.value()));
    }

    #[test]
    fn register_success_response_from_peer_and_increment_match_index_and_next_first_time() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peer = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061);

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            vec![peer],
            Box::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            return State::new(replica, HeartbeatConfig::default());
        });

        let follower_state = Arc::new(FollowerState::new(
            state.get_replica_reference(),
        ));

        runtime.block_on(async {
            let response = ReplicateLogResponseBuilder::success_response(
                1, 10, 5,
            );
            follower_state.clone().register(state, response, peer.clone());
        });

        let next_log_index_by_peer = follower_state.next_log_index_by_peer.get(&peer).unwrap();
        assert_eq!(1, *(next_log_index_by_peer.value()));
    }

    #[test]
    fn replicate_log_response_and_leader_steps_down() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peers = vec![HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061)];

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Box::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            let state = State::new(replica, HeartbeatConfig::default());
            state.clone().change_to_leader();
            return state;
        });

        let follower_state = Arc::new(FollowerState::new(
            state.get_replica_reference(),
        ));

        let handler = FollowerState::append_entries_response_handler(
            follower_state,
            state.clone(),
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061),
            Ok(ReplicateLogResponseBuilder::failure_response(3, 10)),
        );

        runtime.block_on(async {
            let future = handler.unwrap();
            future.await;

            assert_eq!(ReplicaRole::Follower, state.get_role());
            assert_eq!(3, state.get_term());
        });
    }

    #[test]
    fn replicate_log_response_and_leader_steps_down_does_not_acknowledge_following_log_responses() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peer_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061);
        let peers = vec![peer_host_and_port];

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Box::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            let state = State::new(replica, HeartbeatConfig::default());
            let content = String::from("anything");
            let command = Command { command: content.as_bytes().to_vec() };
            let term = state.get_term();
            state.get_replicated_log_reference().append(&command, term);

            state.clone().change_to_leader();
            return state;
        });

        let follower_state = Arc::new(FollowerState::new(
            state.get_replica_reference(),
        ));

        let handler = FollowerState::append_entries_response_handler(
            follower_state.clone(),
            state.clone(),
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061),
            Ok(ReplicateLogResponseBuilder::failure_response(3, 10)),
        );

        runtime.block_on(async {
            handler.unwrap().await;

            FollowerState::append_entries_response_handler(
                follower_state.clone(),
                state.clone(),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061),
                Ok(ReplicateLogResponseBuilder::success_response(0, 10, 0)),
            ).unwrap().await;

            FollowerState::append_entries_response_handler(
                follower_state.clone(),
                state.clone(),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061),
                Ok(ReplicateLogResponseBuilder::success_response(0, 10, 0)),
            ).unwrap().await;

            assert_eq!(ReplicaRole::Follower, state.get_role());
            assert_eq!(3, state.get_term());
            assert_eq!(None, state.get_replicated_log_reference().get_commit_index());
        });
    }

    #[test]
    fn replicate_log_response_and_leader_does_not_step_down() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peers = vec![HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061)];

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Box::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            let state = State::new(replica, HeartbeatConfig::default());
            let content = String::from("Content");
            let command = Command { command: content.as_bytes().to_vec() };
            state.get_replicated_log_reference().append(
                &command,
                0,
            );
            state.clone().change_to_leader();
            return state;
        });

        let follower_state = Arc::new(FollowerState::new(
            state.get_replica_reference(),
        ));

        let handler = FollowerState::append_entries_response_handler(
            follower_state.clone(),
            state.clone(),
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061),
            Ok(ReplicateLogResponseBuilder::success_response(0, 10, 0)),
        );

        runtime.block_on(async {
            handler.unwrap().await;

            assert_eq!(ReplicaRole::Leader, state.get_role());
            assert_eq!(0, state.get_term());
        });
    }

    #[test]
    fn replicate_log_response_and_do_not_commit_as_the_response_has_higher_term() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peers = vec![HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061)];

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Box::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            let state = State::new(replica, HeartbeatConfig::default());
            let content = String::from("anything");
            let command = Command { command: content.as_bytes().to_vec() };
            let term = state.get_term();

            state.get_replicated_log_reference().append(&command, term);
            state.clone().change_to_leader();
            return state;
        });

        let follower_state = Arc::new(FollowerState::new(
            state.get_replica_reference(),
        ));

        let handler = FollowerState::append_entries_response_handler(
            follower_state.clone(),
            state.clone(),
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061),
            Ok(ReplicateLogResponseBuilder::failure_response(1, 10)),
        );

        runtime.block_on(async {
            handler.unwrap().await;
            assert_eq!(None, state.get_replicated_log_reference().get_commit_index());
        });
    }

    #[test]
    fn replicate_log_response_and_commit() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peers = vec![HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061)];

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Box::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            let state = State::new(replica, HeartbeatConfig::default());
            let content = String::from("anything");
            let command = Command { command: content.as_bytes().to_vec() };
            let term = state.get_term();

            state.get_replicated_log_reference().append(&command, term);
            state.clone().change_to_leader();
            return state;
        });

        let follower_state = Arc::new(FollowerState::new(
            state.get_replica_reference(),
        ));

        runtime.block_on(async {
            FollowerState::append_entries_response_handler(
                follower_state.clone(),
                state.clone(),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061),
                Ok(ReplicateLogResponseBuilder::success_response(0, 10, 0)),
            ).unwrap().await;

            FollowerState::append_entries_response_handler(
                follower_state.clone(),
                state.clone(),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2062),
                Ok(ReplicateLogResponseBuilder::success_response(0, 10, 0)),
            ).unwrap().await;

            assert_eq!(0, state.get_replicated_log_reference().get_commit_index().unwrap());
        });
    }

    #[test]
    fn replicate_log_response_and_commit_with_one_false_response() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peer_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061);
        let peers = vec![peer_host_and_port];

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Box::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            let state = State::new(replica, HeartbeatConfig::default());
            let content = String::from("anything");
            let command = Command { command: content.as_bytes().to_vec() };
            let term = state.get_term();

            state.get_replicated_log_reference().append(&command, term);
            state.clone().change_to_leader();
            return state;
        });

        let follower_state = Arc::new(FollowerState::new(
            state.get_replica_reference(),
        ));

        runtime.block_on(async {
            FollowerState::append_entries_response_handler(
                follower_state.clone(),
                state.clone(),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061),
                Ok(ReplicateLogResponseBuilder::failure_response(0, 10)),
            ).unwrap().await;

            FollowerState::append_entries_response_handler(
                follower_state.clone(),
                state.clone(),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2062),
                Ok(ReplicateLogResponseBuilder::success_response(0, 10, 0)),
            ).unwrap().await;

            FollowerState::append_entries_response_handler(
                follower_state.clone(),
                state.clone(),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2062),
                Ok(ReplicateLogResponseBuilder::success_response(0, 10, 0)),
            ).unwrap().await;

            assert_eq!(0, state.get_replicated_log_reference().get_commit_index().unwrap());
        });
    }

    #[test]
    fn reset_next_index_to_0() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peer_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061);
        let peers = vec![peer_host_and_port];

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Box::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            return State::new(replica, HeartbeatConfig::default());
        });

        let follower_state = Arc::new(FollowerState::new(
            state.get_replica_reference(),
        ));

        follower_state.reset_next_log_index_to(0);

        let next_log_index_ref = follower_state.next_log_index_by_peer.get(&peer_host_and_port).unwrap();
        let next_log_index = next_log_index_ref.value();

        assert_eq!(0, *next_log_index);
    }

    #[test]
    fn reset_next_index_to_5() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peer_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061);
        let peers = vec![peer_host_and_port];

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Box::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            return State::new(replica, HeartbeatConfig::default());
        });

        let follower_state = Arc::new(FollowerState::new(
            state.get_replica_reference(),
        ));

        follower_state.reset_next_log_index_to(5);

        let next_log_index_ref = follower_state.next_log_index_by_peer.get(&peer_host_and_port).unwrap();
        let next_log_index = next_log_index_ref.value();

        assert_eq!(5, *next_log_index);
    }
}