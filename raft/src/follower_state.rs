use std::sync::Arc;

use dashmap::DashMap;
use tokio::task::JoinHandle;

use replicate::net::connect::async_network::AsyncNetwork;
use replicate::net::connect::error::ServiceResponseError;
use replicate::net::connect::host_and_port::HostAndPort;
use replicate::net::connect::service_client::ServiceRequest;

use crate::net::factory::service_request::ServiceRequestFactory;
use crate::net::rpc::grpc::{AppendEntries, AppendEntriesResponse, Command, Entry};
use crate::state::State;

type NextLogIndex = u64;

pub(crate) struct FollowerState {
    peers: Vec<HostAndPort>,
    state: Arc<State>,
    next_log_index_by_peer: DashMap<HostAndPort, NextLogIndex>,
    service_request_factory: Arc<dyn ServiceRequestFactory>,
}

impl FollowerState {
    pub(crate) fn new(
        state: Arc<State>,
        service_request_factory: Arc<dyn ServiceRequestFactory>,
    ) -> Self {
        let replica = state.get_replica_reference();
        let peers = replica.get_peers();

        let next_log_index_by_peer = DashMap::new();
        peers.iter().for_each(|host_and_port| {
            next_log_index_by_peer.insert(host_and_port.clone(), 0);
        });

        let follower_state = FollowerState {
            peers,
            state,
            next_log_index_by_peer,
            service_request_factory,
        };
        return follower_state;
    }

    pub(crate) fn replicate_log_at(self: Arc<FollowerState>, log_entry_index: u64) -> Vec<JoinHandle<Result<(), ServiceResponseError>>> {
        let term = self.state.get_term();
        let source_address = self.state.get_replica_reference().get_self_address();
        let mut task_handles = Vec::new();

        for peer in &self.peers {
            let next_log_index_by_peer = self.next_log_index_by_peer_for(peer);
            println!("replicating log at log index {} for the peer {:?}", next_log_index_by_peer.1, peer);

            let service_request = self.service_request(next_log_index_by_peer.1, log_entry_index, term);
            let target_address = peer.clone();

            task_handles.push(
                tokio::spawn(async move {
                    AsyncNetwork::send_with_source_footprint(
                        service_request,
                        source_address,
                        target_address,
                    ).await
                })
            );
        }
        return task_handles;
    }

    pub(crate) fn register(self: Arc<FollowerState>, response: AppendEntriesResponse, from: HostAndPort) {
        if response.success {
            self.acknowledge_log_index(response, from);
            return;
        }
        self.retry_reducing_log_index(from);
    }

    fn acknowledge_log_index(&self, response: AppendEntriesResponse, peer: HostAndPort) {
        let response_log_entry_index = response.log_entry_index.unwrap();
        self.next_log_index_by_peer.entry(peer)
            .and_modify(|next_log_index| *next_log_index = response_log_entry_index + 1);
    }

    fn retry_reducing_log_index(&self, peer: HostAndPort) {
        let next_log_index_by_peer = self.next_log_index_by_peer_for(&peer);
        let (previous_log_index, _) = self.previous_log_index_term(next_log_index_by_peer.1);

        if let Some(previous_log_index) = previous_log_index {
            {
                self.next_log_index_by_peer.entry(peer.clone())
                    .and_modify(|next_log_index| *next_log_index = previous_log_index);
            }

            let term = self.state.get_term();
            let latest_log_entry_index = self.state.get_replicated_log_reference().get_latest_log_entry_index();
            let source_address = self.state.get_replica_reference().get_self_address();
            let next_log_index_by_peer = self.next_log_index_by_peer_for(&peer);
            let service_request = self.service_request(next_log_index_by_peer.1, latest_log_entry_index, term);

            println!("retrying log replication at log index {} for the peer {:?}", next_log_index_by_peer.1, peer);
            tokio::spawn(async move {
                AsyncNetwork::send_with_source_footprint(
                    service_request,
                    source_address,
                    peer,
                ).await
            });
        }
    }

    fn service_request(&self, next_log_index: NextLogIndex, latest_log_entry_index: u64, term: u64) -> ServiceRequest<AppendEntries, ()> {
        let (previous_log_index, previous_log_term) = self.previous_log_index_term(latest_log_entry_index);

        let entry = match self.state.get_replicated_log_reference().get_log_entry_at(next_log_index as usize) {
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
        return self.service_request_factory.replicate_log(
            term,
            self.state.get_replica_reference().get_id(),
            previous_log_index,
            previous_log_term,
            self.state.get_replicated_log_reference().get_commit_index(),
            entry,
        );
    }

    fn previous_log_index_term(&self, latest_log_entry_index: u64) -> (Option<u64>, Option<u64>) {
        let previous_log_index = if latest_log_entry_index >= 1 {
            Some(latest_log_entry_index - 1)
        } else {
            None
        };

        let previous_log_term = match previous_log_index {
            None => None,
            Some(index) => self.state.get_replicated_log_reference().get_log_term_at(index as usize)
        };
        return (previous_log_index, previous_log_term);
    }

    fn next_log_index_by_peer_for(&self, peer: &HostAndPort) -> (HostAndPort, NextLogIndex) {
        return {
            let next_log_index_by_peer = self.next_log_index_by_peer.get(peer).unwrap();
            (next_log_index_by_peer.key().clone(), *next_log_index_by_peer.value())
        };
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;

    use tokio::runtime::Builder;

    use replicate::clock::clock::SystemClock;
    use replicate::net::connect::host_and_port::HostAndPort;
    use replicate::net::connect::service_client::ServiceRequest;
    use replicate::net::replica::Replica;

    use crate::follower_state::{FollowerState, NextLogIndex};
    use crate::heartbeat_config::HeartbeatConfig;
    use crate::net::factory::service_request::BuiltInServiceRequestFactory;
    use crate::net::rpc::grpc::{AppendEntries, AppendEntriesResponse, Command};
    use crate::state::State;

    #[test]
    fn service_request_with_term() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peer = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061);

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            vec![peer],
            Arc::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            let state = State::new(Arc::new(replica), HeartbeatConfig::default());
            state.change_to_candidate();
            return state;
        });

        let follower_state = Arc::new(FollowerState::new(
            state,
            Arc::new(BuiltInServiceRequestFactory::new()),
        ));

        let next_log_index : NextLogIndex = 1;
        let latest_log_entry_index: u64 = 1;
        let term = 1;

        let service_request: ServiceRequest<AppendEntries, ()> = follower_state.service_request(
            next_log_index,
            latest_log_entry_index,
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
            Arc::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            return State::new(Arc::new(replica), HeartbeatConfig::default());
        });

        let follower_state = Arc::new(FollowerState::new(
            state,
            Arc::new(BuiltInServiceRequestFactory::new()),
        ));

        let next_log_index : NextLogIndex = 1;
        let latest_log_entry_index: u64 = 1;
        let term = 1;

        let service_request: ServiceRequest<AppendEntries, ()> = follower_state.service_request(
            next_log_index,
            latest_log_entry_index,
            term
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
            Arc::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            let state = State::new(Arc::new(replica), HeartbeatConfig::default());
            let content = String::from("Content");
            let command = Command { command: content.as_bytes().to_vec() };
            state.get_replicated_log_reference().append_command(
                &command,
                1,
            );

            state.get_replicated_log_reference().acknowledge_log_entry_at(0);
            state.get_replicated_log_reference().acknowledge_log_entry_at(0);
            state.get_replicated_log_reference().commit(|_| {});
            return state;
        });

        let follower_state = Arc::new(FollowerState::new(
            state,
            Arc::new(BuiltInServiceRequestFactory::new()),
        ));

        let next_log_index : NextLogIndex = 1;
        let latest_log_entry_index: u64 = 1;
        let term = 1;

        let service_request: ServiceRequest<AppendEntries, ()> = follower_state.service_request(
            next_log_index,
            latest_log_entry_index,
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
            Arc::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            return State::new(Arc::new(replica), HeartbeatConfig::default());
        });

        let follower_state = Arc::new(FollowerState::new(
            state,
            Arc::new(BuiltInServiceRequestFactory::new()),
        ));

        let next_log_index : NextLogIndex = 0;
        let latest_log_entry_index: u64 = 0;
        let term = 1;

        let service_request: ServiceRequest<AppendEntries, ()> = follower_state.service_request(
            next_log_index,
            latest_log_entry_index,
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
            Arc::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            return State::new(Arc::new(replica), HeartbeatConfig::default());
        });

        let follower_state = Arc::new(FollowerState::new(
            state,
            Arc::new(BuiltInServiceRequestFactory::new()),
        ));

        let next_log_index : NextLogIndex = 1;
        let latest_log_entry_index: u64 = 1;
        let term = 1;

        let service_request: ServiceRequest<AppendEntries, ()> = follower_state.service_request(
            next_log_index,
            latest_log_entry_index,
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
            Arc::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            let state = State::new(Arc::new(replica), HeartbeatConfig::default());
            let content = String::from("Content");
            let command = Command { command: content.as_bytes().to_vec() };
            state.get_replicated_log_reference().append_command(
                &command,
                1,
            );
            return state;
        });

        let follower_state = Arc::new(FollowerState::new(
            state,
            Arc::new(BuiltInServiceRequestFactory::new()),
        ));

        let next_log_index : NextLogIndex = 1;
        let latest_log_entry_index: u64 = 1;
        let term = 1;

        let service_request: ServiceRequest<AppendEntries, ()> = follower_state.service_request(
            next_log_index,
            latest_log_entry_index,
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
            Arc::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            return State::new(Arc::new(replica), HeartbeatConfig::default());
        });

        let follower_state = Arc::new(FollowerState::new(
            state,
            Arc::new(BuiltInServiceRequestFactory::new()),
        ));

        let next_log_index : NextLogIndex = 1;
        let latest_log_entry_index: u64 = 1;
        let term = 1;

        let service_request: ServiceRequest<AppendEntries, ()> = follower_state.service_request(
            next_log_index,
            latest_log_entry_index,
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
            Arc::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            let state = State::new(Arc::new(replica), HeartbeatConfig::default());
            let content = String::from("Content");
            let command = Command { command: content.as_bytes().to_vec() };
            state.get_replicated_log_reference().append_command(
                &command,
                1,
            );
            return state;
        });

        let follower_state = Arc::new(FollowerState::new(
            state,
            Arc::new(BuiltInServiceRequestFactory::new()),
        ));

        let next_log_index : NextLogIndex = 0;
        let latest_log_entry_index: u64 = 0;
        let term = 1;

        let service_request: ServiceRequest<AppendEntries, ()> = follower_state.service_request(
            next_log_index,
            latest_log_entry_index,
            term
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
            Arc::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            return State::new(Arc::new(replica), HeartbeatConfig::default());
        });

        let follower_state = Arc::new(FollowerState::new(
            state,
            Arc::new(BuiltInServiceRequestFactory::new()),
        ));

        runtime.block_on(async {
            follower_state.clone().register(AppendEntriesResponse {
                term: 1,
                success: true,
                log_entry_index: Some(10),
                correlation_id: 10,
            }, peer.clone());
        });

        let next_log_index_by_peer = follower_state.next_log_index_by_peer.get(&peer).unwrap();
        assert_eq!(11, *(next_log_index_by_peer.value()));
    }
}