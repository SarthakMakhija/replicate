use std::sync::Arc;

use tonic::{Request, Response};

use replicate::callback::async_quorum_callback::AsyncQuorumCallback;
use replicate::net::connect::async_network::AsyncNetwork;
use replicate::net::connect::correlation_id::CorrelationIdGenerator;
use replicate::net::connect::host_port_extractor::HostAndPortExtractor;
use replicate::net::connect::random_correlation_id_generator::RandomCorrelationIdGenerator;

use crate::net::factory::service_request::{BuiltInServiceRequestFactory, ServiceRequestFactory};
use crate::net::rpc::grpc::{AppendEntries, AppendEntriesResponse, Command, Entry, RequestVote, RequestVoteResponse};
use crate::net::rpc::grpc::raft_server::Raft;
use crate::state::{ReplicaRole, State};

pub struct RaftService {
    state: Arc<State>,
    service_request_factory: Arc<dyn ServiceRequestFactory>,
}

impl RaftService {
    pub fn new(state: Arc<State>) -> Self {
        return RaftService { state, service_request_factory: Arc::new(BuiltInServiceRequestFactory::new()) };
    }
}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn acknowledge_request_vote(&self, request: Request<RequestVote>) -> Result<Response<()>, tonic::Status> {
        let originating_host_port = request.try_referral_host_port()?;

        let state = self.state.clone();
        let request = request.into_inner();
        let correlation_id = request.correlation_id;
        let replica = self.state.get_replica();
        let source_address = replica.get_self_address();
        let service_request_factory = self.service_request_factory.clone();

        println!("received RequestVote with term {}", request.term);
        let handler = async move {
            let term = state.get_term();
            let role = state.get_role();
            let voted: bool = if request.term > term && role != ReplicaRole::Leader && state.has_not_voted_for_or_matches(request.replica_id) {
                true
            } else {
                false
            };
            if voted {
                state.voted_for(request.replica_id);
            }

            let send_result = AsyncNetwork::send_with_source_footprint(
                service_request_factory.request_vote_response(term, voted, correlation_id),
                source_address,
                originating_host_port,
            ).await;

            if send_result.is_err() {
                eprintln!("failed to send request_vote_response to {:?}", originating_host_port);
            }
        };
        let _ = replica.submit_to_queue(handler);
        return Ok(Response::new(()));
    }

    async fn finish_request_vote(&self, request: Request<RequestVoteResponse>) -> Result<Response<()>, tonic::Status> {
        let originating_host_port = request.try_referral_host_port()?;
        let response = request.into_inner();
        println!("received RequestVoteResponse with voted? {}", response.voted);

        let _ = &self.state.get_replica_reference().register_response(response.correlation_id, originating_host_port, Ok(Box::new(response)));
        return Ok(Response::new(()));
    }

    async fn acknowledge_heartbeat(&self, request: Request<AppendEntries>) -> Result<Response<AppendEntriesResponse>, tonic::Status> {
        println!("received heartbeat on {:?}", self.state.get_replica_reference().get_self_address());
        let state = self.state.clone();
        let replica = self.state.get_replica_reference();

        let append_entries = request.into_inner();
        let handler = async move {
            state.mark_heartbeat_received();
            let term = state.get_term();
            if append_entries.term > term {
                state.change_to_follower(append_entries.term);
                return AppendEntriesResponse { success: true, term: append_entries.term, correlation_id: append_entries.correlation_id };
            }
            if append_entries.term == term {
                return AppendEntriesResponse { success: true, term, correlation_id: append_entries.correlation_id };
            }
            return AppendEntriesResponse { success: false, term, correlation_id: append_entries.correlation_id };
        };

        return match replica.add_to_queue(handler).await {
            Ok(append_entries_response) =>
                Ok(Response::new(append_entries_response)),
            Err(err) =>
                Err(tonic::Status::unknown(err.to_string()))
        };
    }

    async fn acknowledge_replicate_log(&self, request: Request<AppendEntries>) -> Result<Response<()>, tonic::Status> {
        println!("received replicate_log on {:?}", self.state.get_replica_reference().get_self_address());

        let originating_host_port = request.try_referral_host_port()?;
        let state = self.state.clone();
        let replica = self.state.get_replica_reference();

        let inner_replica = self.state.get_replica();
        let service_request_factory = self.service_request_factory.clone();
        let append_entries = request.into_inner();

        let handler = async move {
            state.mark_heartbeat_received();

            let term = state.get_term();
            if append_entries.term > term {
                state.clone().change_to_follower(append_entries.term);
            }

            let success;
            if term > append_entries.term {
                success = false;
            } else if append_entries.previous_log_index.is_none() {
                success = true;
            } else if !state.matches_log_entry_term_at(append_entries.previous_log_index.unwrap() as usize, append_entries.previous_log_term.unwrap()) {
                success = false;
            } else {
                success = true;
            };
            if success {
                let command = append_entries.entry.unwrap().command.unwrap();
                state.append_command(&command);
            }

            let correlation_id_generator = RandomCorrelationIdGenerator::new();
            let correlation_id = correlation_id_generator.generate();

            let send_result = AsyncNetwork::send_with_source_footprint(
                service_request_factory.replicate_log_response(term, success, correlation_id),
                inner_replica.get_self_address(),
                originating_host_port,
            ).await;

            if send_result.is_err() {
                eprintln!("failed to send append_entries_response to {:?}", originating_host_port);
            }
        };

        let _ = replica.submit_to_queue(handler);
        return Ok(Response::new(()));
    }

    async fn finish_replicate_log(&self, request: Request<AppendEntriesResponse>) -> Result<Response<()>, tonic::Status> {
        let originating_host_port = request.try_referral_host_port()?;
        let response = request.into_inner();
        println!("received AppendEntriesResponse with success? {}", response.success);

        let _ = &self.state.get_replica_reference().register_response(response.correlation_id, originating_host_port, Ok(Box::new(response)));
        return Ok(Response::new(()));
    }

    async fn execute(&self, request: Request<Command>) -> Result<Response<()>, tonic::Status> {
        println!("received command on {:?}", self.state.get_replica_reference().get_self_address());
        let state = self.state.clone();
        let replica = self.state.get_replica_reference();
        let inner_replica = self.state.get_replica();
        let service_request_factory = self.service_request_factory.clone();
        let command = request.into_inner();

        let handler = async move {
            state.append_command(&command);

            let previous_log_index = state.get_previous_log_index();
            let previous_log_term = match previous_log_index {
                None => None,
                Some(index) => state.get_log_term_at(index as usize)
            };

            let service_request_constructor = || {
                let term = state.get_term();
                let entry = match state.get_log_entry_at(state.get_next_log_index() as usize) {
                    None => None,
                    Some(entry) => {
                        Some(
                            Entry {
                                command: Some(Command{command: entry.get_bytes_as_vec()}),
                                term: entry.get_term(),
                                index: entry.get_index(),
                            }
                        )
                    }
                };

                service_request_factory.replicate_log(
                    term,
                    inner_replica.get_id(),
                    previous_log_index,
                    previous_log_term,
                    entry
                )
            };

            let callback = AsyncQuorumCallback::<AppendEntriesResponse>::new(inner_replica.total_peer_count());
            let total_failed_sends = inner_replica.send_to_replicas(service_request_constructor, callback.clone()).await;
            println!("total_failed_sends while replicating log {}", total_failed_sends);

            let _ = callback.handle().await;
        };

        let _ = replica.submit_to_queue(handler);
        return Ok(Response::new(()));
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use tokio::runtime::Builder;
    use tonic::{Request, Response};

    use replicate::clock::clock::SystemClock;
    use replicate::net::connect::host_and_port::HostAndPort;
    use replicate::net::connect::host_port_extractor::HostAndPortHeaderAdder;
    use replicate::net::replica::Replica;

    use crate::heartbeat_config::HeartbeatConfig;
    use crate::net::rpc::grpc::{AppendEntries, AppendEntriesResponse, Command, Entry, RequestVote};
    use crate::net::rpc::grpc::raft_server::Raft;
    use crate::net::service::raft_service::RaftService;
    use crate::state::{ReplicaRole, State};

    #[test]
    fn acknowledge_request_vote_successfully_voted() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peers = vec![HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061)];

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Arc::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            return State::new(Arc::new(replica), HeartbeatConfig::default());
        });

        let inner_state = state.clone();
        let _ = runtime.block_on(async move {
            let raft_service = RaftService::new(inner_state.clone());

            let mut request = Request::new(RequestVote { term: 10, replica_id: 30, correlation_id: 20 });
            request.add_host_port(self_host_and_port);

            let _ = raft_service.acknowledge_request_vote(request).await;
        });

        thread::sleep(Duration::from_millis(5));
        assert_eq!(Some(30), state.get_voted_for());
    }

    #[test]
    fn acknowledge_request_vote_do_not_vote_given_replica_is_the_leader() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peers = vec![HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061)];

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Arc::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            let state = State::new(Arc::new(replica), HeartbeatConfig::default());
            let state_clone = state.clone();

            state_clone.change_to_leader();
            return state;
        });

        let inner_state = state.clone();
        let _ = runtime.block_on(async move {
            let raft_service = RaftService::new(inner_state.clone());

            let mut request = Request::new(RequestVote { term: 10, replica_id: 30, correlation_id: 20 });
            request.add_host_port(self_host_and_port);

            let _ = raft_service.acknowledge_request_vote(request).await;
        });

        thread::sleep(Duration::from_millis(5));
        assert_eq!(None, state.get_voted_for());
    }

    #[test]
    fn acknowledge_request_vote_do_not_vote_given_replica_has_already_voted() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peers = vec![HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061)];

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Arc::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            let state = State::new(Arc::new(replica), HeartbeatConfig::default());
            let state_clone = state.clone();

            state_clone.voted_for(20);
            return state;
        });

        let inner_state = state.clone();
        let _ = runtime.block_on(async move {
            let raft_service = RaftService::new(inner_state.clone());

            let mut request = Request::new(RequestVote { term: 10, replica_id: 30, correlation_id: 20 });
            request.add_host_port(self_host_and_port);

            let _ = raft_service.acknowledge_request_vote(request).await;
        });

        thread::sleep(Duration::from_millis(5));
        assert_eq!(Some(20), state.get_voted_for());
    }

    #[test]
    fn acknowledge_request_vote_do_not_vote_given_the_request_term_not_higher() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peers = vec![HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061)];

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Arc::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            return State::new(Arc::new(replica), HeartbeatConfig::default());
        });

        let inner_state = state.clone();
        let _ = runtime.block_on(async move {
            let raft_service = RaftService::new(inner_state.clone());

            let mut request = Request::new(RequestVote { term: 0, replica_id: 30, correlation_id: 20 });
            request.add_host_port(self_host_and_port);

            let _ = raft_service.acknowledge_request_vote(request).await;
        });

        thread::sleep(Duration::from_millis(5));
        assert_eq!(None, state.get_voted_for());
    }

    #[test]
    fn acknowledge_heartbeat_mark_heartbeat_received() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peers = vec![HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061)];
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Arc::new(SystemClock::new()),
        );

        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let state = runtime.block_on(async move {
            return State::new(Arc::new(replica), HeartbeatConfig::default());
        });

        let inner_state = state.clone();
        let _ = runtime.block_on(async move {
            let raft_service = RaftService::new(inner_state.clone());
            let _ = raft_service.acknowledge_heartbeat(
                Request::new(
                    AppendEntries {
                        term: 1,
                        leader_id: 10,
                        correlation_id: 20,
                        entry: None,
                        previous_log_index: None,
                        previous_log_term: None,
                    }
                )
            ).await;

            assert!(inner_state.get_heartbeat_received_time().is_some());
        });
    }

    #[test]
    fn acknowledge_heartbeat_with_request_containing_higher_term() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peers = vec![HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061)];
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Arc::new(SystemClock::new()),
        );

        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let state = runtime.block_on(async move {
            return State::new(Arc::new(replica), HeartbeatConfig::default());
        });

        let inner_state = state.clone();
        let _ = runtime.block_on(async move {
            let raft_service = RaftService::new(inner_state.clone());
            let result: Result<Response<AppendEntriesResponse>, tonic::Status> = raft_service.acknowledge_heartbeat(
                Request::new(
                    AppendEntries {
                        term: 1,
                        leader_id: 10,
                        correlation_id: 20,
                        entry: None,
                        previous_log_index: None,
                        previous_log_term: None,
                    }
                )
            ).await;

            let response = result.unwrap().into_inner();
            assert_eq!(true, response.success);
            assert_eq!(1, response.term);
            assert_eq!(1, inner_state.get_term());
        });
    }

    #[test]
    fn acknowledge_heartbeat_with_request_containing_same_term() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2069);
        let peers = vec![HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061)];
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Arc::new(SystemClock::new()),
        );

        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let state = runtime.block_on(async move {
            return State::new(Arc::new(replica), HeartbeatConfig::default());
        });

        let inner_state = state.clone();
        let _ = runtime.block_on(async move {
            let raft_service = RaftService::new(inner_state.clone());

            let result: Result<Response<AppendEntriesResponse>, tonic::Status> = raft_service.acknowledge_heartbeat(
                Request::new(
                    AppendEntries {
                        term: 0,
                        leader_id: 10,
                        correlation_id: 20,
                        entry: None,
                        previous_log_index: None,
                        previous_log_term: None,
                    }
                )
            ).await;

            let response = result.unwrap().into_inner();

            assert_eq!(true, response.success);
            assert_eq!(0, response.term);
            assert_eq!(0, inner_state.get_term());
        });
    }

    #[test]
    fn acknowledge_heartbeat_with_request_containing_smaller_term() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2068);
        let peers = vec![HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061)];
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Arc::new(SystemClock::new()),
        );

        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let state = runtime.block_on(async move {
            let state = State::new(Arc::new(replica), HeartbeatConfig::default());
            state.change_to_candidate();
            return state;
        });

        let inner_state = state.clone();
        let _ = runtime.block_on(async move {
            let raft_service = RaftService::new(inner_state.clone());
            let result: Result<Response<AppendEntriesResponse>, tonic::Status> = raft_service.acknowledge_heartbeat(
                Request::new(
                    AppendEntries {
                        term: 0,
                        leader_id: 10,
                        correlation_id: 20,
                        entry: None,
                        previous_log_index: None,
                        previous_log_term: None,
                    }
                )
            ).await;

            let response = result.unwrap().into_inner();

            assert_eq!(false, response.success);
            assert_eq!(1, response.term);
            assert_eq!(1, inner_state.get_term());
        });
    }

    #[test]
    fn execute_command() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peers = vec![HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061)];

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Arc::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            return State::new(Arc::new(replica), HeartbeatConfig::default());
        });

        let inner_state = state.clone();
        let _ = runtime.block_on(async move {
            let raft_service = RaftService::new(inner_state.clone());
            let content = String::from("Content");
            let command = Command { command: content.as_bytes().to_vec() };

            let mut request = Request::new(command);
            request.add_host_port(self_host_and_port);

            let _ = raft_service.execute(request).await;
        });

        thread::sleep(Duration::from_millis(5));
        assert!(state.matches_log_entry_term_at(0, state.get_term()));

        let expected_command = Command { command: String::from("Content").as_bytes().to_vec() };
        assert!(state.matches_log_entry_command_at(0, &expected_command));
    }

    #[test]
    fn do_not_replicate_log_given_the_request_term_not_higher() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peers = vec![HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061)];

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Arc::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            let state = State::new(Arc::new(replica), HeartbeatConfig::default());
            state.change_to_candidate();

            return state;
        });

        let inner_state = state.clone();
        let _ = runtime.block_on(async move {
            let raft_service = RaftService::new(inner_state.clone());
            let content = String::from("Content");
            let command = Command { command: content.as_bytes().to_vec() };

            let mut request = Request::new(AppendEntries {
                term: 0,
                leader_id: 30,
                correlation_id: 10,
                entry: Some(Entry {
                    term: 0,
                    index: 1,
                    command: Some(command),
                }),
                previous_log_index: None,
                previous_log_term: None,
            });
            request.add_host_port(self_host_and_port);

            let _ = raft_service.acknowledge_replicate_log(request).await;
        });

        thread::sleep(Duration::from_millis(5));
        assert_eq!(0, state.total_log_entries());
    }

    #[test]
    fn become_follower_on_replicate_log_given_request_term_is_higher() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peers = vec![HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061)];

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Arc::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            let state = State::new(Arc::new(replica), HeartbeatConfig::default());
            state.change_to_candidate();

            return state;
        });

        let inner_state = state.clone();
        let _ = runtime.block_on(async move {
            let raft_service = RaftService::new(inner_state.clone());
            let content = String::from("Content");
            let command = Command { command: content.as_bytes().to_vec() };

            let mut request = Request::new(AppendEntries {
                term: 3,
                leader_id: 30,
                correlation_id: 10,
                entry: Some(Entry {
                    term: 3,
                    index: 1,
                    command: Some(command),
                }),
                previous_log_index: None,
                previous_log_term: None,
            });
            request.add_host_port(self_host_and_port);

            let _ = raft_service.acknowledge_replicate_log(request).await;
        });

        thread::sleep(Duration::from_millis(5));
        assert_eq!(ReplicaRole::Follower, state.get_role());
    }

    #[test]
    fn do_not_replicate_log_given_the_previous_entry_terms_do_not_match() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peers = vec![HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061)];

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Arc::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            return State::new(Arc::new(replica), HeartbeatConfig::default());
        });

        let inner_state = state.clone();
        let _ = runtime.block_on(async move {
            let raft_service = RaftService::new(inner_state.clone());
            let content = String::from("Content");
            let command = Command { command: content.as_bytes().to_vec() };

            let mut request = Request::new(AppendEntries {
                term: 1,
                leader_id: 30,
                correlation_id: 10,
                entry: Some(Entry {
                    term: 1,
                    index: 1,
                    command: Some(command),
                }),
                previous_log_index: Some(0),
                previous_log_term: Some(0),
            });
            request.add_host_port(self_host_and_port);

            let _ = raft_service.acknowledge_replicate_log(request).await;
        });

        thread::sleep(Duration::from_millis(5));
        assert_eq!(0, state.total_log_entries());
    }

    #[test]
    fn replicate_log() {
        let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060);
        let peers = vec![HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2061)];

        let runtime = Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let replica = Replica::new(
            30,
            self_host_and_port.clone(),
            peers,
            Arc::new(SystemClock::new()),
        );

        let state = runtime.block_on(async move {
            let state = State::new(Arc::new(replica), HeartbeatConfig::default());
            let content = String::from("anything");
            let command = Command { command: content.as_bytes().to_vec() };

            state.append_command(&command);
            return state;
        });

        let inner_state = state.clone();
        let _ = runtime.block_on(async move {
            let raft_service = RaftService::new(inner_state.clone());
            let content = String::from("Content");
            let command = Command { command: content.as_bytes().to_vec() };

            let mut request = Request::new(AppendEntries {
                term: 1,
                leader_id: 30,
                correlation_id: 10,
                entry: Some(Entry {
                    term: 1,
                    index: 1,
                    command: Some(command),
                }),
                previous_log_index: Some(0),
                previous_log_term: Some(0),
            });
            request.add_host_port(self_host_and_port);

            let _ = raft_service.acknowledge_replicate_log(request).await;
        });

        thread::sleep(Duration::from_millis(5));
        assert_eq!(2, state.total_log_entries());
        assert!(state.matches_log_entry_term_at(1, 1));
        assert!(state.matches_log_entry_index_at(1, 1));

        let expected_command = Command { command: String::from("Content").as_bytes().to_vec() };
        assert!(state.matches_log_entry_command_at(1, &expected_command));
    }
}