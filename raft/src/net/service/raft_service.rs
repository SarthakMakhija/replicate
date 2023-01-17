use std::sync::Arc;

use tokio::sync::mpsc;
use tonic::{Request, Response};

use replicate::callback::quorum_completion_response::QuorumCompletionResponse;
use replicate::callback::single_response_completion_callback::SingleResponseCompletionCallback;

use crate::follower_state::FollowerState;
use crate::net::factory::service_request::{BuiltInServiceRequestFactory, ServiceRequestFactory};
use crate::net::rpc::grpc::{AppendEntries, AppendEntriesResponse, Command, RequestVote, RequestVoteResponse};
use crate::net::rpc::grpc::raft_server::Raft;
use crate::state::{ReplicaRole, State};

pub struct RaftService {
    state: Arc<State>,
    service_request_factory: Arc<dyn ServiceRequestFactory>,
    follower_state: Arc<FollowerState>,
}

impl RaftService {
    pub fn new(state: Arc<State>) -> Self {
        let inner_state = state.clone();
        let service_request_factory = Arc::new(BuiltInServiceRequestFactory::new());
        let inner_service_request_factory = service_request_factory.clone();

        return RaftService {
            state,
            service_request_factory,
            follower_state: Arc::new(FollowerState::new(inner_state, inner_service_request_factory)),
        };
    }
}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn acknowledge_request_vote(&self, request: Request<RequestVote>) -> Result<Response<RequestVoteResponse>, tonic::Status> {
        let state = self.state.clone();
        let request = request.into_inner();
        let correlation_id = request.correlation_id;
        let replica = self.state.get_replica();

        println!("received RequestVote with term {}", request.term);
        let (sender, mut receiver) = mpsc::channel(1);
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
            let response = RequestVoteResponse { term, voted, correlation_id };
            let _ = sender.send(response).await;
        };
        let _ = replica.add_to_queue(handler).await;
        return match receiver.recv().await {
            Some(request_vote_response) =>
                Ok(Response::new(request_vote_response)),
            None =>
                Err(tonic::Status::unknown("failed receiving RequestVoteResponse from the async handler"))
        };
    }

    async fn acknowledge_heartbeat(&self, request: Request<AppendEntries>) -> Result<Response<AppendEntriesResponse>, tonic::Status> {
        println!("received heartbeat on {:?}", self.state.get_replica_reference().get_self_address());
        let state = self.state.clone();
        let replica = self.state.get_replica_reference();

        let append_entries = request.into_inner();

        let (sender, mut receiver) = mpsc::channel::<AppendEntriesResponse>(1);
        let handler = async move {
            state.mark_heartbeat_received();
            let term = state.get_term();
            if append_entries.term > term {
                state.change_to_follower(append_entries.term);
                let _ = sender.send(AppendEntriesResponse {
                    success: true,
                    term: append_entries.term,
                    correlation_id: append_entries.correlation_id,
                    log_entry_index: None,
                }).await;
            }
            if append_entries.term == term {
                let _ = sender.send(AppendEntriesResponse {
                    success: true,
                    term,
                    correlation_id: append_entries.correlation_id,
                    log_entry_index: None,
                }).await;
            }
            let _ = sender.send(AppendEntriesResponse {
                success: false,
                term,
                correlation_id: append_entries.correlation_id,
                log_entry_index: None,
            }).await;
        };

        let _ = replica.add_to_queue(handler).await;
        return match receiver.recv().await {
            Some(append_entries_response) =>
                Ok(Response::new(append_entries_response)),
            None =>
                Err(tonic::Status::unknown("failed receiving AppendEntriesResponse from the async handler"))
        };
    }

    async fn acknowledge_replicate_log(&self, request: Request<AppendEntries>) -> Result<Response<AppendEntriesResponse>, tonic::Status> {
        println!("received replicate_log on {:?}", self.state.get_replica_reference().get_self_address());

        let state = self.state.clone();
        let replica = self.state.get_replica_reference();
        let append_entries = request.into_inner();

        let (sender, mut receiver) = mpsc::channel(1);
        let handler = async move {
            state.mark_heartbeat_received();

            let term = state.get_term();
            if append_entries.term > term {
                state.clone().change_to_follower(append_entries.term);
            }
            let should_accept = state.should_accept(&append_entries);
            let log_entry_index = if should_accept {
                let replicated_log = state.get_replicated_log_reference();
                let entry = append_entries.entry.unwrap();
                let command = entry.command.unwrap();

                replicated_log.append_command(&command, append_entries.term);
                replicated_log.maybe_advance_commit_index_to(append_entries.leader_commit_index);
                Some(entry.index)
            } else {
                None
            };

            let _ = sender.send(AppendEntriesResponse {
                term,
                success: should_accept,
                log_entry_index,
                correlation_id: append_entries.correlation_id
            }).await;
        };

        let _ = replica.add_to_queue(handler).await;
        return match receiver.recv().await {
            Some(append_entries_response) =>
                Ok(Response::new(append_entries_response)),
            None =>
                Err(tonic::Status::unknown("failed receiving AppendEntriesResponse from the async handler"))
        };
    }

    async fn execute(&self, request: Request<Command>) -> Result<Response<()>, tonic::Status> {
        println!("received command on {:?}", self.state.get_replica_reference().get_self_address());
        let state = self.state.clone();
        let replica = self.state.get_replica_reference();
        let command = request.into_inner();
        let follower_state = self.follower_state.clone();

        let (sender, mut receiver) = mpsc::channel(1);
        let handler = async move {
            let term: u64 = state.get_term();
            let log_entry_index = state.get_replicated_log_reference().append_command(&command, term);
            let _ = follower_state.replicate_log_at(log_entry_index);
            let _ = sender.send(log_entry_index).await;
        };

        let _ = replica.add_to_queue(handler).await;
        let entry_index = receiver.recv().await.unwrap();
        let response_callback = SingleResponseCompletionCallback::<()>::new();

        self.state.get_pending_committed_log_entries_reference().add(entry_index,
                                                                     self.state.get_replica_reference().get_self_address(),
                                                                     response_callback.clone());
        return match response_callback.handle().await {
            QuorumCompletionResponse::Success(_) =>
                Ok(Response::new(())),
            _ =>
                Err(tonic::Status::unknown(format!("failed receiving the response of command execution for raft log entry index {}", entry_index))),
        };
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
                        leader_commit_index: None,
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
                        leader_commit_index: None,
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
                        leader_commit_index: None,
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
                        leader_commit_index: None,
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
        let raft_service = Arc::new(
            RaftService::new(inner_state.clone())
        );
        let inner_raft_service = raft_service.clone();
        let _ = runtime.spawn(async move {
            let content = String::from("Content");
            let command = Command { command: content.as_bytes().to_vec() };

            let mut request = Request::new(command);
            request.add_host_port(self_host_and_port);

            let _ = inner_raft_service.execute(request).await;
        });

        runtime.block_on(async {
            raft_service.state.get_pending_committed_log_entries_reference().handle_response(
                0,
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2060),
                Ok(Box::new(())),
            );
        });

        thread::sleep(Duration::from_millis(5));
        let log_entry = state.get_replicated_log_reference().get_log_entry_at(0).unwrap();

        assert_eq!(0, log_entry.get_term());
        assert_eq!(String::from("Content").as_bytes().to_vec(), log_entry.get_bytes_as_vec());
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
                leader_commit_index: None,
            });
            request.add_host_port(self_host_and_port);

            let _ = raft_service.acknowledge_replicate_log(request).await;
        });

        thread::sleep(Duration::from_millis(5));
        assert_eq!(0, state.get_replicated_log_reference().total_log_entries());
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
                leader_commit_index: None,
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
                leader_commit_index: None,
            });
            request.add_host_port(self_host_and_port);

            let _ = raft_service.acknowledge_replicate_log(request).await;
        });

        thread::sleep(Duration::from_millis(5));
        assert_eq!(0, state.get_replicated_log_reference().total_log_entries());
    }

    #[test]
    fn acknowledge_replicate_log() {
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
            let term = state.get_term();

            state.get_replicated_log_reference().append_command(&command, term);
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
                leader_commit_index: None,
            });
            request.add_host_port(self_host_and_port);

            let _ = raft_service.acknowledge_replicate_log(request).await;
        });

        thread::sleep(Duration::from_millis(20));

        assert_eq!(2, state.get_replicated_log_reference().total_log_entries());
        let log_entry = state.get_replicated_log_reference().get_log_entry_at(1).unwrap();

        assert_eq!(1, log_entry.get_term());
        assert_eq!(1, log_entry.get_index());
        assert_eq!(String::from("Content").as_bytes().to_vec(), log_entry.get_bytes_as_vec());
    }

    #[test]
    fn acknowledge_replicate_log_and_advance_commit_index() {
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
            let term = state.get_term();

            state.get_replicated_log_reference().append_command(&command, term);
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
                leader_commit_index: Some(0),
            });
            request.add_host_port(self_host_and_port);

            let _ = raft_service.acknowledge_replicate_log(request).await;
        });

        thread::sleep(Duration::from_millis(20));

        assert_eq!(2, state.get_replicated_log_reference().total_log_entries());
        let log_entry = state.get_replicated_log_reference().get_log_entry_at(1).unwrap();

        assert_eq!(1, log_entry.get_term());
        assert_eq!(1, log_entry.get_index());
        assert_eq!(String::from("Content").as_bytes().to_vec(), log_entry.get_bytes_as_vec());
        assert_eq!(Some(0), state.get_replicated_log_reference().get_commit_index());
    }
}