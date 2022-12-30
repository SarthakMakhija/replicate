use std::sync::Arc;

use tonic::{Request, Response};

use replicate::net::connect::async_network::AsyncNetwork;
use replicate::net::connect::host_port_extractor::HostAndPortExtractor;

use crate::net::factory::service_request::ServiceRequestFactory;
use crate::net::rpc::grpc::{AppendEntries, AppendEntriesResponse, RequestVote, RequestVoteResponse};
use crate::net::rpc::grpc::raft_server::Raft;
use crate::state::State;

pub struct RaftService {
    state: Arc<State>,
}

impl RaftService {
    pub fn new(state: Arc<State>) -> Self {
        return RaftService { state };
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

        println!("received RequestVote with term {}", request.term);
        let handler = async move {
            let term = state.get_term();
            let voted: bool = if request.term > term {
                true
            } else {
                false
            };
            let send_result = AsyncNetwork::send_with_source_footprint(
                ServiceRequestFactory::request_vote_response(term, voted, correlation_id),
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

        let request = request.into_inner();
        let handler = async move {
            state.mark_heartbeat_received();
            let term = state.get_term();
            if request.term > term {
                state.change_to_follower(request.term);
                return AppendEntriesResponse { success: true, term: request.term };
            }
            if request.term == term {
                return AppendEntriesResponse { success: true, term };
            }
            return AppendEntriesResponse { success: false, term };
        };

        return match replica.add_to_queue(handler).await {
            Ok(append_entries_response) =>
                Ok(Response::new(append_entries_response)),
            Err(err) =>
                Err(tonic::Status::unknown(err.to_string()))
        };
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;
    use tonic::{Request, Response};
    use replicate::clock::clock::SystemClock;
    use replicate::net::connect::host_and_port::HostAndPort;
    use replicate::net::replica::Replica;
    use crate::net::service::raft_service::RaftService;
    use crate::state::State;
    use crate::net::rpc::grpc::raft_server::Raft;
    use crate::net::rpc::grpc::{AppendEntries, AppendEntriesResponse};
    use tokio::runtime::Builder;

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
        let replica = Arc::new(replica);
        let state = State::new(replica.clone(), Arc::new(SystemClock::new()));
        let raft_service = RaftService::new(state.clone());

        let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();

        let _ = blocking_runtime.block_on(async move {
            let result: Result<Response<AppendEntriesResponse>, tonic::Status> = raft_service.acknowledge_heartbeat(
                Request::new(
                    AppendEntries {
                        term: 1,
                        leader_id: replica.get_id(),
                        correlation_id: 20
                    }
                )
            ).await;
            return result.unwrap().into_inner();
        });

        assert!(state.get_heartbeat_received_time().is_some());
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
        let replica = Arc::new(replica);
        let state = State::new(replica.clone(), Arc::new(SystemClock::new()));
        let raft_service = RaftService::new(state.clone());

        let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();

        let response = blocking_runtime.block_on(async move {
            let result: Result<Response<AppendEntriesResponse>, tonic::Status> = raft_service.acknowledge_heartbeat(
                Request::new(
                    AppendEntries {
                        term: 1,
                        leader_id: replica.get_id(),
                        correlation_id: 20
                    }
                )
            ).await;
            return result.unwrap().into_inner();
        });

        assert_eq!(true, response.success);
        assert_eq!(1, response.term);
        assert_eq!(1, state.get_term());
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
        let replica = Arc::new(replica);
        let state = State::new(replica.clone(), Arc::new(SystemClock::new()));
        let raft_service = RaftService::new(state.clone());

        let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();

        let response = blocking_runtime.block_on(async move {
            let result: Result<Response<AppendEntriesResponse>, tonic::Status> = raft_service.acknowledge_heartbeat(
                Request::new(
                    AppendEntries {
                        term: 0,
                        leader_id: replica.get_id(),
                        correlation_id: 20
                    }
                )
            ).await;
            return result.unwrap().into_inner();
        });

        assert_eq!(true, response.success);
        assert_eq!(0, response.term);
        assert_eq!(0, state.get_term());
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
        let replica = Arc::new(replica);
        let state = State::new(replica.clone(), Arc::new(SystemClock::new()));
        state.change_to_candidate();

        let raft_service = RaftService::new(state.clone());
        let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();

        let response = blocking_runtime.block_on(async move {
            let result: Result<Response<AppendEntriesResponse>, tonic::Status> = raft_service.acknowledge_heartbeat(
                Request::new(
                    AppendEntries {
                        term: 0,
                        leader_id: replica.get_id(),
                        correlation_id: 20
                    }
                )
            ).await;
            return result.unwrap().into_inner();
        });

        assert_eq!(false, response.success);
        assert_eq!(1, response.term);
        assert_eq!(1, state.get_term());
    }
}