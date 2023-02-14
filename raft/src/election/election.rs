use std::future::Future;
use std::sync::Arc;

use tokio::sync::mpsc;

use replicate::callback::async_quorum_callback::AsyncQuorumCallback;
use replicate::net::connect::correlation_id::RESERVED_CORRELATION_ID;
use replicate::net::connect::error::ServiceResponseError;
use replicate::net::connect::host_and_port::HostAndPort;
use replicate::net::request_waiting_list::response_callback::ResponseCallback;

use crate::net::builder::request_vote::RequestVoteResponseBuilder;
use crate::net::rpc::grpc::RequestVoteResponse;
use crate::state::State;

pub struct Election {}

impl Election {
    pub fn new() -> Self {
        return Election {};
    }

    pub async fn start(&self, state: Arc<State>) {
        let replica = state.get_replica_reference();
        let (inner_state, response_state) = (state.clone(), state.clone());

        let async_quorum_callback = AsyncQuorumCallback::<RequestVoteResponse>::new_with_success_condition(
            replica.cluster_size(),
            replica.cluster_size(),
            Box::new(|response: &RequestVoteResponse| response.voted),
        );

        let inner_async_quorum_callback = async_quorum_callback.clone();
        let (sender, mut receiver) = mpsc::channel(1);

        let request_vote_handler = async move {
            let term = inner_state.change_to_candidate();
            println!("starting election with term {} on {:?}", term, inner_state.get_replica_reference().get_self_address());
            let state = inner_state.clone();
            let service_request_factory = state.get_service_request_factory_reference();
            let replica_reference = state.get_replica_reference();
            let (last_log_index, last_log_term) = inner_state.get_replicated_log_reference().get_last_log_index_and_term();

            replica_reference.non_pipeline_mode().send_to_replicas_with_handler_hook(
                || { service_request_factory.request_vote(replica_reference.get_id(), term, last_log_index, last_log_term) },
                Arc::new(move |peer, response: Result<RequestVoteResponse, ServiceResponseError>| {
                    return Self::request_vote_response_handler(inner_state.clone(), peer, response);
                }),
                || Some(inner_async_quorum_callback.clone() as Arc<dyn ResponseCallback>),
            );

            inner_async_quorum_callback.on_response(replica_reference.get_self_address(), Ok(Box::new(
                RequestVoteResponseBuilder::voted_response(term, RESERVED_CORRELATION_ID)
            )));
            let _ = sender.send(term).await;
        };

        let _ = replica.add_to_queue(request_vote_handler).await;
        let quorum_completion_response = async_quorum_callback.handle().await;
        let election_term = receiver.recv().await.unwrap();

        let _ = replica.add_to_queue(async move {
            if quorum_completion_response.is_success() {
                println!("{:?} becoming a leader", response_state.get_replica_reference().get_self_address());
                response_state.change_to_leader();
            } else {
                response_state.change_to_follower(election_term);
            }
        }).await;
    }

    fn request_vote_response_handler(
        state: Arc<State>,
        peer: HostAndPort,
        response: Result<RequestVoteResponse, ServiceResponseError>,
    ) -> Option<impl Future<Output=()>> {
        return Some(
            async move {
                match response {
                    Ok(request_vote_response) => {
                        println!("received RequestVoteResponse with voted? {}", request_vote_response.voted);
                        let _ = state
                            .get_replica_reference()
                            .register_response(request_vote_response.correlation_id, peer, Ok(Box::new(request_vote_response)));
                    }
                    Err(_err) => {
                        eprintln!("received RequestVoteResponse with an error from the host {:?}", peer);
                    }
                }
            }
        );
    }
}

#[cfg(all(test, feature="test_type_unit"))]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::atomic::AtomicU64;
    use std::sync::RwLock;
    use std::thread;
    use std::time::Duration;

    use tokio::runtime::Builder;

    use replicate::clock::clock::SystemClock;
    use replicate::net::connect::host_and_port::HostAndPort;
    use replicate::net::replica::Replica;

    use crate::election::election::Election;
    use crate::election::election::tests::setup::ClientType::{Failure, Success};
    use crate::election::election::tests::setup::IncrementingCorrelationIdServiceRequestFactory;
    use crate::heartbeat_config::HeartbeatConfig;
    use crate::state::{ReplicaRole, State};

    mod setup {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::RwLock;

        use async_trait::async_trait;
        use tonic::{Request, Response};
        use tonic::transport::Channel;

        use replicate::net::connect::correlation_id::CorrelationId;
        use replicate::net::connect::error::ServiceResponseError;
        use replicate::net::connect::host_and_port::HostAndPort;
        use replicate::net::connect::service_client::{ServiceClientProvider, ServiceRequest};
        use replicate::net::replica::ReplicaId;

        use crate::election::election::tests::setup::ClientType::Success;
        use crate::net::builder::request_vote::{RequestVoteBuilder, RequestVoteResponseBuilder};
        use crate::net::factory::service_request::ServiceRequestFactory;
        use crate::net::rpc::grpc::{RequestVote, RequestVoteResponse};

        #[derive(PartialEq)]
        pub(crate) enum ClientType {
            Success,
            Failure,
        }

        pub(crate) struct IncrementingCorrelationIdServiceRequestFactory {
            pub(crate) base_correlation_id: RwLock<AtomicU64>,
            pub(crate) client_type: ClientType,
        }

        impl ServiceRequestFactory for IncrementingCorrelationIdServiceRequestFactory {
            fn request_vote(&self,
                            replica_id: ReplicaId,
                            term: u64,
                            last_log_index: Option<u64>,
                            last_log_term: Option<u64>,
            ) -> ServiceRequest<RequestVote, RequestVoteResponse> {
                {
                    let write_guard = self.base_correlation_id.write().unwrap();
                    write_guard.fetch_add(1, Ordering::SeqCst);
                }

                let guard = self.base_correlation_id.read().unwrap();
                let correlation_id: CorrelationId = guard.load(Ordering::SeqCst);
                let client: Box<dyn ServiceClientProvider<RequestVote, RequestVoteResponse>> = if self.client_type == Success {
                    Box::new(VotedRequestVoteClient { correlation_id })
                } else {
                    Box::new(NotVotedRequestVoteClient { correlation_id })
                };

                return ServiceRequest::new(
                    RequestVoteBuilder::request_vote_with_log(
                        replica_id,
                        term,
                        correlation_id,
                        last_log_index,
                        last_log_term,
                    ),
                    client,
                    correlation_id,
                );
            }
        }

        struct VotedRequestVoteClient {
            correlation_id: CorrelationId,
        }

        struct NotVotedRequestVoteClient {
            correlation_id: CorrelationId,
        }

        #[async_trait]
        impl ServiceClientProvider<RequestVote, RequestVoteResponse> for VotedRequestVoteClient {
            async fn call(&self, _: Request<RequestVote>, _: HostAndPort, _channel: Option<Channel>) -> Result<Response<RequestVoteResponse>, ServiceResponseError> {
                return Ok(
                    Response::new(
                        RequestVoteResponseBuilder::voted_response(1, self.correlation_id)
                    )
                );
            }
        }

        #[async_trait]
        impl ServiceClientProvider<RequestVote, RequestVoteResponse> for NotVotedRequestVoteClient {
            async fn call(&self, _: Request<RequestVote>, _: HostAndPort, _channel: Option<Channel>) -> Result<Response<RequestVoteResponse>, ServiceResponseError> {
                return Ok(
                    Response::new(
                        RequestVoteResponseBuilder::not_voted_response(1, self.correlation_id)
                    )
                );
            }
        }
    }

    #[test]
    fn win_the_election() {
        let self_host = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971);
        let peer_host = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297);
        let peer_other_host = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1298);

        let some_replica = Replica::new(
            10,
            self_host,
            vec![peer_host, peer_other_host],
            Box::new(SystemClock::new()),
        );

        let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let state = blocking_runtime.block_on(async move {
            return State::new_with(some_replica,
                                   HeartbeatConfig::default(),
                                   Box::new(
                                       IncrementingCorrelationIdServiceRequestFactory {
                                           base_correlation_id: RwLock::new(AtomicU64::new(0)),
                                           client_type: Success,
                                       }
                                   ),
            );
        });

        let election = Election::new();

        let election_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let inner_state = state.clone();
        let handle = election_runtime.spawn(async move {
            election.start(inner_state).await;
        });

        let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        blocking_runtime.block_on(async move {
            let _ = handle.await;
            thread::sleep(Duration::from_millis(10));

            assert_eq!(ReplicaRole::Leader, state.get_role());
            assert_eq!(1, state.get_term());
        });
    }

    #[test]
    fn lose_the_election() {
        let self_host = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971);
        let peer_host = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297);
        let peer_other_host = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1298);

        let some_replica = Replica::new(
            10,
            self_host,
            vec![peer_host, peer_other_host],
            Box::new(SystemClock::new()),
        );

        let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let state = blocking_runtime.block_on(async move {
            return State::new_with(some_replica, HeartbeatConfig::default(),
                                   Box::new(
                                       IncrementingCorrelationIdServiceRequestFactory {
                                           base_correlation_id: RwLock::new(AtomicU64::new(0)),
                                           client_type: Failure,
                                       }
                                   ),
            );
        });

        let election = Election::new();

        let election_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let inner_state = state.clone();
        let handle = election_runtime.spawn(async move {
            election.start(inner_state).await;
        });

        let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        blocking_runtime.block_on(async move {
            let _ = handle.await;
            thread::sleep(Duration::from_millis(10));

            assert_eq!(ReplicaRole::Follower, state.get_role());
        });
    }
}