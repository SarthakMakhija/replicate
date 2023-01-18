use std::future::Future;
use std::sync::Arc;

use tokio::sync::mpsc;

use replicate::callback::async_quorum_callback::AsyncQuorumCallback;
use replicate::net::connect::correlation_id::RESERVED_CORRELATION_ID;
use replicate::net::connect::error::ServiceResponseError;
use replicate::net::connect::host_and_port::HostAndPort;
use replicate::net::replica::Replica;
use replicate::net::request_waiting_list::response_callback::ResponseCallback;

use crate::net::factory::service_request::{BuiltInServiceRequestFactory, ServiceRequestFactory};
use crate::net::rpc::grpc::RequestVoteResponse;
use crate::state::State;

pub struct Election {
    state: Arc<State>,
    service_request_factory: Arc<dyn ServiceRequestFactory>,
}

impl Election {
    pub fn new(state: Arc<State>) -> Self {
        return Election { state, service_request_factory: Arc::new(BuiltInServiceRequestFactory::new()) };
    }

    fn new_with(state: Arc<State>, service_request_factory: Arc<dyn ServiceRequestFactory>) -> Self {
        return Election { state, service_request_factory };
    }

    pub async fn start(&self) {
        let replica = self.state.get_replica();
        let inner_replica = replica.clone();
        let (inner_state, response_state) = (self.state.clone(), self.state.clone());
        let service_request_factory = self.service_request_factory.clone();

        let async_quorum_callback = AsyncQuorumCallback::<RequestVoteResponse>::new_with_success_condition(
            inner_replica.cluster_size(),
            replica.cluster_size(),
            Box::new(|response: &RequestVoteResponse| response.voted),
        );

        let inner_async_quorum_callback = async_quorum_callback.clone();

        let (sender, mut receiver) = mpsc::channel(1);
        let request_vote_handler = async move {
            let term = inner_state.change_to_candidate();
            println!("starting election with term {}", term);

            let replica_id = inner_replica.get_id();
            let replica = inner_replica.clone();
            let (last_log_index, last_log_term): (Option<u64>, Option<u64>) = inner_state.get_replicated_log_reference().get_last_log_index_and_term();
            replica.send_to_replicas_with_handler_hook(
                || { service_request_factory.request_vote(replica_id, term, last_log_index, last_log_term) },
                Arc::new(move |peer, response: Result<RequestVoteResponse, ServiceResponseError>| {
                    return Self::request_vote_response_handler(inner_replica.clone(), peer, response);
                }),
                || Some(inner_async_quorum_callback.clone() as Arc<dyn ResponseCallback>),
            );

            inner_async_quorum_callback.on_response(replica.get_self_address(), Ok(Box::new(RequestVoteResponse {
                term,
                voted: true,
                correlation_id: RESERVED_CORRELATION_ID,
            })));
            let _ = sender.send(term).await;
        };

        let _ = replica.add_to_queue(request_vote_handler).await;
        let quorum_completion_response = async_quorum_callback.handle().await;
        let election_term = receiver.recv().await.unwrap();

        let _ = replica.add_to_queue(async move {
            if quorum_completion_response.is_success() {
                response_state.change_to_leader();
            } else {
                response_state.change_to_follower(election_term);
            }
        }).await;
    }

    fn request_vote_response_handler(
        replica: Arc<Replica>,
        peer: HostAndPort,
        response: Result<RequestVoteResponse, ServiceResponseError>,
    ) -> Option<impl Future<Output=()>> {
        return Some(
            async move {
                match response {
                    Ok(request_vote_response) => {
                        println!("received RequestVoteResponse with voted? {}", request_vote_response.voted);
                        let _ = replica.register_response(request_vote_response.correlation_id, peer, Ok(Box::new(request_vote_response)));
                    }
                    Err(_err) => {
                        eprintln!("received RequestVoteResponse with an error from the host {:?}", peer);
                    }
                }
            }
        );
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

        use replicate::net::connect::correlation_id::CorrelationId;
        use replicate::net::connect::error::ServiceResponseError;
        use replicate::net::connect::host_and_port::HostAndPort;
        use replicate::net::connect::service_client::{ServiceClientProvider, ServiceRequest};
        use replicate::net::replica::ReplicaId;

        use crate::election::election::tests::setup::ClientType::Success;
        use crate::net::factory::service_request::ServiceRequestFactory;
        use crate::net::rpc::grpc::RequestVote;
        use crate::net::rpc::grpc::RequestVoteResponse;

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
                            last_log_term: Option<u64>
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
                    RequestVote {
                        replica_id,
                        term,
                        correlation_id,
                        last_log_index,
                        last_log_term
                    },
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
            async fn call(&self, _: Request<RequestVote>, _: HostAndPort) -> Result<Response<RequestVoteResponse>, ServiceResponseError> {
                return Ok(
                    Response::new(RequestVoteResponse {
                        voted: true,
                        term: 1,
                        correlation_id: self.correlation_id,
                    })
                );
            }
        }

        #[async_trait]
        impl ServiceClientProvider<RequestVote, RequestVoteResponse> for NotVotedRequestVoteClient {
            async fn call(&self, _: Request<RequestVote>, _: HostAndPort) -> Result<Response<RequestVoteResponse>, ServiceResponseError> {
                return Ok(
                    Response::new(RequestVoteResponse {
                        voted: false,
                        term: 1,
                        correlation_id: self.correlation_id,
                    })
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
            Arc::new(SystemClock::new()),
        );

        let some_replica = Arc::new(some_replica);
        let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();

        let inner_replica = some_replica.clone();
        let state = blocking_runtime.block_on(async move {
            return State::new(inner_replica, HeartbeatConfig::default());
        });

        let election = Election::new_with(
            state.clone(),
            Arc::new(IncrementingCorrelationIdServiceRequestFactory {
                base_correlation_id: RwLock::new(AtomicU64::new(0)),
                client_type: Success,
            }),
        );

        let election_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let handle = election_runtime.spawn(async move {
            election.start().await;
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
            Arc::new(SystemClock::new()),
        );

        let some_replica = Arc::new(some_replica);
        let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();

        let inner_replica = some_replica.clone();
        let state = blocking_runtime.block_on(async move {
            return State::new(inner_replica, HeartbeatConfig::default());
        });

        let election = Election::new_with(
            state.clone(),
            Arc::new(IncrementingCorrelationIdServiceRequestFactory {
                base_correlation_id: RwLock::new(AtomicU64::new(0)),
                client_type: Failure,
            }),
        );

        let election_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let handle = election_runtime.spawn(async move {
            election.start().await;
        });

        let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        blocking_runtime.block_on(async move {
            let _ = handle.await;
            thread::sleep(Duration::from_millis(10));

            assert_eq!(ReplicaRole::Follower, state.get_role());
        });
    }
}