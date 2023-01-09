use std::sync::Arc;

use replicate::callback::async_quorum_callback::AsyncQuorumCallback;
use replicate::net::connect::correlation_id::RESERVED_CORRELATION_ID;
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

    pub fn start(&self) {
        let replica = self.state.get_replica();
        let inner_replica = replica.clone();
        let state = self.state.clone();
        let service_request_factory = self.service_request_factory.clone();

        replica.submit_to_queue(async move {
            let term = state.change_to_candidate();
            println!("starting election with term {}", term);

            let service_request_constructor = || {
                service_request_factory.request_vote(
                    inner_replica.get_id(),
                    term,
                )
            };
            let success_condition = Box::new(|response: &RequestVoteResponse| response.voted);
            let expected_responses = inner_replica.cluster_size();
            let async_quorum_callback = AsyncQuorumCallback::<RequestVoteResponse>::new_with_success_condition(
                inner_replica.cluster_size(),
                expected_responses,
                success_condition,
            );
            let _ = inner_replica.send_to_replicas(
                service_request_constructor,
                async_quorum_callback.clone(),
            ).await;

            async_quorum_callback.on_response(inner_replica.get_self_address(), Ok(Box::new(RequestVoteResponse {
                term,
                voted: true,
                correlation_id: RESERVED_CORRELATION_ID,
            })));

            let quorum_completion_response = async_quorum_callback.handle().await;
            if quorum_completion_response.is_success() {
                state.change_to_leader();
            } else {
                state.change_to_follower(term); //TODO: Change the term to the highest term received
            }
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
    use replicate::net::request_waiting_list::request_waiting_list_config::RequestWaitingListConfig;

    use crate::election::election::Election;
    use crate::election::election::tests::setup::IncrementingCorrelationIdServiceRequestFactory;
    use crate::heartbeat_config::HeartbeatConfig;
    use crate::net::rpc::grpc::RequestVoteResponse;
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

        use crate::net::factory::service_request::ServiceRequestFactory;
        use crate::net::rpc::grpc::RequestVote;

        pub(crate) struct IncrementingCorrelationIdServiceRequestFactory {
            pub(crate) base_correlation_id: RwLock<AtomicU64>,
        }

        impl ServiceRequestFactory for IncrementingCorrelationIdServiceRequestFactory {
            fn request_vote(&self, replica_id: ReplicaId, term: u64) -> ServiceRequest<RequestVote, ()> {
                {
                    let write_guard = self.base_correlation_id.write().unwrap();
                    write_guard.fetch_add(1, Ordering::SeqCst);
                }

                let guard = self.base_correlation_id.read().unwrap();
                let correlation_id: CorrelationId = guard.load(Ordering::SeqCst);

                return ServiceRequest::new(
                    RequestVote {
                        replica_id,
                        term,
                        correlation_id,
                    },
                    Box::new(TestRequestVoteClient {}),
                    correlation_id,
                );
            }
        }

        struct TestRequestVoteClient {}

        #[async_trait]
        impl ServiceClientProvider<RequestVote, ()> for TestRequestVoteClient {
            async fn call(&self, _: Request<RequestVote>, _: HostAndPort) -> Result<Response<()>, ServiceResponseError> {
                return Ok(
                    Response::new(())
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
            }),
        );
        election.start();

        let response = RequestVoteResponse {
            term: 1,
            voted: true,
            correlation_id: 1,
        };

        thread::sleep(Duration::from_millis(20));
        some_replica.register_response(1, peer_host, Ok(Box::new(response)));

        let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        blocking_runtime.block_on(async move {
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
            }),
        );
        election.start();

        let response_with_higher_term_one = RequestVoteResponse {
            term: 2,
            voted: false,
            correlation_id: 1,
        };
        let response_with_higher_term_two = RequestVoteResponse {
            term: 2,
            voted: false,
            correlation_id: 2,
        };

        thread::sleep(Duration::from_millis(20));
        some_replica.register_response(1, peer_host, Ok(Box::new(response_with_higher_term_one)));
        some_replica.register_response(2, peer_other_host, Ok(Box::new(response_with_higher_term_two)));

        let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        blocking_runtime.block_on(async move {
            thread::sleep(Duration::from_millis(100));

            assert_eq!(ReplicaRole::Follower, state.get_role());
        });
    }

    #[test]
    fn lose_the_election_with_request_timeout() {
        let self_host = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1971);
        let peer_host = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1297);
        let peer_other_host = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1298);

        let some_replica = Replica::new_with_waiting_list_config(
            10,
            self_host,
            vec![peer_host, peer_other_host],
            Arc::new(SystemClock::new()),
            RequestWaitingListConfig::new(
                Duration::from_millis(50),
                Duration::from_millis(30),
            ),
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
            }),
        );
        election.start();

        thread::sleep(Duration::from_millis(20));

        let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        blocking_runtime.block_on(async move {
            thread::sleep(Duration::from_millis(100));

            assert_eq!(ReplicaRole::Follower, state.get_role());
        });
    }
}