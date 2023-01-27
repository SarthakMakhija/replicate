use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;

use tokio::task::JoinHandle;

use crate::clock::clock::Clock;
use crate::net::connect::async_network::AsyncNetwork;
use crate::net::connect::correlation_id::CorrelationId;
use crate::net::connect::error::ServiceResponseError;
use crate::net::connect::host_and_port::HostAndPort;
use crate::net::connect::service_client::ServiceRequest;
use crate::net::peers::Peers;
use crate::net::request_waiting_list::request_waiting_list::RequestWaitingList;
use crate::net::request_waiting_list::request_waiting_list_config::RequestWaitingListConfig;
use crate::net::request_waiting_list::response_callback::{AnyResponse, ResponseCallbackType, ResponseErrorType};
use crate::singular_update_queue::singular_update_queue::SingularUpdateQueue;

pub type TotalFailedSends = usize;

pub type ReplicaId = u64;

pub struct Replica {
    id: ReplicaId,
    self_address: HostAndPort,
    peers: Peers,
    request_waiting_list: RequestWaitingList,
    singular_update_queue: Arc<SingularUpdateQueue>,
}

impl Replica {
    pub fn new(id: ReplicaId,
               self_address: HostAndPort,
               peer_addresses: Vec<HostAndPort>,
               clock: Box<dyn Clock>) -> Self {
        return Self::new_with_waiting_list_config(
            id,
            self_address,
            peer_addresses,
            clock,
            RequestWaitingListConfig::default(),
        );
    }

    pub fn new_with_waiting_list_config(id: ReplicaId,
                                        self_address: HostAndPort,
                                        peer_addresses: Vec<HostAndPort>,
                                        clock: Box<dyn Clock>,
                                        request_waiting_list_config: RequestWaitingListConfig) -> Self {
        let request_waiting_list = RequestWaitingList::new(
            clock.clone(),
            request_waiting_list_config,
        );

        return Replica {
            id,
            self_address,
            peers: Peers::new(peer_addresses),
            request_waiting_list,
            singular_update_queue: Arc::new(SingularUpdateQueue::new()),
        };
    }

    pub async fn send_to_replicas<Payload, S, Response>(&self,
                                                        service_request_constructor: S,
                                                        response_callback: ResponseCallbackType) -> TotalFailedSends
        where Payload: Send + 'static,
              Response: Send + Debug + 'static,
              S: Fn() -> ServiceRequest<Payload, Response> {
        return self.send_to(&self.peers, service_request_constructor, response_callback).await;
    }

    pub async fn send_to<Payload, S, Response>(&self,
                                               peers: &Peers,
                                               service_request_constructor: S,
                                               response_callback: ResponseCallbackType) -> TotalFailedSends
        where Payload: Send + 'static,
              Response: Send + Debug + 'static,
              S: Fn() -> ServiceRequest<Payload, Response> {
        let mut send_task_handles = Vec::new();
        for address in peers.get_peer_addresses() {
            if address.eq(&self.self_address) {
                continue;
            }

            let service_request: ServiceRequest<Payload, Response> = service_request_constructor();
            send_task_handles.push(self.send(
                &self.request_waiting_list,
                service_request,
                address.clone(),
                response_callback.clone(),
            ));
        }

        let mut total_failed_sends: TotalFailedSends = 0;
        for task_handle in send_task_handles {
            let (result, correlation_id, target_address) = task_handle.await.unwrap();
            if result.is_err() {
                let _ = &self.request_waiting_list.handle_response(correlation_id, target_address, Err(result.unwrap_err()));
                total_failed_sends = total_failed_sends + 1;
            }
        }
        return total_failed_sends;
    }

    pub fn send_to_replicas_with_handler_hook<Payload, S, Response, F, T, U>(&self,
                                                                             service_request_constructor: S,
                                                                             response_handler_generator: Arc<F>,
                                                                             response_callback_generator: U)
        where Payload: Send + 'static,
              Response: Send + Debug + 'static,
              S: Fn() -> ServiceRequest<Payload, Response>,
              F: Fn(HostAndPort, Result<Response, ServiceResponseError>) -> Option<T> + Send + Sync + 'static,
              T: Future<Output=()> + Send + 'static,
              U: Fn() -> Option<ResponseCallbackType> {
        self.send_to_with_handler_hook(
            &self.peers.get_peer_addresses(),
            service_request_constructor,
            response_handler_generator,
            response_callback_generator,
        );
    }

    pub fn send_to_with_handler_hook<Payload, S, Response, F, T, U>(&self,
                                                                    hosts: &Vec<HostAndPort>,
                                                                    service_request_constructor: S,
                                                                    response_handler_generator: Arc<F>,
                                                                    response_callback_generator: U)
        where Payload: Send + 'static,
              Response: Send + Debug + 'static,
              S: Fn() -> ServiceRequest<Payload, Response>,
              F: Fn(HostAndPort, Result<Response, ServiceResponseError>) -> Option<T> + Send + Sync + 'static,
              T: Future<Output=()> + Send + 'static,
              U: Fn() -> Option<ResponseCallbackType> {
        for peer_address in hosts {
            if peer_address.eq(&self.self_address) {
                continue;
            }

            let peer_address = peer_address.clone();
            let source_address = self.self_address.clone();
            let singular_update_queue = self.singular_update_queue.clone();
            let service_request: ServiceRequest<Payload, Response> = service_request_constructor();
            let peer_handler_generator = response_handler_generator.clone();

            if let Some(response_callback) = response_callback_generator() {
                let correlation_id = service_request.correlation_id;
                self.request_waiting_list.add(correlation_id, peer_address.clone(), response_callback);
            }
            tokio::spawn(async move {
                let response = AsyncNetwork::send_with_source_footprint(
                    service_request,
                    source_address,
                    peer_address,
                ).await;

                if let Some(handler) = peer_handler_generator(peer_address, response) {
                    let _ = singular_update_queue.add(handler).await;
                }
            });
        }
    }

    pub async fn add_to_queue<F>(&self, handler: F)
        where
            F: Future<Output=()> + Send + 'static {
        let singular_update_queue = &self.singular_update_queue;
        let _ = singular_update_queue.add(handler).await;
    }

    pub fn register_response(&self, correlation_id: CorrelationId, from: HostAndPort, response: Result<AnyResponse, ResponseErrorType>) {
        let _ = &self.request_waiting_list.handle_response(correlation_id, from, response);
    }

    pub fn cluster_size(&self) -> usize {
        return self.total_peer_count() + 1;
    }

    pub fn total_peer_count(&self) -> usize {
        let self_address = self.self_address;
        return self.peers.get_peer_addresses().iter().filter(|peer_address| peer_address.ne(&&self_address)).count();
    }

    pub fn get_self_address(&self) -> HostAndPort {
        return self.self_address.clone();
    }

    pub fn get_peers(&self) -> Vec<HostAndPort> {
        let self_address = self.self_address;
        return self.peers.get_peer_addresses()
            .iter()
            .filter(|peer_address| peer_address.ne(&&self_address))
            .map(|peer_address| peer_address.clone())
            .collect();
    }

    pub fn get_id(&self) -> ReplicaId {
        return self.id;
    }

    pub fn get_clock(&self) -> Box<dyn Clock> {
        return self.request_waiting_list.get_clock().clone();
    }

    fn send<Payload, Response>(&self,
                               request_waiting_list: &RequestWaitingList,
                               service_request: ServiceRequest<Payload, Response>,
                               target_address: HostAndPort,
                               response_callback: ResponseCallbackType) -> JoinHandle<(Result<Response, ServiceResponseError>, CorrelationId, HostAndPort)>
        where Payload: Send + 'static,
              Response: Send + Debug + 'static {
        let correlation_id = service_request.correlation_id;
        request_waiting_list.add(correlation_id, target_address.clone(), response_callback);

        let source_address = self.self_address.clone();
        return tokio::spawn(async move {
            let result = AsyncNetwork::send_with_source_footprint(service_request, source_address, target_address.clone()).await;
            return (result, correlation_id, target_address);
        });
    }
}


#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::future::Future;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::{Arc, RwLock};
    use std::sync::atomic::{AtomicI8, Ordering};

    use tokio::runtime::Builder;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Sender;

    use crate::callback::async_quorum_callback::AsyncQuorumCallback;
    use crate::clock::clock::SystemClock;
    use crate::net::connect::correlation_id::CorrelationIdGenerator;
    use crate::net::connect::error::ServiceResponseError;
    use crate::net::connect::host_and_port::HostAndPort;
    use crate::net::connect::random_correlation_id_generator::RandomCorrelationIdGenerator;
    use crate::net::connect::service_client::ServiceRequest;
    use crate::net::peers::Peers;
    use crate::net::replica::Replica;
    use crate::net::replica::tests::setup::{FixedCorrelationIdGenerator, GetValueRequest, GetValueRequestFailureClient, GetValueRequestSuccessClient, GetValueResponse, ResponseCounter};

    mod setup {
        use std::error::Error;
        use std::fmt::{Display, Formatter};
        use std::sync::atomic::AtomicI8;

        use async_trait::async_trait;
        use tonic::{Request, Response};

        use crate::net::connect::correlation_id::{CorrelationId, CorrelationIdGenerator};
        use crate::net::connect::error::ServiceResponseError;
        use crate::net::connect::host_and_port::HostAndPort;
        use crate::net::connect::service_client::ServiceClientProvider;

        #[derive(Debug)]
        pub struct GetValueRequest {}

        #[derive(Debug)]
        pub struct GetValueResponse {
            pub value: String,
        }

        pub struct GetValueRequestSuccessClient {}

        pub struct GetValueRequestFailureClient {}

        #[derive(Debug)]
        pub struct TestError {
            pub message: String,
        }

        #[async_trait]
        impl ServiceClientProvider<GetValueRequest, ()> for GetValueRequestSuccessClient {
            async fn call(&self, _: Request<GetValueRequest>, _: HostAndPort) -> Result<Response<()>, ServiceResponseError> {
                return Ok(Response::new(()));
            }
        }

        #[async_trait]
        impl ServiceClientProvider<GetValueRequest, ()> for GetValueRequestFailureClient {
            async fn call(&self, _: Request<GetValueRequest>, _: HostAndPort) -> Result<Response<()>, ServiceResponseError> {
                return Err(Box::new(TestError { message: "Test error".to_string() }));
            }
        }

        impl Display for TestError {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "{}", self.message)
            }
        }

        impl Error for TestError {}

        pub struct FixedCorrelationIdGenerator {
            value: CorrelationId,
        }

        impl FixedCorrelationIdGenerator {
            pub fn new(fixed_value: CorrelationId) -> FixedCorrelationIdGenerator {
                return FixedCorrelationIdGenerator { value: fixed_value };
            }
        }

        impl CorrelationIdGenerator for FixedCorrelationIdGenerator {
            fn generate(&self) -> CorrelationId {
                return self.value;
            }
        }

        pub struct ResponseCounter {
            pub counter: AtomicI8,
        }
    }

    #[test]
    fn send_one_way_to_replicas_successfully() {
        let any_replica_port = 8988;
        let any_other_replica_port = 8989;

        let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let replica = blocking_runtime.block_on(async {
            return Replica::new(
                10,
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080),
                vec![
                    HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), any_replica_port),
                    HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), any_other_replica_port),
                ],
                Box::new(SystemClock::new()),
            );
        });

        let correlation_id_generator = RandomCorrelationIdGenerator::new();
        let async_quorum_callback = AsyncQuorumCallback::<()>::new(3, 2);
        let service_request_constructor = || {
            ServiceRequest::new(
                GetValueRequest {},
                Box::new(GetValueRequestSuccessClient {}),
                correlation_id_generator.generate(),
            )
        };

        blocking_runtime.block_on(async {
            let total_failed_sends =
                replica.send_to_replicas(service_request_constructor, async_quorum_callback.clone()).await;

            assert_eq!(0, total_failed_sends);
        })
    }

    #[test]
    fn send_one_way_to_the_hosts_successfully() {
        let any_replica_port = 9988;
        let any_other_replica_port = 9989;

        let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let replica = blocking_runtime.block_on(async {
            return Replica::new(
                10,
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2080),
                vec![
                    HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), any_other_replica_port),
                ],
                Box::new(SystemClock::new()),
            );
        });

        let correlation_id_generator = RandomCorrelationIdGenerator::new();
        let async_quorum_callback = AsyncQuorumCallback::<()>::new(2, 1);
        let service_request_constructor = || {
            ServiceRequest::new(
                GetValueRequest {},
                Box::new(GetValueRequestSuccessClient {}),
                correlation_id_generator.generate(),
            )
        };

        blocking_runtime.block_on(async {
            let peers = Peers::new(vec![HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), any_replica_port)]);
            let total_failed_sends =
                replica.send_to(
                    &peers,
                    service_request_constructor,
                    async_quorum_callback.clone(),
                ).await;

            assert_eq!(0, total_failed_sends);
        });
    }

    #[test]
    fn send_one_way_to_replicas_with_failure() {
        let any_replica_port = 8988;
        let any_other_replica_port = 8988;

        let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let replica = blocking_runtime.block_on(async {
            return Replica::new(
                10,
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080),
                vec![
                    HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), any_replica_port),
                    HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), any_other_replica_port),
                ],
                Box::new(SystemClock::new()),
            );
        });

        let correlation_id_generator = RandomCorrelationIdGenerator::new();
        let async_quorum_callback = AsyncQuorumCallback::<()>::new(3, 2);
        let service_request_constructor = || {
            ServiceRequest::new(
                GetValueRequest {},
                Box::new(GetValueRequestFailureClient {}),
                correlation_id_generator.generate(),
            )
        };

        blocking_runtime.block_on(async {
            let total_failed_sends =
                replica.send_to_replicas(service_request_constructor, async_quorum_callback.clone()).await;

            assert_eq!(2, total_failed_sends);
        })
    }

    #[test]
    fn add_async_to_queue() {
        let any_replica_port = 8988;
        let storage = Arc::new(RwLock::new(HashMap::new()));
        let readable_storage = storage.clone();

        let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let replica = blocking_runtime.block_on(async {
            return Replica::new(
                10,
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080),
                vec![
                    HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), any_replica_port),
                ],
                Box::new(SystemClock::new()),
            );
        });

        let (sender, mut receiver) = mpsc::channel(1);
        blocking_runtime.block_on(async {
            let _ = replica.add_to_queue(async move {
                storage.write().unwrap().insert("WAL".to_string(), "write-ahead log".to_string());
                sender.send(()).await.unwrap();
            }).await;
        });

        blocking_runtime.block_on(async {
            let _ = receiver.recv().await.unwrap();
            let read_storage = readable_storage.read().unwrap();

            assert_eq!("write-ahead log", read_storage.get("WAL").unwrap());
        });
    }

    #[test]
    fn await_for_completion_of_callback() {
        let any_other_replica_port = 8989;

        let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let replica = blocking_runtime.block_on(async {
            return Replica::new(
                10,
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080),
                vec![
                    HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), any_other_replica_port),
                ],
                Box::new(SystemClock::new()),
            );
        });

        let correlation_id_generator = FixedCorrelationIdGenerator::new(100);
        let async_quorum_callback = AsyncQuorumCallback::<GetValueResponse>::new(1, 1);
        let service_request_constructor = || {
            ServiceRequest::new(
                GetValueRequest {},
                Box::new(GetValueRequestSuccessClient {}),
                correlation_id_generator.generate(),
            )
        };

        blocking_runtime.block_on(async {
            let total_failed_sends =
                replica.send_to_replicas(service_request_constructor, async_quorum_callback.clone()).await;

            assert_eq!(0, total_failed_sends);

            let from = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), any_other_replica_port);
            let _ = replica.register_response(
                correlation_id_generator.generate(),
                from.clone(),
                Ok(Box::new(GetValueResponse { value: "some value".to_string() })),
            );

            let quorum_completion_response = async_quorum_callback.handle().await;
            assert_eq!("some value".to_string(), quorum_completion_response.success_response().unwrap().get(&from).unwrap().value);
        });
    }

    #[test]
    fn total_peer_count_excluding_self() {
        let replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8989),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9090),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080),
            ],
            Box::new(SystemClock::new()),
        );

        let total_peer_count = replica.total_peer_count();
        assert_eq!(2, total_peer_count);
    }

    #[test]
    fn total_peer_count() {
        let replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8989),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9090),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9098),
            ],
            Box::new(SystemClock::new()),
        );

        let total_peer_count = replica.total_peer_count();
        assert_eq!(3, total_peer_count);
    }

    #[test]
    fn all_peers_excluding_self() {
        let replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8989),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9090),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080),
            ],
            Box::new(SystemClock::new()),
        );

        let all_peers = replica.get_peers();
        assert_eq!(vec![
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8989),
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9090),
        ], all_peers);
    }

    #[test]
    fn all_peers() {
        let replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8989),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9090),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9098),
            ],
            Box::new(SystemClock::new()),
        );

        let all_peers = replica.get_peers();
        assert_eq!(vec![
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8989),
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9090),
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9098),
        ], all_peers);
    }

    #[test]
    fn send_one_way_to_the_replicas_without_callback_successfully() {
        let runtime = Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        let replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1080),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1989),
            ],
            Box::new(SystemClock::new()),
        );
        let replica = Arc::new(replica);
        let inner_replica = replica.clone();

        runtime.block_on(async move {
            let (sender, mut receiver) = mpsc::channel(1);

            let correlation_id_generator = RandomCorrelationIdGenerator::new();
            let service_request_constructor = move || {
                ServiceRequest::new(
                    GetValueRequest {},
                    Box::new(GetValueRequestSuccessClient {}),
                    correlation_id_generator.generate(),
                )
            };

            let response_counter = Arc::new(ResponseCounter { counter: AtomicI8::new(0) });
            let inner_response_counter = response_counter.clone();
            let response_handler_generator = move |_peer, response: Result<(), ServiceResponseError>| {
                if response.is_ok() {
                    return Some(handler(&response_counter, 1, sender.clone()));
                }
                return Some(handler(&response_counter, -1, sender.clone()));
            };
            let response_handler_generator = Arc::new(response_handler_generator);

            inner_replica.send_to_replicas_with_handler_hook(
                service_request_constructor,
                response_handler_generator.clone(),
                || None,
            );

            receiver.recv().await.unwrap();
            assert_eq!(1, inner_response_counter.counter.load(Ordering::SeqCst));
        });
    }

    #[test]
    fn send_one_way_to_replicas_without_callback_with_failure() {
        let runtime = Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        let replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1080),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1989),
            ],
            Box::new(SystemClock::new()),
        );
        let replica = Arc::new(replica);
        let inner_replica = replica.clone();

        runtime.block_on(async move {
            let (sender, mut receiver) = mpsc::channel(1);

            let correlation_id_generator = RandomCorrelationIdGenerator::new();
            let service_request_constructor = move || {
                ServiceRequest::new(
                    GetValueRequest {},
                    Box::new(GetValueRequestFailureClient {}),
                    correlation_id_generator.generate(),
                )
            };

            let response_counter = Arc::new(ResponseCounter { counter: AtomicI8::new(0) });
            let inner_response_counter = response_counter.clone();
            let response_handler_generator = Arc::new(move |_peer, response: Result<(), ServiceResponseError>| {
                if response.is_ok() {
                    return Some(handler(&response_counter, 1, sender.clone()));
                }
                return Some(handler(&response_counter, -1, sender.clone()));
            });

            inner_replica.send_to_replicas_with_handler_hook(
                service_request_constructor,
                response_handler_generator.clone(),
                || None,
            );

            receiver.recv().await.unwrap();
            assert_eq!(-1, inner_response_counter.counter.load(Ordering::SeqCst));
        });
    }

    #[test]
    fn send_one_way_to_the_replicas_with_response_hook_and_callback() {
        let runtime = Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        let replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1080),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1989),
            ],
            Box::new(SystemClock::new()),
        );
        let replica = Arc::new(replica);
        let inner_replica = replica.clone();
        let self_address = inner_replica.self_address;

        runtime.block_on(async move {
            let correlation_id_generator = FixedCorrelationIdGenerator::new(30);
            let service_request_constructor = move || {
                ServiceRequest::new(
                    GetValueRequest {},
                    Box::new(GetValueRequestSuccessClient {}),
                    correlation_id_generator.generate(),
                )
            };

            let replica = inner_replica.clone();
            let response_handler_generator = move |_peer, _response: Result<(), ServiceResponseError>| {
                let replica = inner_replica.clone();
                return Some(async move {
                    replica.register_response(
                        30,
                        self_address,
                        Ok(Box::new(String::from("success response"))),
                    )
                });
            };
            let callback = AsyncQuorumCallback::<String>::new(1, 1);
            replica.send_to_replicas_with_handler_hook(
                service_request_constructor,
                Arc::new(response_handler_generator),
                || Some(callback.clone()),
            );

            let completion_response = callback.handle().await;
            assert_eq!(&String::from("success response"),
                       completion_response.success_response().unwrap().get(&replica.get_self_address()).unwrap()
            );
        });
    }

    #[test]
    fn send_to_the_host_with_response_hook_and_callback() {
        let runtime = Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        let replica = Replica::new(
            10,
            HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1080),
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1989),
            ],
            Box::new(SystemClock::new()),
        );
        let replica = Arc::new(replica);
        let inner_replica = replica.clone();
        let self_address = inner_replica.self_address;

        runtime.block_on(async move {
            let correlation_id_generator = FixedCorrelationIdGenerator::new(30);
            let service_request_constructor = move || {
                ServiceRequest::new(
                    GetValueRequest {},
                    Box::new(GetValueRequestSuccessClient {}),
                    correlation_id_generator.generate(),
                )
            };

            let replica = inner_replica.clone();
            let response_handler_generator = move |_peer, _response: Result<(), ServiceResponseError>| {
                let replica = inner_replica.clone();
                return Some(async move {
                    replica.register_response(
                        30,
                        self_address,
                        Ok(Box::new(String::from("success response"))),
                    )
                });
            };
            let callback = AsyncQuorumCallback::<String>::new(1, 1);
            replica.send_to_with_handler_hook(
                &vec![HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1989)],
                service_request_constructor,
                Arc::new(response_handler_generator),
                || Some(callback.clone()),
            );

            let completion_response = callback.handle().await;
            assert_eq!(&String::from("success response"),
                       completion_response.success_response().unwrap().get(&replica.get_self_address()).unwrap()
            );
        });
    }

    fn handler(response_counter: &Arc<ResponseCounter>, value_add: i8, sender: Sender<()>) -> impl Future<Output=()> {
        let response_counter = response_counter.clone();
        return async move {
            response_counter.counter.fetch_add(value_add, Ordering::SeqCst);
            let _ = sender.send(()).await;
        };
    }
}
