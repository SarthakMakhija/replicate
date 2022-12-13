use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::clock::clock::Clock;
use crate::net::connect::async_network::AsyncNetwork;
use crate::net::connect::correlation_id::CorrelationId;
use crate::net::connect::host_and_port::HostAndPort;
use crate::net::connect::service_client::{ServiceRequest, ServiceResponseError};
use crate::net::request_waiting_list::request_waiting_list::RequestWaitingList;
use crate::net::request_waiting_list::response_callback::{AnyResponse, ResponseCallbackType, ResponseErrorType};
use crate::singular_update_queue::singular_update_queue::SingularUpdateQueue;

pub type TotalFailedSends = usize;

pub struct Replica {
    name: String,
    peer_connection_address: Arc<HostAndPort>,
    peer_addresses: Vec<Arc<HostAndPort>>,
    request_waiting_list: RequestWaitingList,
    singular_update_queue: SingularUpdateQueue,
}

impl Replica {
    pub fn new(name: String,
               peer_connection_address: Arc<HostAndPort>,
               peer_addresses: Vec<Arc<HostAndPort>>,
               clock: Arc<dyn Clock>) -> Self {
        let request_waiting_list = RequestWaitingList::new(
            clock,
            Duration::from_millis(3),
            Duration::from_millis(3),
        );
        return Replica {
            name,
            peer_connection_address,
            peer_addresses,
            request_waiting_list,
            singular_update_queue: SingularUpdateQueue::new(),
        };
    }

    pub async fn send_one_way_to_replicas<Payload: Send + 'static, S>(&self,
                                                                      service_request_constructor: S,
                                                                      response_callback: ResponseCallbackType) -> TotalFailedSends
        where S: Fn() -> ServiceRequest<Payload, ()> {
        let mut send_task_handles: Vec<JoinHandle<(Result<(), ServiceResponseError>, CorrelationId)>> = Vec::new();
        for peer_address in &self.peer_addresses {
            let service_request: ServiceRequest<Payload, ()> = service_request_constructor();

            send_task_handles.push(Self::send_one_way_to(
                &self.request_waiting_list,
                service_request,
                peer_address.clone(),
                response_callback.clone(),
            ));
        }

        let mut total_failed_sends: TotalFailedSends = 0;
        for task_handle in send_task_handles {
            let response: (Result<(), ServiceResponseError>, CorrelationId) = task_handle.await.unwrap();
            if response.0.is_err() {
                let _ = &self.request_waiting_list.handle_response(response.1, Err(response.0.unwrap_err()));
                total_failed_sends = total_failed_sends + 1;
            }
        }
        return total_failed_sends;
    }

    pub fn add_to_queue<F>(&self, handler: F)
        where
            F: Future + Send + 'static,
            F::Output: Send + 'static {
        let singular_update_queue = &self.singular_update_queue;
        singular_update_queue.submit(handler);
    }

    pub fn handle_response(&self, correlation_id: CorrelationId, response: Result<AnyResponse, ResponseErrorType>) {
        let _ = &self.request_waiting_list.handle_response(correlation_id, response);
    }

    pub fn get_peer_connection_address(&self) -> Arc<HostAndPort> {
        return self.peer_connection_address.clone();
    }

    fn send_one_way_to<Payload: Send + 'static>(request_waiting_list: &RequestWaitingList,
                                                service_request: ServiceRequest<Payload, ()>,
                                                address: Arc<HostAndPort>,
                                                response_callback: ResponseCallbackType) -> JoinHandle<(Result<(), ServiceResponseError>, CorrelationId)> {
        let correlation_id = service_request.correlation_id;
        request_waiting_list.add(correlation_id, response_callback);

        return tokio::spawn(async move {
            let result = AsyncNetwork::send(service_request, address).await;
            return (result, correlation_id);
        });
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::{Arc, RwLock};

    use tokio::sync::mpsc;

    use crate::clock::clock::SystemClock;
    use crate::consensus::quorum::async_quorum_callback::AsyncQuorumCallback;
    use crate::net::connect::correlation_id::CorrelationIdGenerator;
    use crate::net::connect::host_and_port::HostAndPort;
    use crate::net::connect::random_correlation_id_generator::RandomCorrelationIdGenerator;
    use crate::net::connect::service_client::ServiceRequest;
    use crate::net::replica::Replica;
    use crate::net::replica::tests::setup::{FixedCorrelationIdGenerator, GetValueRequest, GetValueRequestFailureClient, GetValueRequestSuccessClient, GetValueResponse};

    mod setup {
        use std::error::Error;
        use std::fmt::{Display, Formatter};
        use std::sync::Arc;

        use async_trait::async_trait;
        use tonic::Response;

        use crate::net::connect::correlation_id::{CorrelationId, CorrelationIdGenerator};
        use crate::net::connect::host_and_port::HostAndPort;
        use crate::net::connect::service_client::{ServiceClientProvider, ServiceResponseError};

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
            async fn call(&self, _: GetValueRequest, _: Arc<HostAndPort>) -> Result<Response<()>, ServiceResponseError> {
                return Ok(Response::new(()));
            }
        }

        #[async_trait]
        impl ServiceClientProvider<GetValueRequest, ()> for GetValueRequestFailureClient {
            async fn call(&self, _: GetValueRequest, _: Arc<HostAndPort>) -> Result<Response<()>, ServiceResponseError> {
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
    }

    #[tokio::test]
    async fn send_one_way_to_replicas_successfully() {
        let any_replica_port = 8988;
        let any_other_replica_port = 8989;

        let replica = Replica::new(
            String::from("neptune"),
            Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080)),
            vec![
                Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), any_replica_port)),
                Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), any_other_replica_port)),
            ],
            Arc::new(SystemClock::new()),
        );

        let correlation_id_generator = RandomCorrelationIdGenerator::new();
        let async_quorum_callback = AsyncQuorumCallback::<GetValueResponse>::new(2);
        let service_request_constructor = || {
            ServiceRequest::new(
                GetValueRequest {},
                Box::new(GetValueRequestSuccessClient {}),
                correlation_id_generator.generate(),
            )
        };

        let total_failed_sends =
            replica.send_one_way_to_replicas(service_request_constructor, async_quorum_callback.clone()).await;

        assert_eq!(0, total_failed_sends);
        replica.singular_update_queue.shutdown();
    }

    #[tokio::test]
    async fn send_one_way_to_replicas_with_failure() {
        let any_replica_port = 8988;
        let any_other_replica_port = 8988;

        let replica = Replica::new(
            String::from("neptune"),
            Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080)),
            vec![
                Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), any_replica_port)),
                Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), any_other_replica_port)),
            ],
            Arc::new(SystemClock::new()),
        );

        let correlation_id_generator = RandomCorrelationIdGenerator::new();
        let async_quorum_callback = AsyncQuorumCallback::<GetValueResponse>::new(2);
        let service_request_constructor = || {
            ServiceRequest::new(
                GetValueRequest {},
                Box::new(GetValueRequestFailureClient {}),
                correlation_id_generator.generate(),
            )
        };

        let total_failed_sends =
            replica.send_one_way_to_replicas(service_request_constructor, async_quorum_callback.clone()).await;

        assert_eq!(2, total_failed_sends);
        replica.singular_update_queue.shutdown();
    }

    #[tokio::test]
    async fn add_to_queue() {
        let any_replica_port = 8988;
        let storage = Arc::new(RwLock::new(HashMap::new()));
        let readable_storage = storage.clone();
        let replica = Replica::new(
            String::from("neptune"),
            Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080)),
            vec![
                Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), any_replica_port)),
            ],
            Arc::new(SystemClock::new()),
        );

        let (sender, mut receiver) = mpsc::channel(1);
        replica.add_to_queue(async move {
            storage.write().unwrap().insert("WAL".to_string(), "write-ahead log".to_string());
            sender.send(()).await.unwrap();
        });

        let _ = receiver.recv().await.unwrap();
        let read_storage = readable_storage.read().unwrap();

        assert_eq!("write-ahead log", read_storage.get("WAL").unwrap());
        replica.singular_update_queue.shutdown();
    }

    #[tokio::test]
    async fn await_for_completion_of_callback() {
        let any_other_replica_port = 8989;

        let replica = Replica::new(
            String::from("neptune"),
            Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080)),
            vec![
                Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), any_other_replica_port)),
            ],
            Arc::new(SystemClock::new()),
        );

        let correlation_id_generator = FixedCorrelationIdGenerator::new(100);
        let async_quorum_callback = AsyncQuorumCallback::<GetValueResponse>::new(1);
        let service_request_constructor = || {
            ServiceRequest::new(
                GetValueRequest {},
                Box::new(GetValueRequestSuccessClient {}),
                correlation_id_generator.generate(),
            )
        };

        let total_failed_sends =
            replica.send_one_way_to_replicas(service_request_constructor, async_quorum_callback.clone()).await;

        assert_eq!(0, total_failed_sends);

        let _ = replica.handle_response(
            correlation_id_generator.generate(),
            Ok(Box::new(GetValueResponse { value: "ok".to_string() }))
        );

        let quorum_completion_response = async_quorum_callback.handle().await;
        assert_eq!("ok".to_string(), quorum_completion_response.success_responses().unwrap().get(0).unwrap().value);

        replica.singular_update_queue.shutdown();
    }
}
