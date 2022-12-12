use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::clock::clock::Clock;
use crate::net::connect::async_network::AsyncNetwork;
use crate::net::connect::correlation_id::CorrelationId;
use crate::net::connect::host_and_port::HostAndPort;
use crate::net::connect::service_client::{ServiceRequest, ServiceResponseError};
use crate::net::request_waiting_list::request_waiting_list::RequestWaitingList;
use crate::net::request_waiting_list::response_callback::ResponseCallbackType;

pub(crate) type TotalFailedSends = usize;

pub(crate) struct Replica {
    name: String,
    peer_addresses: Vec<Arc<HostAndPort>>,
    request_waiting_list: RequestWaitingList,
}

impl Replica {
    fn new(name: String,
           peer_addresses: Vec<Arc<HostAndPort>>,
           clock: Arc<dyn Clock>) -> Self {

        let request_waiting_list = RequestWaitingList::new(
            clock,
            Duration::from_millis(3),
            Duration::from_millis(3),
        );
        return Replica {
            name,
            peer_addresses,
            request_waiting_list,
        };
    }

    pub(crate) async fn send_one_way_to_replicas<Payload: Send + 'static, S>(&mut self,
                                                                             mut service_request_constructor: S,
                                                                             response_callback: ResponseCallbackType) -> TotalFailedSends
        where S: FnMut() -> ServiceRequest<Payload, ()> {

        let mut send_task_handles: Vec<JoinHandle<(Result<(), ServiceResponseError>, CorrelationId)>> = Vec::new();
        for peer_address in &self.peer_addresses {
            let service_request: ServiceRequest<Payload, ()> = service_request_constructor();

            send_task_handles.push(Self::send_one_way_to(
                &mut self.request_waiting_list,
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

    fn send_one_way_to<Payload: Send + 'static>(request_waiting_list: &mut RequestWaitingList,
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
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;

    use crate::clock::clock::SystemClock;
    use crate::consensus::quorum::async_quorum_callback::AsyncQuorumCallback;
    use crate::net::connect::correlation_id::RandomCorrelationIdGenerator;
    use crate::net::connect::host_and_port::HostAndPort;
    use crate::net::connect::service_client::ServiceRequest;
    use crate::net::replica::Replica;
    use crate::net::replica::tests::setup::{GetValueRequest, GetValueRequestFailureClient, GetValueRequestSuccessClient, GetValueResponse};

    mod setup {
        use std::error::Error;
        use std::fmt::{Display, Formatter};
        use std::sync::Arc;

        use async_trait::async_trait;
        use tonic::Response;

        use crate::net::connect::host_and_port::HostAndPort;
        use crate::net::connect::service_client::{ServiceClientProvider, ServiceResponseError};

        #[derive(Debug)]
        pub struct GetValueRequest {}

        #[derive(Debug)]
        pub struct GetValueResponse {}

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
    }

    #[tokio::test]
    async fn send_one_way_to_replicas_successfully() {
        let any_replica_port = 8988;
        let any_other_replica_port = 8988;

        let mut replica = Replica::new(
            String::from("neptune"),
            vec![
                Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), any_replica_port)),
                Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), any_other_replica_port)),
            ],
            Arc::new(SystemClock::new()),
        );

        let mut correlation_id_generator = RandomCorrelationIdGenerator::new();
        let async_quorum_callback = AsyncQuorumCallback::<GetValueResponse>::new(2);
        let service_request_constructor = || {
            ServiceRequest::new(
                GetValueRequest {},
                Box::new(GetValueRequestSuccessClient {}),
                correlation_id_generator.generate()
            )
        };

        let total_failed_sends =
            replica.send_one_way_to_replicas(service_request_constructor, async_quorum_callback.clone()).await;

        assert_eq!(0, total_failed_sends);
    }

    #[tokio::test]
    async fn send_one_way_to_replicas_with_failure() {
        let any_replica_port = 8988;
        let any_other_replica_port = 8988;

        let mut replica = Replica::new(
            String::from("neptune"),
            vec![
                Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), any_replica_port)),
                Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), any_other_replica_port)),
            ],
            Arc::new(SystemClock::new()),
        );

        let mut correlation_id_generator = RandomCorrelationIdGenerator::new();
        let async_quorum_callback = AsyncQuorumCallback::<GetValueResponse>::new(2);
        let service_request_constructor = || {
            ServiceRequest::new(
                GetValueRequest {},
                Box::new(GetValueRequestFailureClient {}),
                correlation_id_generator.generate()
            )
        };

        let total_failed_sends =
            replica.send_one_way_to_replicas(service_request_constructor, async_quorum_callback.clone()).await;

        assert_eq!(2, total_failed_sends);
    }
}