use crate::net::connect::host_and_port::HostAndPort;
use crate::net::connect::service_client::{ServiceRequest, ServiceResponseError};

pub(crate) struct AsyncNetwork {}

impl AsyncNetwork {
    pub(crate) async fn send<Payload: Send, R: Send>(service_server_request: ServiceRequest<Payload, R>, address: &HostAndPort) -> Result<R, ServiceResponseError> {
        let client = &service_server_request.service_client;
        let payload = service_server_request.payload;
        let result = client.call(payload, &address).await;
        return match result {
            Ok(response) => { Ok(response.into_inner()) }
            Err(e) => { Err(e) }
        };
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use crate::net::connect::async_network::tests::setup::{test_failure_service_request, test_success_service_request};
    use crate::net::connect::async_network::tests::setup_error::TestError;
    use crate::net::connect::service::heartbeat::service_request::HeartbeatServiceRequest;
    use crate::net::connect::service_registration::{AllServicesShutdownHandle, ServiceRegistration};

    use super::*;

    mod setup_error {
        use std::error::Error;
        use std::fmt::{Display, Formatter};

        #[derive(Debug)]
        pub struct TestError {
            pub message: String,
        }

        impl Display for TestError {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "{}", self.message)
            }
        }

        impl Error for TestError {}
    }


    mod setup {
        use async_trait::async_trait;
        use tonic::Response;

        use crate::net::connect::async_network::tests::setup_error::TestError;
        use crate::net::connect::host_and_port::HostAndPort;
        use crate::net::connect::service_client::{ServiceClient, ServiceRequest, ServiceResponseError};

        pub(crate) struct TestRequest {
            pub(crate) id: u8,
        }

        #[derive(Debug)]
        pub(crate) struct TestResponse {
            pub(crate) correlation_id: u8,
        }

        pub(crate) struct SuccessTestClient {}

        pub(crate) struct FailureTestClient {}

        #[async_trait]
        impl ServiceClient<TestRequest, TestResponse> for SuccessTestClient {
            async fn call(&self, request: TestRequest, _: &HostAndPort) -> Result<Response<TestResponse>, ServiceResponseError> {
                return Ok(Response::new(TestResponse { correlation_id: request.id }));
            }
        }

        #[async_trait]
        impl ServiceClient<TestRequest, TestResponse> for FailureTestClient {
            async fn call(&self, _: TestRequest, _: &HostAndPort) -> Result<Response<TestResponse>, ServiceResponseError> {
                return Err(Box::new(TestError { message: "test error".to_string() }));
            }
        }

        pub(crate) fn test_success_service_request(id: u8) -> ServiceRequest<TestRequest, TestResponse> {
            return ServiceRequest { payload: TestRequest { id }, service_client: Box::new(SuccessTestClient {}) };
        }

        pub(crate) fn test_failure_service_request(id: u8) -> ServiceRequest<TestRequest, TestResponse> {
            return ServiceRequest { payload: TestRequest { id }, service_client: Box::new(FailureTestClient {}) };
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send() {
        let server_address = Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051));
        let server_address_clone_one = server_address.clone();
        let server_address_clone_other = server_address.clone();

        let (all_services_shutdown_handle, all_services_shutdown_receiver) = AllServicesShutdownHandle::new();
        let server_handle = tokio::spawn(async move {
            ServiceRegistration::register_all_services_on(&server_address_clone_one, all_services_shutdown_receiver).await;
        });

        thread::sleep(Duration::from_secs(3));
        let client_handle = tokio::spawn(async move {
            let response = send_client_request(&server_address_clone_other).await;
            assert!(response.is_ok());
            all_services_shutdown_handle.shutdown();
        });
        server_handle.await.unwrap();
        client_handle.await.unwrap();
    }

    #[tokio::test]
    async fn send_successfully() {
        let server_address = Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051));
        let id = 100;
        let result = AsyncNetwork::send(test_success_service_request(id), &server_address).await;

        assert!(result.is_ok());
        assert_eq!(id, result.unwrap().correlation_id);
    }

    #[tokio::test]
    async fn send_with_failure() {
        let server_address = Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051));
        let id = 100;
        let result = AsyncNetwork::send(test_failure_service_request(id), &server_address).await;

        assert!(result.is_err());
        assert_eq!("test error", result.unwrap_err().downcast_ref::<TestError>().unwrap().message);
    }

    async fn send_client_request(address: &HostAndPort) -> Result<(), ServiceResponseError> {
        let node_id = "mark";
        let service_server_request = HeartbeatServiceRequest::new(node_id.to_string());
        return AsyncNetwork::send(service_server_request, address).await;
    }
}
