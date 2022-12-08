use std::sync::Arc;
use crate::net::connect::host_and_port::HostAndPort;
use crate::net::connect::service_client::{ServiceRequest, ServiceResponseError};

pub struct AsyncNetwork {}

impl AsyncNetwork {
    pub async fn send<Payload: Send, R>(service_server_request: ServiceRequest<Payload, R>, address: Arc<HostAndPort>) -> Result<R, ServiceResponseError> {
        let client = &service_server_request.service_client;
        let payload = service_server_request.payload;
        let result = client.call(payload, address.clone()).await;
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

    use crate::net::connect::async_network::tests::setup::{test_failure_service_request, test_success_service_request};
    use crate::net::connect::async_network::tests::setup_error::TestError;

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
        use std::sync::Arc;
        use async_trait::async_trait;
        use tonic::Response;

        use crate::net::connect::async_network::tests::setup_error::TestError;
        use crate::net::connect::correlation_id::{CorrelationId, CorrelationIdGenerator};
        use crate::net::connect::host_and_port::HostAndPort;
        use crate::net::connect::service_client::{ServiceClientProvider, ServiceRequest, ServiceResponseError};

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
        impl ServiceClientProvider<TestRequest, TestResponse> for SuccessTestClient {
            async fn call(&self, request: TestRequest, _: Arc<HostAndPort>) -> Result<Response<TestResponse>, ServiceResponseError> {
                return Ok(Response::new(TestResponse { correlation_id: request.id }));
            }
        }

        #[async_trait]
        impl ServiceClientProvider<TestRequest, TestResponse> for FailureTestClient {
            async fn call(&self, _: TestRequest, _: Arc<HostAndPort>) -> Result<Response<TestResponse>, ServiceResponseError> {
                return Err(Box::new(TestError { message: "test error".to_string() }));
            }
        }

        pub(crate) fn test_success_service_request(id: u8) -> ServiceRequest<TestRequest, TestResponse> {
            let any_correlation_id: CorrelationId = CorrelationIdGenerator::fixed();
            return ServiceRequest::new(TestRequest { id }, Box::new(SuccessTestClient {}), any_correlation_id);
        }

        pub(crate) fn test_failure_service_request(id: u8) -> ServiceRequest<TestRequest, TestResponse> {
            let any_correlation_id: CorrelationId = CorrelationIdGenerator::fixed();
            return ServiceRequest::new(TestRequest { id }, Box::new(FailureTestClient {}), any_correlation_id);
        }
    }

    #[tokio::test]
    async fn send_successfully() {
        let server_address = Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051));
        let id = 100;
        let result = AsyncNetwork::send(test_success_service_request(id), server_address.clone()).await;

        assert!(result.is_ok());
        assert_eq!(id, result.unwrap().correlation_id);
    }

    #[tokio::test]
    async fn send_with_failure() {
        let server_address = Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051));
        let id = 100;
        let result = AsyncNetwork::send(test_failure_service_request(id), server_address.clone()).await;

        assert!(result.is_err());
        assert_eq!("test error", result.unwrap_err().downcast_ref::<TestError>().unwrap().message);
    }
}
