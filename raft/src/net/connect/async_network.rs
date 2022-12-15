use tonic::metadata::MetadataValue;
use tonic::Request;

use crate::net::connect::headers::{REFERRAL_HOST, REFERRAL_PORT};
use crate::net::connect::host_and_port::HostAndPort;
use crate::net::connect::service_client::{ServiceRequest, ServiceResponseError};

pub struct AsyncNetwork {}

impl AsyncNetwork {
    pub async fn send_with_source_footprint<Payload: Send, R>(
        service_request: ServiceRequest<Payload, R>,
        source_address: HostAndPort,
        target_address: HostAndPort,
    ) -> Result<R, ServiceResponseError>
        where Payload: Send { return Self::send(service_request, Some(source_address), target_address).await; }

    pub async fn send_without_source_footprint<Payload: Send, R>(
        service_request: ServiceRequest<Payload, R>,
        target_address: HostAndPort,
    ) -> Result<R, ServiceResponseError>
        where Payload: Send { return Self::send(service_request, None, target_address).await; }

    async fn send<Payload: Send, R>(
        service_request: ServiceRequest<Payload, R>,
        source_address: Option<HostAndPort>,
        target_address: HostAndPort,
    ) -> Result<R, ServiceResponseError>
        where Payload: Send {
        let client = &service_request.service_client;
        let payload = service_request.payload;
        let mut request = Request::new(payload);

        if let Some(address) = source_address {
            let headers = request.metadata_mut();
            headers.insert(REFERRAL_HOST, address.host_as_string().parse().unwrap());
            headers.insert(REFERRAL_PORT, MetadataValue::from(address.port()));
        }

        let result = client.call(request, target_address).await;
        return match result {
            Ok(response) => { Ok(response.into_inner()) }
            Err(e) => { Err(e) }
        };
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use crate::net::connect::async_network::tests::setup::{test_failure_service_request, test_service_request_with_footprint, test_success_service_request};
    use crate::net::connect::async_network::tests::setup_error::TestError;
    use crate::net::connect::random_correlation_id_generator::RandomCorrelationIdGenerator;

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
        use tonic::{Request, Response};

        use crate::net::connect::async_network::tests::setup_error::TestError;
        use crate::net::connect::correlation_id::{CorrelationId, CorrelationIdGenerator};
        use crate::net::connect::headers::{get_referral_host_from, get_referral_port_from};
        use crate::net::connect::host_and_port::HostAndPort;
        use crate::net::connect::service_client::{ServiceClientProvider, ServiceRequest, ServiceResponseError};

        pub(crate) struct TestRequest {
            pub(crate) id: u8,
        }

        #[derive(Debug)]
        pub(crate) struct ResponseWithSourceFootprint {
            pub(crate) host: Option<String>,
            pub(crate) port: Option<u32>,
        }

        #[derive(Debug)]
        pub(crate) struct TestResponse {
            pub(crate) correlation_id: u8,
        }

        pub(crate) struct SuccessTestClient {}

        pub(crate) struct FailureTestClient {}

        pub(crate) struct FootprintTestClient {}

        #[async_trait]
        impl ServiceClientProvider<TestRequest, TestResponse> for SuccessTestClient {
            async fn call(&self, request: Request<TestRequest>, _: HostAndPort) -> Result<Response<TestResponse>, ServiceResponseError> {
                return Ok(Response::new(TestResponse { correlation_id: request.into_inner().id }));
            }
        }

        #[async_trait]
        impl ServiceClientProvider<TestRequest, TestResponse> for FailureTestClient {
            async fn call(&self, _: Request<TestRequest>, _: HostAndPort) -> Result<Response<TestResponse>, ServiceResponseError> {
                return Err(Box::new(TestError { message: "test error".to_string() }));
            }
        }

        #[async_trait]
        impl<'a> ServiceClientProvider<TestRequest, ResponseWithSourceFootprint> for FootprintTestClient {
            async fn call(&self, request: Request<TestRequest>, _: HostAndPort) -> Result<Response<ResponseWithSourceFootprint>, ServiceResponseError> {
                let response = ResponseWithSourceFootprint {
                    host: get_referral_host_from(&request),
                    port: get_referral_port_from(&request),
                };

                return Ok(Response::new(response));
            }
        }

        pub(crate) fn test_success_service_request(id: u8, correlation_id_generator: &dyn CorrelationIdGenerator) -> ServiceRequest<TestRequest, TestResponse> {
            let any_correlation_id: CorrelationId = correlation_id_generator.generate();
            return ServiceRequest::new(TestRequest { id }, Box::new(SuccessTestClient {}), any_correlation_id);
        }

        pub(crate) fn test_failure_service_request(id: u8, correlation_id_generator: &dyn CorrelationIdGenerator) -> ServiceRequest<TestRequest, TestResponse> {
            let any_correlation_id: CorrelationId = correlation_id_generator.generate();
            return ServiceRequest::new(TestRequest { id }, Box::new(FailureTestClient {}), any_correlation_id);
        }

        pub(crate) fn test_service_request_with_footprint(id: u8, correlation_id_generator: &dyn CorrelationIdGenerator) -> ServiceRequest<TestRequest, ResponseWithSourceFootprint> {
            let any_correlation_id: CorrelationId = correlation_id_generator.generate();
            return ServiceRequest::new(TestRequest { id }, Box::new(FootprintTestClient {}), any_correlation_id);
        }
    }

    #[tokio::test]
    async fn send_successfully() {
        let target_address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
        let source_address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9090);
        let id = 100;
        let correlation_id_generator = RandomCorrelationIdGenerator::new();

        let result = AsyncNetwork::send_with_source_footprint(
            test_success_service_request(id, &correlation_id_generator),
            source_address,
            target_address,
        ).await;

        assert!(result.is_ok());
        assert_eq!(id, result.unwrap().correlation_id);
    }

    #[tokio::test]
    async fn send_with_failure() {
        let target_address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
        let source_address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9090);

        let id = 100;
        let correlation_id_generator = RandomCorrelationIdGenerator::new();
        let result = AsyncNetwork::send_with_source_footprint(
            test_failure_service_request(id, &correlation_id_generator),
            source_address,
            target_address,
        ).await;

        assert!(result.is_err());
        assert_eq!("test error", result.unwrap_err().downcast_ref::<TestError>().unwrap().message);
    }

    #[tokio::test]
    async fn send_with_source_footprint() {
        let target_address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
        let source_address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9090);
        let id = 100;
        let correlation_id_generator = RandomCorrelationIdGenerator::new();

        let result = AsyncNetwork::send_with_source_footprint(
            test_service_request_with_footprint(id, &correlation_id_generator),
            source_address,
            target_address,
        ).await;

        let response = result.unwrap();
        assert_eq!("127.0.0.1".to_string(), response.host.unwrap());
        assert_eq!(9090, response.port.unwrap());
    }

    #[tokio::test]
    async fn send_without_source_footprint() {
        let target_address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
        let id = 100;
        let correlation_id_generator = RandomCorrelationIdGenerator::new();

        let result = AsyncNetwork::send_without_source_footprint(
            test_service_request_with_footprint(id, &correlation_id_generator),
            target_address,
        ).await;

        let response = result.unwrap();
        assert_eq!(None, response.host);
        assert_eq!(None, response.port);
    }
}
