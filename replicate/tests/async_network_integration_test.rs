use std::net::{IpAddr, Ipv4Addr};
use std::thread;
use std::time::Duration;

use async_trait::async_trait;
use tonic::{Request, Response};
use tonic::transport::Channel;

use replicate::net::connect::async_network::AsyncNetwork;
use replicate::net::connect::correlation_id::CorrelationIdGenerator;
use replicate::net::connect::host_and_port::HostAndPort;
use replicate::net::connect::random_correlation_id_generator::RandomCorrelationIdGenerator;
use replicate::net::connect::service_client::{ServiceClientProvider, ServiceRequest};
use replicate::net::connect::error::ServiceResponseError;
use replicate::net::connect::service_registration::{AllServicesShutdownHandle, ServiceRegistration};

use crate::grpc::{EchoRequest, EchoResponse};
use crate::grpc::echo_client::EchoClient;
use crate::grpc::echo_server::{Echo, EchoServer};

pub mod grpc {
    tonic::include_proto!("replicate.tests.echo");
}

struct EchoService {}

#[tonic::async_trait]
impl Echo for EchoService {
    async fn acknowledge_echo(&self, request: Request<EchoRequest>) -> Result<Response<EchoResponse>, tonic::Status> {
        let request = request.into_inner();
        let correlation_id = request.correlation_id;
        return Ok(Response::new(EchoResponse { message: request.message, correlation_id }));
    }
}

struct EchoServiceClient {}

#[async_trait]
impl ServiceClientProvider<EchoRequest, EchoResponse> for EchoServiceClient {
    async fn call(&self, request: Request<EchoRequest>, address: HostAndPort, _channel: Option<Channel>) -> Result<Response<EchoResponse>, ServiceResponseError> {
        let mut client = EchoClient::connect(address.as_string_with_http()).await?;
        let response = client.acknowledge_echo(request).await?;
        return Ok(response);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send() {
    let server_address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
    let server_address_clone_one = server_address.clone();
    let server_address_clone_other = server_address.clone();

    let (all_services_shutdown_handle, all_services_shutdown_receiver) = AllServicesShutdownHandle::new();
    let echo_service = EchoService {};

    let server_handle = tokio::spawn(async move {
        ServiceRegistration::register_services_on(
            &server_address_clone_one,
            EchoServer::new(echo_service),
            all_services_shutdown_receiver).await;
    });
    thread::sleep(Duration::from_secs(3));
    let client_handle = tokio::spawn(async move {
        let response = send_client_request(server_address_clone_other).await;
        assert!(response.is_ok());
        assert_eq!("test message".to_string(), response.unwrap().message);

        all_services_shutdown_handle.shutdown().await.unwrap();
    });

    server_handle.await.unwrap();
    client_handle.await.unwrap();
}

async fn send_client_request(target_address: HostAndPort) -> Result<EchoResponse, ServiceResponseError> {
    let correlation_id = RandomCorrelationIdGenerator::new().generate();
    let request = EchoRequest {
        message: "test message".to_string(),
        correlation_id,
    };
    let service_request = ServiceRequest::new(
        request,
        Box::new(EchoServiceClient {}),
        correlation_id,
    );
    return AsyncNetwork::send_without_source_footprint(service_request, target_address).await;
}