use std::net::{IpAddr, Ipv4Addr};
use std::thread;
use std::time::Duration;

use async_trait::async_trait;
use tonic::{Request, Response};
use tonic::transport::Channel;

use replicate::net::connect::host_and_port::HostAndPort;
use replicate::net::connect::service_client::{ServiceClientProvider, ServiceRequest};
use replicate::net::connect::error::ServiceResponseError;
use replicate::net::connect::service_registration::{AllServicesShutdownHandle, ServiceRegistration};
use replicate_simulation::net::connect::async_network::{AsyncNetwork, RequestDropError};

use crate::grpc::{EchoRequest, EchoResponse};
use crate::grpc::echo_client::EchoClient;
use crate::grpc::echo_server::{Echo, EchoServer};

pub mod grpc {
    tonic::include_proto!("simulation.tests.echo");
}

struct EchoService {}

#[tonic::async_trait]
impl Echo for EchoService {
    async fn acknowledge_echo(&self, request: Request<EchoRequest>) -> Result<Response<EchoResponse>, tonic::Status> {
        let request = request.into_inner();
        return Ok(Response::new(EchoResponse { message: request.message }));
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
#[cfg(feature = "test_type_integration")]
async fn send_successfully() {
    let server_address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8713);
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
        let network = AsyncNetwork::new();
        let response = send_client_request(server_address_clone_other, &network).await;
        assert!(response.is_ok());
        assert_eq!("test message".to_string(), response.unwrap().message);

        all_services_shutdown_handle.shutdown().await.unwrap();
    });

    server_handle.await.unwrap();
    client_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "test_type_integration")]
async fn drop_requests() {
    let server_address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8716);
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
        let network = AsyncNetwork::new();
        network.drop_requests_to(server_address.clone());

        let response = send_client_request(server_address_clone_other, &network).await;
        assert!(response.is_err());
        assert_eq!(server_address.clone(), response.unwrap_err().downcast::<RequestDropError>().unwrap().host_and_port);

        all_services_shutdown_handle.shutdown().await.unwrap();
    });

    server_handle.await.unwrap();
    client_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "test_type_integration")]
async fn drop_requests_after_2_requests() {
    let server_address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8719);
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
        let network = AsyncNetwork::new();
        network.drop_requests_after(2, server_address.clone());

        let response = send_client_request(server_address_clone_other, &network).await;
        assert!(response.is_ok());
        let response = send_client_request(server_address_clone_other, &network).await;
        assert!(response.is_ok());

        let response = send_client_request(server_address_clone_other, &network).await;
        assert!(response.is_err());
        assert_eq!(server_address.clone(), response.unwrap_err().downcast::<RequestDropError>().unwrap().host_and_port);

        all_services_shutdown_handle.shutdown().await.unwrap();
    });

    server_handle.await.unwrap();
    client_handle.await.unwrap();
}

async fn send_client_request(target_address: HostAndPort, network: &AsyncNetwork) -> Result<EchoResponse, ServiceResponseError> {
    let request = EchoRequest {
        message: "test message".to_string(),
    };
    let service_request = ServiceRequest::new(
        request,
        Box::new(EchoServiceClient {}),
        0,
    );
    return network.send_without_source_footprint(service_request, target_address).await;
}