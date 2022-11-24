use tonic::Response;

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

    use crate::net::connect::service::heartbeat::service_request::HeartbeatServiceRequest;
    use crate::net::connect::service_registration::{AllServicesShutdownHandle, ServiceRegistration};

    use super::*;

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

    async fn send_client_request(address: &HostAndPort) -> Result<(), ServiceResponseError> {
        let service_server_request = HeartbeatServiceRequest::new("100".to_string());
        return AsyncNetwork::send(service_server_request, address).await;
    }
}
