use tonic::Response;

use crate::net::connect::host_and_port::HostAndPort;
use crate::net::connect::service_server_callable::{ServiceServerError, ServiceServerRequest};

pub(crate) struct Network {}

impl Network {
    pub(crate) async fn send<Payload: Send, R: Send>(service_server_request: ServiceServerRequest<Payload, R>, address: &HostAndPort) -> Result<Response<R>, ServiceServerError> {
        let callable = &service_server_request.callable;
        let payload = service_server_request.payload;
        return callable.call(payload, &address).await;
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use crate::net::connect::service_server::heartbeat::service_request::HeartbeatServiceServerRequest;
    use crate::net::connect::service_server_registration::{ServiceServerRegistration, ServiceServerShutdownHandle};

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send() {
        let server_address = Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051));
        let server_address_clone_one = server_address.clone();
        let server_address_clone_other = server_address.clone();

        let (handle, receiver) = ServiceServerShutdownHandle::new();
        let server_handle = tokio::spawn(async move {
            ServiceServerRegistration::register_all_services_on(&server_address_clone_one, receiver).await;
        });

        thread::sleep(Duration::from_secs(3));
        let client_handle = tokio::spawn(async move {
            let response = send_client_request(&server_address_clone_other).await;
            assert!(response.is_ok());
            handle.shutdown();
        });
        server_handle.await.unwrap();
        client_handle.await.unwrap();
    }

    async fn send_client_request(address: &HostAndPort) -> Result<Response<()>, ServiceServerError> {
        let service_server_request = HeartbeatServiceServerRequest::new("100".to_string());
        return Network::send(service_server_request, address).await;
    }
}
