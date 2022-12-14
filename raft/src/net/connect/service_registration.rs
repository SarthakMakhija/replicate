use std::convert::Infallible;

use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::error::SendError;
use tonic::body::BoxBody;
use tonic::codegen::http::Response;
use tonic::codegen::Service;
use tonic::transport::Server;
use tonic::transport::server::Router;

use crate::net::connect::host_and_port::HostAndPort;
use crate::net::connect::service::heartbeat::service::grpc::heartbeat_server::HeartbeatServer;
use crate::net::connect::service::heartbeat::service::HeartbeatService;

pub struct ServiceRegistration {}

impl ServiceRegistration {
    pub async fn register_default_services_on(address: &HostAndPort, all_services_shutdown_signal_receiver: Receiver<()>) {
        let heartbeat_service = HeartbeatService::default();
        let router = Server::builder().add_service(HeartbeatServer::new(heartbeat_service));

        Self::serve(router, address, all_services_shutdown_signal_receiver).await;
    }

    pub async fn register_services_on<S>(address: &HostAndPort, service: S, all_services_shutdown_signal_receiver: Receiver<()>)
        where
            S: Service<tonic::codegen::http::Request<tonic::transport::Body>, Response=Response<BoxBody>, Error=Infallible>
            + Clone
            + tonic::server::NamedService
            + Send
            + 'static, S::Future: Send + 'static, {

        let mut server: Server = Server::builder();
        let router = server.add_service(service);

        let heartbeat_service = HeartbeatService::default();
        let router = router.add_service(HeartbeatServer::new(heartbeat_service));

        Self::serve(router, address, all_services_shutdown_signal_receiver).await;
    }

    async fn serve(router: Router, address: &HostAndPort, mut all_services_shutdown_signal_receiver: Receiver<()>) {
        let socket_address = address.as_socket_address().unwrap();
        let shutdown_block = async move {
            all_services_shutdown_signal_receiver.recv().await.map(|_| ());
            return;
        };

        router
            .serve_with_shutdown(socket_address, shutdown_block)
            .await.expect(format!("Failed to register services on {:?}", socket_address).as_str());
    }
}

pub struct AllServicesShutdownHandle {
    all_services_shutdown_signal_sender: Sender<()>,
}

impl AllServicesShutdownHandle {
    pub fn new() -> (AllServicesShutdownHandle, Receiver<()>) {
        let (all_services_shutdown_signal_sender, all_services_shutdown_signal_receiver) = mpsc::channel(1);
        return (AllServicesShutdownHandle { all_services_shutdown_signal_sender }, all_services_shutdown_signal_receiver);
    }

    pub async fn shutdown(&self) -> Result<(), SendError<()>> {
        return self.all_services_shutdown_signal_sender.clone().send(()).await;
    }
}