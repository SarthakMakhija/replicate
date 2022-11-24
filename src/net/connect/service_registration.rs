use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::error::SendError;
use tonic::transport::Server;

use crate::net::connect::host_and_port::HostAndPort;
use crate::net::connect::service::heartbeat::service::grpc::heartbeat_server::HeartbeatServer;
use crate::net::connect::service::heartbeat::service::HeartbeatService;

pub(crate) struct ServiceRegistration {}

impl ServiceRegistration {
    pub(crate) async fn register_all_services_on(address: &HostAndPort, mut all_services_shutdown_signal_receiver: Receiver<()>) {
        let heartbeat_service = HeartbeatService::default();
        let socket_address = address.as_socket_address().unwrap();

        let shutdown_block = async move {
            all_services_shutdown_signal_receiver.recv().await.map(|_| ());
            return;
        };
        Server::builder()
            .add_service(HeartbeatServer::new(heartbeat_service))
            .serve_with_shutdown(socket_address, shutdown_block)
            .await
            .expect(format!("Failed to register services on {:?}", socket_address).as_str());
    }
}

pub(crate) struct AllServicesShutdownHandle {
    all_services_shutdown_signal_sender: Sender<()>,
}

impl AllServicesShutdownHandle {
    pub(crate) fn new() -> (AllServicesShutdownHandle, Receiver<()>) {
        let (all_services_shutdown_signal_sender, all_services_shutdown_signal_receiver) = mpsc::channel(1);
        return (AllServicesShutdownHandle { all_services_shutdown_signal_sender }, all_services_shutdown_signal_receiver);
    }

    pub(crate) async fn shutdown(&self) -> Result<(), SendError<()>> {
        return self.all_services_shutdown_signal_sender.clone().send(()).await;
    }
}