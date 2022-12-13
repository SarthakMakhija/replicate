use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use raft::heartbeat::builtin_heartbeat_sender::BuiltinHeartbeatSender;
use raft::heartbeat::heartbeat_sender::HeartbeatSender;
use raft::net::connect::host_and_port::HostAndPort;
use raft::net::connect::service_registration::{AllServicesShutdownHandle, ServiceRegistration};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn builtin_heartbeat_sender_with_success() {
    let server_address = Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051));
    let server_address_clone = server_address.clone();

    let (all_services_shutdown_handle, all_services_shutdown_receiver) = AllServicesShutdownHandle::new();
    let server_handle = tokio::spawn(async move {
        ServiceRegistration::register_default_services_on(&server_address_clone, all_services_shutdown_receiver).await;
    });

    thread::sleep(Duration::from_secs(3));

    let heartbeat_sender = Arc::new(BuiltinHeartbeatSender::new(server_address.clone()));
    let result = heartbeat_sender.send().await;

    assert!(result.is_ok());

    all_services_shutdown_handle.shutdown().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn builtin_heartbeat_sender_with_failure_without_services_running() {
    let any_server_address = Arc::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50091));
    let heartbeat_sender = Arc::new(BuiltinHeartbeatSender::new(any_server_address.clone()));
    let result = heartbeat_sender.send().await;

    assert!(result.is_err());
}