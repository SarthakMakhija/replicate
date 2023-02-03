use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use raft::heartbeat_config::HeartbeatConfig;
use raft::net::service::raft_service::RaftService;
use raft::state::State;
use replicate::clock::clock::SystemClock;
use replicate::net::connect::host_and_port::HostAndPort;
use replicate::net::connect::service_registration::{AllServicesShutdownHandle, ServiceRegistration};
use replicate::net::replica::Replica;
use raft::net::rpc::grpc::raft_server::RaftServer;

#[test]
#[cfg(feature = "test_type_integration")]
fn send_heartbeats_to_followers() {
    let runtime = Builder::new_multi_thread()
        .thread_name("send_heartbeats".to_string())
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1560);
    let peer_one = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1561);
    let peer_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1562);

    let (all_services_shutdown_handle_one,state) = spin_self(&runtime, self_host_and_port.clone(), vec![peer_one, peer_other]);
    let all_services_shutdown_handle_two = spin_peer(&runtime, peer_one.clone(), vec![self_host_and_port, peer_other]);
    let all_services_shutdown_handle_three = spin_other_peer(&runtime, peer_other.clone(), vec![self_host_and_port, peer_one]);

    let_services_start();

    let blocking_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    blocking_runtime.block_on(async move {
        let result = state.get_heartbeat_sender().await;
        assert!(result.is_ok());

        let _ = all_services_shutdown_handle_one.shutdown().await;
        let _ = all_services_shutdown_handle_two.shutdown().await;
        let _ = all_services_shutdown_handle_three.shutdown().await;
    });
}

fn spin_self(runtime: &Runtime, self_host_and_port: HostAndPort, peers: Vec<HostAndPort>) -> (AllServicesShutdownHandle, Arc<State>) {
    let (all_services_shutdown_handle, all_services_shutdown_receiver) = AllServicesShutdownHandle::new();
    let replica = Replica::new(
        10,
        self_host_and_port.clone(),
        peers,
        Box::new(SystemClock::new()),
    );

    let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let state = blocking_runtime.block_on(async move {
        return State::new(replica, HeartbeatConfig::default());
    });
    let inner_state = state.clone();
    runtime.spawn(async move {
        ServiceRegistration::register_services_on(
            &self_host_and_port,
            RaftServer::new(RaftService::new(inner_state)),
            all_services_shutdown_receiver,
        ).await;
    });
    (all_services_shutdown_handle, state)
}

fn spin_peer(runtime: &Runtime, self_host_and_port: HostAndPort, peers: Vec<HostAndPort>) -> AllServicesShutdownHandle {
    let (all_services_shutdown_handle, all_services_shutdown_receiver) = AllServicesShutdownHandle::new();
    let replica = Replica::new(
        20,
        self_host_and_port.clone(),
        peers,
        Box::new(SystemClock::new()),
    );

    let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let state = blocking_runtime.block_on(async move {
        return State::new(replica, HeartbeatConfig::default());
    });
    runtime.spawn(async move {
        ServiceRegistration::register_services_on(
            &self_host_and_port,
            RaftServer::new(RaftService::new(state)),
            all_services_shutdown_receiver,
        ).await;
    });
    all_services_shutdown_handle
}

fn spin_other_peer(runtime: &Runtime, self_host_and_port: HostAndPort, peers: Vec<HostAndPort>) -> AllServicesShutdownHandle {
    let (all_services_shutdown_handle, all_services_shutdown_receiver) = AllServicesShutdownHandle::new();
    let replica = Replica::new(
        30,
        self_host_and_port.clone(),
        peers,
        Box::new(SystemClock::new()),
    );

    let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let state = blocking_runtime.block_on(async move {
        return State::new(replica, HeartbeatConfig::default());
    });
    runtime.spawn(async move {
        ServiceRegistration::register_services_on(
            &self_host_and_port,
            RaftServer::new(RaftService::new(state)),
            all_services_shutdown_receiver,
        ).await;
    });
    all_services_shutdown_handle
}

fn let_services_start() {
    thread::sleep(Duration::from_secs(4));
}