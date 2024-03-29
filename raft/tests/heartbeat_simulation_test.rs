use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tokio::runtime::{Builder, Runtime};

use raft::election::election::Election;
use raft::heartbeat_config::HeartbeatConfig;
use raft::net::rpc::grpc::raft_server::RaftServer;
use raft::net::service::raft_service::RaftService;
use raft::state::{ReplicaRole, State};
use replicate::clock::clock::SystemClock;
use replicate::net::connect::host_and_port::HostAndPort;
use replicate::net::connect::service_registration::{AllServicesShutdownHandle, ServiceRegistration};
use replicate::net::peers::Peer;
use replicate::net::replica::Replica;

#[test]
#[cfg(feature = "test_type_simulation")]
fn send_heartbeat_with_leader_getting_completely_disconnected_results_in_new_leader() {
    let runtime = Builder::new_multi_thread()
        .thread_name("heartbeat".to_string())
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3109);
    let peer_one = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3119);
    let peer_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3129);

    let (all_services_shutdown_handle_one, state) = spin_self(&runtime, self_host_and_port.clone(), vec![peer_one, peer_other]);
    let (all_services_shutdown_handle_two, state_peer_one) = spin_peer(&runtime, peer_one.clone(), vec![self_host_and_port, peer_other]);
    let (all_services_shutdown_handle_three, state_peer_other) = spin_other_peer(&runtime, peer_other.clone(), vec![self_host_and_port, peer_one]);
    let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();

    let election = Election::new();
    blocking_runtime.block_on(async {
        election.start(state.clone()).await;
    });

    thread::sleep(Duration::from_millis(30));
    assert_eq!(ReplicaRole::Leader, state.get_role());

    //allow heartbeat exchange
    thread::sleep(Duration::from_millis(50));

    //disconnect the leader completely
    state.get_replica_reference().drop_requests_to(Peer::new(peer_one.clone()));
    state.get_replica_reference().drop_requests_to(Peer::new(peer_other.clone()));
    state_peer_one.get_replica_reference().drop_requests_to(Peer::new(self_host_and_port.clone()));
    state_peer_other.get_replica_reference().drop_requests_to(Peer::new(self_host_and_port.clone()));

    blocking_runtime.block_on(async move {
        thread::sleep(Duration::from_secs(1));

        all_services_shutdown_handle_one.shutdown().await.unwrap();
        all_services_shutdown_handle_two.shutdown().await.unwrap();
        all_services_shutdown_handle_three.shutdown().await.unwrap();

        assert!(state_peer_one.get_role() == ReplicaRole::Leader ||
            state_peer_other.get_role() == ReplicaRole::Leader
        );
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

    let state = runtime.block_on(async move {
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
    (all_services_shutdown_handle, state.clone())
}

fn spin_peer(runtime: &Runtime, self_host_and_port: HostAndPort, peers: Vec<HostAndPort>) -> (AllServicesShutdownHandle, Arc<State>) {
    let (all_services_shutdown_handle, all_services_shutdown_receiver) = AllServicesShutdownHandle::new();
    let replica = Replica::new(
        20,
        self_host_and_port.clone(),
        peers,
        Box::new(SystemClock::new()),
    );
    let state = runtime.block_on(async move {
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
    (all_services_shutdown_handle, state.clone())
}

fn spin_other_peer(runtime: &Runtime, self_host_and_port: HostAndPort, peers: Vec<HostAndPort>) -> (AllServicesShutdownHandle, Arc<State>) {
    let (all_services_shutdown_handle, all_services_shutdown_receiver) = AllServicesShutdownHandle::new();
    let replica = Replica::new(
        30,
        self_host_and_port.clone(),
        peers,
        Box::new(SystemClock::new()),
    );

    let state = runtime.block_on(async move {
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
    (all_services_shutdown_handle, state.clone())
}
