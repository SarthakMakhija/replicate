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
use replicate::net::replica::Replica;

#[test]
fn start_elections_with_new_term() {
    let runtime = Builder::new_multi_thread()
        .thread_name("start_election".to_string())
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3560);
    let peer_one = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3561);
    let peer_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3562);

    let (all_services_shutdown_handle_one, state) = spin_self(&runtime, self_host_and_port.clone(), vec![peer_one, peer_other]);
    let (all_services_shutdown_handle_two, _) = spin_peer(&runtime, peer_one.clone(), vec![self_host_and_port, peer_other]);
    let (all_services_shutdown_handle_three, _) = spin_other_peer(&runtime, peer_other.clone(), vec![self_host_and_port, peer_one]);

    let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();

    let election = Election::new(state.clone());
    blocking_runtime.block_on(async {
        election.start().await;
    });

    thread::sleep(Duration::from_millis(10));
    assert_eq!(1, state.get_term());

    blocking_runtime.block_on(async {
        election.start().await;
    });

    thread::sleep(Duration::from_millis(20));
    assert_eq!(2, state.get_term());
    assert_eq!(ReplicaRole::Leader, state.get_role());

    blocking_runtime.block_on(async move {
        all_services_shutdown_handle_one.shutdown().await.unwrap();
        all_services_shutdown_handle_two.shutdown().await.unwrap();
        all_services_shutdown_handle_three.shutdown().await.unwrap();
    });
}

#[test]
fn elect_a_leader() {
    let runtime = Builder::new_multi_thread()
        .thread_name("elect_a_leader".to_string())
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2990);
    let peer_one = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2991);
    let peer_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2992);

    let (all_services_shutdown_handle_one, state) = spin_self(&runtime, self_host_and_port.clone(), vec![peer_one, peer_other]);
    let (all_services_shutdown_handle_two, state_peer_one) = spin_peer(&runtime, peer_one.clone(), vec![self_host_and_port, peer_other]);
    let (all_services_shutdown_handle_three, state_peer_two) = spin_other_peer(&runtime, peer_other.clone(), vec![self_host_and_port, peer_one]);

    thread::sleep(Duration::from_secs(2));

    let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
    blocking_runtime.block_on(async move {
        all_services_shutdown_handle_one.shutdown().await.unwrap();
        all_services_shutdown_handle_two.shutdown().await.unwrap();
        all_services_shutdown_handle_three.shutdown().await.unwrap();
    });

    assert!(state.get_role() == ReplicaRole::Leader ||
        state_peer_one.get_role() == ReplicaRole::Leader ||
        state_peer_two.get_role() == ReplicaRole::Leader
    );

    let mut leader_count = 0;
    if state.get_role() == ReplicaRole::Leader {
        leader_count = leader_count + 1
    }
    if state_peer_one.get_role() == ReplicaRole::Leader {
        leader_count = leader_count + 1
    }
    if state_peer_two.get_role() == ReplicaRole::Leader {
        leader_count = leader_count + 1
    }

    assert_eq!(1, leader_count);
}

fn spin_self(runtime: &Runtime, self_host_and_port: HostAndPort, peers: Vec<HostAndPort>) -> (AllServicesShutdownHandle, Arc<State>) {
    let (all_services_shutdown_handle, all_services_shutdown_receiver) = AllServicesShutdownHandle::new();
    let replica = Replica::new(
        10,
        self_host_and_port.clone(),
        peers,
        Arc::new(SystemClock::new()),
    );

    let state = runtime.block_on(async move {
        return State::new(Arc::new(replica), HeartbeatConfig::default());
    });
    let inner_state = state.clone();
    runtime.spawn(async move {
        ServiceRegistration::register_services_on(
            &self_host_and_port,
            RaftServer::new(RaftService::new(inner_state, Arc::new(SystemClock::new()))),
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
        Arc::new(SystemClock::new()),
    );
    let state = runtime.block_on(async move {
        return State::new(Arc::new(replica), HeartbeatConfig::default());
    });
    let inner_state = state.clone();
    runtime.spawn(async move {
        ServiceRegistration::register_services_on(
            &self_host_and_port,
            RaftServer::new(RaftService::new(inner_state, Arc::new(SystemClock::new()))),
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
        Arc::new(SystemClock::new()),
    );

    let state = runtime.block_on(async move {
        return State::new(Arc::new(replica), HeartbeatConfig::default());
    });
    let inner_state = state.clone();
    runtime.spawn(async move {
        ServiceRegistration::register_services_on(
            &self_host_and_port,
            RaftServer::new(RaftService::new(inner_state, Arc::new(SystemClock::new()))),
            all_services_shutdown_receiver,
        ).await;
    });
    (all_services_shutdown_handle, state.clone())
}
