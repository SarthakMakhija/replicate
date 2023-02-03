use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tokio::runtime::{Builder, Runtime};
use tonic::{Request, Response};

use raft::election::election::Election;
use raft::heartbeat_config::HeartbeatConfig;
use raft::net::rpc::grpc::Command;
use raft::net::rpc::grpc::raft_client::RaftClient;
use raft::net::rpc::grpc::raft_server::RaftServer;
use raft::net::service::raft_service::RaftService;
use raft::state::{ReplicaRole, State};
use replicate::clock::clock::SystemClock;
use replicate::net::connect::error::ServiceResponseError;
use replicate::net::connect::host_and_port::HostAndPort;
use replicate::net::connect::service_registration::{AllServicesShutdownHandle, ServiceRegistration};
use replicate::net::replica::Replica;

#[test]
#[cfg(feature = "test_type_integration")]
fn replicate_log() {
    let runtime = Builder::new_multi_thread()
        .thread_name("replicate_log".to_string())
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7191);
    let peer_one = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7192);
    let peer_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7193);

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

    let content = String::from("replicate");

    blocking_runtime.block_on(async {
        send_commands(
            self_host_and_port,
            vec![Command { command: content.as_bytes().to_vec() }],
        ).await.unwrap();
    });

    blocking_runtime.block_on(async move {
        thread::sleep(Duration::from_millis(30));

        assert_eq!(1, state.get_replicated_log_reference().total_log_entries());
        assert_eq!(1, state_peer_one.get_replicated_log_reference().total_log_entries());
        assert_eq!(1, state_peer_other.get_replicated_log_reference().total_log_entries());

        assert_eq!(content.as_bytes().to_vec(), state_peer_one.get_replicated_log_reference().get_log_entry_at(0).unwrap().get_bytes_as_vec());
        assert_eq!(content.as_bytes().to_vec(), state_peer_other.get_replicated_log_reference().get_log_entry_at(0).unwrap().get_bytes_as_vec());
        assert!(state.get_replicated_log_reference().get_log_entry_at(0).unwrap().get_acknowledgements() >= 2);

        all_services_shutdown_handle_one.shutdown().await.unwrap();
        all_services_shutdown_handle_two.shutdown().await.unwrap();
        all_services_shutdown_handle_three.shutdown().await.unwrap();
    });
}

#[test]
#[cfg(feature = "test_type_integration")]
fn replicate_multiple_logs_sequentially() {
    let runtime = Builder::new_multi_thread()
        .thread_name("replicate_multiple_logs_sequentially".to_string())
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3190);
    let peer_one = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3191);
    let peer_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3192);

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

    let content_replicate = String::from("replicate");
    let content_raft = String::from("raft");
    let content_log = String::from("log");

    blocking_runtime.block_on(async {
        send_commands(
            self_host_and_port,
            vec![
                Command { command: content_replicate.as_bytes().to_vec() },
                Command { command: content_raft.as_bytes().to_vec() },
                Command { command: content_log.as_bytes().to_vec() },
            ]
        ).await.unwrap();
    });

    blocking_runtime.block_on(async move {
        thread::sleep(Duration::from_millis(30));

        for state in vec![&state, &state_peer_one, &state_peer_other] {
            assert_eq!(3, state.get_replicated_log_reference().total_log_entries());

            assert_eq!(content_replicate.as_bytes().to_vec(),
                       state.get_replicated_log_reference().get_log_entry_at(0).unwrap().get_bytes_as_vec()
            );
            assert_eq!(content_raft.as_bytes().to_vec(),
                       state.get_replicated_log_reference().get_log_entry_at(1).unwrap().get_bytes_as_vec()
            );
            assert_eq!(content_log.as_bytes().to_vec(),
                       state.get_replicated_log_reference().get_log_entry_at(2).unwrap().get_bytes_as_vec()
            );
        }

        assert_eq!(Some(2), state.get_replicated_log_reference().get_commit_index());
        assert_eq!(Some(1), state_peer_one.get_replicated_log_reference().get_commit_index());
        assert_eq!(Some(1), state_peer_other.get_replicated_log_reference().get_commit_index());

        all_services_shutdown_handle_one.shutdown().await.unwrap();
        all_services_shutdown_handle_two.shutdown().await.unwrap();
        all_services_shutdown_handle_three.shutdown().await.unwrap();
    });
}

async fn send_commands(address: HostAndPort, commands: Vec<Command>) -> Result<Response<()>, ServiceResponseError> {
    let mut client = RaftClient::connect(address.as_string_with_http()).await?;
    for command in commands {
        client.execute(Request::new(command)).await?;
        thread::sleep(Duration::from_millis(60));
    }
    return Ok(Response::new(()));
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
