use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tokio::runtime::{Builder, Runtime};
use tonic::{Code, Request, Status};
use raft::heartbeat_config::HeartbeatConfig;

use raft::net::factory::client_provider::{RequestVoteClient, RequestVoteResponseClient};
use raft::net::rpc::grpc::raft_server::RaftServer;
use raft::net::service::raft_service::RaftService;
use raft::net::rpc::grpc::{RequestVote, RequestVoteResponse};
use raft::state::State;
use replicate::clock::clock::SystemClock;
use replicate::net::connect::host_and_port::HostAndPort;
use replicate::net::connect::service_client::ServiceClientProvider;
use replicate::net::connect::service_registration::{AllServicesShutdownHandle, ServiceRegistration};
use replicate::net::replica::Replica;

#[test]
fn request_vote_client_with_missing_host_port_in_request() {
    let runtime = Builder::new_multi_thread()
        .thread_name("request_vote_client_with_missing_host_port_in_request".to_string())
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9960);
    let peer_one = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9961);

    let (all_services_shutdown_handle_one, _) = spin_self(&runtime, self_host_and_port.clone(), vec![peer_one]);

    let_services_start();

    let client = RequestVoteClient {};
    let request = Request::new(RequestVote { term: 1, replica_id: 10, correlation_id: 10 });

    let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
    blocking_runtime.block_on(async move {
        let result = client.call(request, self_host_and_port).await;
        assert!(result.is_err());

        let result = result.unwrap_err().downcast::<Status>();
        assert!(result.is_ok());
        assert_eq!(Code::FailedPrecondition, result.unwrap().code());

        all_services_shutdown_handle_one.shutdown().await.unwrap();
    });
}

#[test]
fn request_vote_response_client_with_missing_host_port_in_request() {
    let runtime = Builder::new_multi_thread()
        .thread_name("request_vote_response_client_with_missing_host_port_in_request".to_string())
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3838);
    let peer_one = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3839);

    let (all_services_shutdown_handle_one, _) = spin_self(&runtime, self_host_and_port.clone(), vec![peer_one]);

    let_services_start();

    let client = RequestVoteResponseClient {};
    let request = Request::new(RequestVoteResponse { term: 1, voted: true, correlation_id: 10 });

    let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
    blocking_runtime.block_on(async move {
        let result = client.call(request, self_host_and_port).await;
        assert!(result.is_err());

        let result = result.unwrap_err().downcast::<Status>();
        assert!(result.is_ok());
        assert_eq!(Code::FailedPrecondition, result.unwrap().code());

        all_services_shutdown_handle_one.shutdown().await.unwrap();
    });
}


fn spin_self(runtime: &Runtime, self_host_and_port: HostAndPort, peers: Vec<HostAndPort>) -> (AllServicesShutdownHandle, Arc<State>) {
    let (all_services_shutdown_handle, all_services_shutdown_receiver) = AllServicesShutdownHandle::new();
    let replica = Replica::new(
        10,
        self_host_and_port.clone(),
        peers,
        Arc::new(SystemClock::new()),
    );

    let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let state = blocking_runtime.block_on(async move {
        return State::new(Arc::new(replica), HeartbeatConfig::default());
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

fn let_services_start() {
    thread::sleep(Duration::from_secs(4));
}