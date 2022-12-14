use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use async_trait::async_trait;
use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle;
use tonic::{Request, Response};

use raft::clock::clock::SystemClock;
use raft::net::connect::async_network::AsyncNetwork;
use raft::net::connect::host_and_port::HostAndPort;
use raft::net::connect::service_client::{ServiceClientProvider, ServiceRequest, ServiceResponseError};
use raft::net::connect::service_registration::{AllServicesShutdownHandle, ServiceRegistration};
use raft::net::replica::Replica;
use raft_examples::quorum::quorum_key_value_store::QuorumKeyValueStoreService;
use raft_examples::quorum::rpc::grpc::{GetValueByKeyRequest, GetValueByKeyResponse};
use raft_examples::quorum::rpc::grpc::quorum_key_value_client::QuorumKeyValueClient;
use raft_examples::quorum::rpc::grpc::quorum_key_value_server::QuorumKeyValueServer;

struct GetValueByKeyRequestClient {}

#[async_trait]
impl ServiceClientProvider<GetValueByKeyRequest, GetValueByKeyResponse> for GetValueByKeyRequestClient {
    async fn call(&self, request: GetValueByKeyRequest, address: HostAndPort) -> Result<Response<GetValueByKeyResponse>, ServiceResponseError> {
        let mut client = QuorumKeyValueClient::connect(address.as_string_with_http()).await?;
        let request = Request::new(request);
        let response = client.get_by(request).await?;
        return Ok(response);
    }
}

#[test]
fn get_value_by_key() {
    let runtime = Builder::new_multi_thread()
        .thread_name("get_value_by_key".to_string())
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9090);
    let peer_one = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9091);
    let peer_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9092);

    let replica = Replica::new(
        String::from("neptune"),
        self_host_and_port.clone(),
        vec![peer_one.clone(), peer_other.clone()],
        Arc::new(SystemClock::new()),
    );
    let replica = Arc::new(replica);
    let all_services_shutdown_handle_one = spin_self(&runtime, self_host_and_port.clone(), replica.clone());
    let all_services_shutdown_handle_two = spin_peer(&runtime, peer_one.clone(), replica.clone());
    let all_services_shutdown_handle_three = spin_other_peer(&runtime, peer_other.clone(), replica.clone());

    let_services_start();

    let handle = send_get_request(self_host_and_port, &runtime);
    let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();

    blocking_runtime.block_on(async move {
        let response: GetValueByKeyResponse = handle.await.unwrap().unwrap();

        all_services_shutdown_handle_one.shutdown().await.unwrap();
        all_services_shutdown_handle_two.shutdown().await.unwrap();
        all_services_shutdown_handle_three.shutdown().await.unwrap();

        assert_eq!("ok".to_string(), response.key.clone());
    });
}

fn spin_self(runtime: &Runtime, self_host_and_port: HostAndPort, replica: Arc<Replica>) -> AllServicesShutdownHandle {
    let (all_services_shutdown_handle, all_services_shutdown_receiver) = AllServicesShutdownHandle::new();
    let store = QuorumKeyValueStoreService::new(replica);
    runtime.spawn(async move {
        ServiceRegistration::register_services_on(
            &self_host_and_port,
            QuorumKeyValueServer::new(store),
            all_services_shutdown_receiver,
        ).await;
    });
    all_services_shutdown_handle
}

fn spin_peer(runtime: &Runtime, peer_one: HostAndPort, replica: Arc<Replica>) -> AllServicesShutdownHandle {
    let (all_services_shutdown_handle, all_services_shutdown_receiver) = AllServicesShutdownHandle::new();
    let store = QuorumKeyValueStoreService::new(replica);
    runtime.spawn(async move {
        ServiceRegistration::register_services_on(
            &peer_one,
            QuorumKeyValueServer::new(store),
            all_services_shutdown_receiver,
        ).await;
    });
    all_services_shutdown_handle
}

fn spin_other_peer(runtime: &Runtime, peer_other: HostAndPort, replica: Arc<Replica>) -> AllServicesShutdownHandle {
    let (all_services_shutdown_handle, all_services_shutdown_receiver) = AllServicesShutdownHandle::new();
    let store = QuorumKeyValueStoreService::new(replica);
    let service = QuorumKeyValueServer::new(store);
    runtime.spawn(async move {
        ServiceRegistration::register_services_on(
            &peer_other,
            service,
            all_services_shutdown_receiver,
        ).await;
    });
    all_services_shutdown_handle
}

fn send_get_request(self_host_and_port: HostAndPort, rt: &Runtime) -> JoinHandle<Result<GetValueByKeyResponse, ServiceResponseError>> {
    let any_correlation_id = 100;
    let service_request = ServiceRequest::new(
        GetValueByKeyRequest { key: "ok".to_string() },
        Box::new(GetValueByKeyRequestClient {}),
        any_correlation_id,
    );

    let address = self_host_and_port.clone();
    let handle = rt.spawn(async move {
        return AsyncNetwork::send(service_request, address).await;
    });
    return handle;
}

fn let_services_start() {
    thread::sleep(Duration::from_secs(4));
}
