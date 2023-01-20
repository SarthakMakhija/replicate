use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use async_trait::async_trait;
use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle;
use tonic::{Request, Response};

use replicate::clock::clock::SystemClock;
use replicate::net::connect::async_network::AsyncNetwork;
use replicate::net::connect::host_and_port::HostAndPort;
use replicate::net::connect::service_client::{ServiceClientProvider, ServiceRequest};
use replicate::net::connect::error::ServiceResponseError;
use replicate::net::connect::service_registration::{AllServicesShutdownHandle, ServiceRegistration};
use replicate::net::replica::Replica;
use replicate_examples::quorum::quorum_key_value_replica::QuorumKeyValueReplicaService;
use replicate_examples::quorum::rpc::grpc::GetValueByKeyRequest;
use replicate_examples::quorum::rpc::grpc::GetValueByKeyResponse;
use replicate_examples::quorum::rpc::grpc::PutKeyValueRequest;
use replicate_examples::quorum::rpc::grpc::PutKeyValueResponse;
use replicate_examples::quorum::rpc::grpc::quorum_key_value_client::QuorumKeyValueClient;
use replicate_examples::quorum::rpc::grpc::quorum_key_value_server::QuorumKeyValueServer;
use replicate_examples::quorum::store::value::Value;

struct GetValueByKeyRequestClient {}

struct PutKeyValueRequestClient {}

#[async_trait]
impl ServiceClientProvider<GetValueByKeyRequest, GetValueByKeyResponse> for GetValueByKeyRequestClient {
    async fn call(&self, request: Request<GetValueByKeyRequest>, address: HostAndPort) -> Result<Response<GetValueByKeyResponse>, ServiceResponseError> {
        let mut client = QuorumKeyValueClient::connect(address.as_string_with_http()).await?;
        let response = client.get_by(request).await?;
        return Ok(response);
    }
}

#[async_trait]
impl ServiceClientProvider<PutKeyValueRequest, PutKeyValueResponse> for PutKeyValueRequestClient {
    async fn call(&self, request: Request<PutKeyValueRequest>, address: HostAndPort) -> Result<Response<PutKeyValueResponse>, ServiceResponseError> {
        let mut client = QuorumKeyValueClient::connect(address.as_string_with_http()).await?;
        let response = client.put(request).await?;
        return Ok(response);
    }
}

#[test]
fn put_key_value() {
    let runtime = Builder::new_multi_thread()
        .thread_name("put_key_value".to_string())
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6540);
    let peer_one = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6541);
    let peer_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6542);

    let all_services_shutdown_handle_one = spin_self(&runtime, self_host_and_port.clone(), vec![peer_one, peer_other], None);
    let all_services_shutdown_handle_two = spin_peer(&runtime, peer_one.clone(), vec![self_host_and_port, peer_other], None);
    let all_services_shutdown_handle_three = spin_other_peer(&runtime, peer_other.clone(), vec![self_host_and_port, peer_one], None);

    let_services_start();

    let key = "HDD".to_string();
    let value = "Hard disk".to_string();
    let put_handle = send_put_request(self_host_and_port, &runtime, key, value);
    let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();

    blocking_runtime.block_on(async move {
        put_handle.await.unwrap().unwrap();
    });

    let get_handle = send_get_request(self_host_and_port, &runtime, "HDD".to_string());
    blocking_runtime.block_on(async move {
        let response: GetValueByKeyResponse = get_handle.await.unwrap().unwrap();

        all_services_shutdown_handle_one.shutdown().await.unwrap();
        all_services_shutdown_handle_two.shutdown().await.unwrap();
        all_services_shutdown_handle_three.shutdown().await.unwrap();

        assert_eq!("HDD".to_string(), response.key.clone());
        assert_eq!("Hard disk".to_string(), response.value.clone());
    });
}

#[test]
fn get_value_for_non_existing_key() {
    let runtime = Builder::new_multi_thread()
        .thread_name("get_key_value".to_string())
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6550);
    let peer_one = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6551);
    let peer_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6552);

    let all_services_shutdown_handle_one = spin_self(&runtime, self_host_and_port.clone(), vec![peer_one, peer_other], None);
    let all_services_shutdown_handle_two = spin_peer(&runtime, peer_one.clone(), vec![self_host_and_port, peer_other], None);
    let all_services_shutdown_handle_three = spin_other_peer(&runtime, peer_other.clone(), vec![self_host_and_port, peer_one], None);

    let_services_start();

    let key = "non-existing".to_string();
    let get_handle = send_get_request(self_host_and_port, &runtime, key);

    let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
    blocking_runtime.block_on(async move {
        let response: GetValueByKeyResponse = get_handle.await.unwrap().unwrap();

        all_services_shutdown_handle_one.shutdown().await.unwrap();
        all_services_shutdown_handle_two.shutdown().await.unwrap();
        all_services_shutdown_handle_three.shutdown().await.unwrap();

        assert_eq!("non-existing".to_string(), response.key.clone());
        assert_eq!("".to_string(), response.value.clone());
    });
}

#[test]
fn get_latest_value_after_read_repair() {
    let runtime = Builder::new_multi_thread()
        .thread_name("read_repair_key_value".to_string())
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let self_host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6560);
    let peer_one = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6561);
    let peer_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6562);

    let initial_state = Some(("HDD".to_string(), Value::new("New Hard disk".to_string(), 200)));
    let all_services_shutdown_handle_one = spin_self(&runtime, self_host_and_port.clone(), vec![peer_one, peer_other], initial_state);

    let initial_state = Some(("HDD".to_string(), Value::new("New Hard disk".to_string(), 200)));
    let all_services_shutdown_handle_two = spin_peer(&runtime, peer_one.clone(), vec![self_host_and_port, peer_other], initial_state);

    let initial_state = Some(("HDD".to_string(), Value::new("Old Hard disk".to_string(), 100)));
    let all_services_shutdown_handle_three = spin_other_peer(&runtime, peer_other.clone(), vec![self_host_and_port, peer_one], initial_state);

    let_services_start();

    let blocking_runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let get_handle = send_get_request(self_host_and_port, &runtime, "HDD".to_string());
    blocking_runtime.block_on(async move {
        let response: GetValueByKeyResponse = get_handle.await.unwrap().unwrap();

        all_services_shutdown_handle_one.shutdown().await.unwrap();
        all_services_shutdown_handle_two.shutdown().await.unwrap();
        all_services_shutdown_handle_three.shutdown().await.unwrap();

        assert_eq!("HDD".to_string(), response.key.clone());
        assert_eq!("New Hard disk".to_string(), response.value.clone());
    });
}

fn spin_self(runtime: &Runtime, self_host_and_port: HostAndPort, peers: Vec<HostAndPort>, initial_state: Option<(String, Value)>) -> AllServicesShutdownHandle {
    let (all_services_shutdown_handle, all_services_shutdown_receiver) = AllServicesShutdownHandle::new();
    let replica = Replica::new(
        10,
        self_host_and_port.clone(),
        peers,
        Box::new(SystemClock::new()),
    );

    let store = QuorumKeyValueReplicaService::new(Arc::new(replica));
    if let Some(state) = initial_state {
        store.set_initial_state(state);
    }
    runtime.spawn(async move {
        ServiceRegistration::register_services_on(
            &self_host_and_port,
            QuorumKeyValueServer::new(store),
            all_services_shutdown_receiver,
        ).await;
    });
    all_services_shutdown_handle
}

fn spin_peer(runtime: &Runtime, self_host_and_port: HostAndPort, peers: Vec<HostAndPort>, initial_state: Option<(String, Value)>) -> AllServicesShutdownHandle {
    let (all_services_shutdown_handle, all_services_shutdown_receiver) = AllServicesShutdownHandle::new();
    let replica = Replica::new(
        20,
        self_host_and_port.clone(),
        peers,
        Box::new(SystemClock::new()),
    );

    let store = QuorumKeyValueReplicaService::new(Arc::new(replica));
    if let Some(state) = initial_state {
        store.set_initial_state(state);
    }
    runtime.spawn(async move {
        ServiceRegistration::register_services_on(
            &self_host_and_port,
            QuorumKeyValueServer::new(store),
            all_services_shutdown_receiver,
        ).await;
    });
    all_services_shutdown_handle
}

fn spin_other_peer(runtime: &Runtime, self_host_and_port: HostAndPort, peers: Vec<HostAndPort>, initial_state: Option<(String, Value)>) -> AllServicesShutdownHandle {
    let (all_services_shutdown_handle, all_services_shutdown_receiver) = AllServicesShutdownHandle::new();
    let replica = Replica::new(
        30,
        self_host_and_port.clone(),
        peers,
        Box::new(SystemClock::new()),
    );

    let store = QuorumKeyValueReplicaService::new(Arc::new(replica));
    if let Some(state) = initial_state {
        store.set_initial_state(state);
    }
    runtime.spawn(async move {
        ServiceRegistration::register_services_on(
            &self_host_and_port,
            QuorumKeyValueServer::new(store),
            all_services_shutdown_receiver,
        ).await;
    });
    all_services_shutdown_handle
}

fn send_get_request(self_host_and_port: HostAndPort, rt: &Runtime, key: String) -> JoinHandle<Result<GetValueByKeyResponse, ServiceResponseError>> {
    let any_correlation_id = 100;
    let service_request = ServiceRequest::new(
        GetValueByKeyRequest { key },
        Box::new(GetValueByKeyRequestClient {}),
        any_correlation_id,
    );

    let address = self_host_and_port.clone();
    let handle = rt.spawn(async move {
        return AsyncNetwork::send_without_source_footprint(service_request, address).await;
    });
    return handle;
}

fn send_put_request(self_host_and_port: HostAndPort, rt: &Runtime, key: String, value: String) -> JoinHandle<Result<PutKeyValueResponse, ServiceResponseError>> {
    let any_correlation_id = 100;
    let service_request = ServiceRequest::new(
        PutKeyValueRequest { key, value },
        Box::new(PutKeyValueRequestClient {}),
        any_correlation_id,
    );

    let address = self_host_and_port.clone();
    let handle = rt.spawn(async move {
        return AsyncNetwork::send_without_source_footprint(service_request, address).await;
    });
    return handle;
}

fn let_services_start() {
    thread::sleep(Duration::from_secs(4));
}
