use std::sync::Arc;

use tonic::{Request, Response, Status};

use replicate::clock::clock::{Clock, SystemClock};
use replicate::consensus::quorum::async_quorum_callback::AsyncQuorumCallback;
use replicate::net::replica::Replica;

use crate::quorum::factory::client_response::ClientResponse;
use crate::quorum::factory::service_request::ServiceRequestFactory;
use crate::quorum::read_repair::ReadRepair;
use crate::quorum::rpc::grpc::CorrelatingGetValueByKeyRequest;
use crate::quorum::rpc::grpc::GetValueByKeyRequest;
use crate::quorum::rpc::grpc::GetValueByKeyResponse;
use crate::quorum::rpc::grpc::PutKeyValueRequest;
use crate::quorum::rpc::grpc::PutKeyValueResponse;
use crate::quorum::rpc::grpc::quorum_key_value_server::QuorumKeyValue;
use crate::quorum::rpc::grpc::VersionedPutKeyValueRequest;
use crate::quorum::store::key_value_store::KeyValueStore;
use crate::quorum::store::value::Value;

pub struct QuorumKeyValueReplicaService {
    replica: Arc<Replica>,
    key_value_store: KeyValueStore,
    clock: Box<dyn Clock>,
}

#[tonic::async_trait]
impl QuorumKeyValue for QuorumKeyValueReplicaService {
    async fn get_by(&self, request: Request<GetValueByKeyRequest>) -> Result<Response<GetValueByKeyResponse>, Status> {
        let request = request.into_inner();
        println!("received a get request by the client for key {}", request.key.clone());
        let service_request_constructor = || {
            ServiceRequestFactory::correlating_get_value_by_key_request(request.key.clone())
        };

        let expected_responses = self.replica.total_peer_count();
        let async_quorum_callback = AsyncQuorumCallback::<GetValueByKeyResponse>::new(expected_responses);
        let _ = &self.replica
            .send_to_replicas(service_request_constructor, async_quorum_callback.clone())
            .await;

        let completion_response = async_quorum_callback.handle().await;
        let response_by_host = completion_response.success_response().unwrap();
        let response = ReadRepair::new(self.replica.clone(), response_by_host).attempt().await;

        return Ok(Response::new(response));
    }

    async fn put(&self, request: Request<PutKeyValueRequest>) -> Result<Response<PutKeyValueResponse>, Status> {
        let request = request.into_inner();
        println!("received a put request by the client for key {}", request.key.clone());
        let service_request_constructor = || {
            ServiceRequestFactory::versioned_put_key_value_request(
                self.clock.now_seconds(),
                request.key.clone(),
                request.value.clone(),
            )
        };

        let expected_responses = self.replica.total_peer_count();
        let async_quorum_callback = AsyncQuorumCallback::<PutKeyValueResponse>::new(expected_responses);
        let _ = &self.replica
            .send_to_replicas(service_request_constructor, async_quorum_callback.clone())
            .await;

        let completion_response = async_quorum_callback.handle().await;
        let response = completion_response.success_response().unwrap().values().next().unwrap();
        return Ok(Response::new(ClientResponse::put_key_value_response(response.was_put, response.correlation_id)));
    }

    async fn acknowledge_get(&self, request: Request<CorrelatingGetValueByKeyRequest>) -> Result<Response<()>, Status> {
        return self.key_value_store.acknowledge_get(request).await;
    }

    async fn finish_get(&self, request: Request<GetValueByKeyResponse>) -> Result<Response<()>, Status> {
        return self.key_value_store.finish_get(request).await;
    }

    async fn acknowledge_put(&self, request: Request<VersionedPutKeyValueRequest>) -> Result<Response<()>, Status> {
        return self.key_value_store.acknowledge_put(request).await;
    }

    async fn finish_put(&self, request: Request<PutKeyValueResponse>) -> Result<Response<()>, Status> {
        return self.key_value_store.finish_put(request).await;
    }
}

impl QuorumKeyValueReplicaService {
    pub fn new(replica: Arc<Replica>) -> QuorumKeyValueReplicaService {
        return QuorumKeyValueReplicaService {
            replica: replica.clone(),
            key_value_store: KeyValueStore::new(replica.clone()),
            clock: Box::new(SystemClock::new()),
        };
    }

    pub fn set_initial_state(&self, key_value: (String, Value)) {
        self.key_value_store.set_initial_state(key_value);
    }
}
