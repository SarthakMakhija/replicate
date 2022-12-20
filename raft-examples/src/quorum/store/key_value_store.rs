use std::sync::Arc;

use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use tonic::{Request, Response, Status};

use raft::net::connect::async_network::AsyncNetwork;
use raft::net::connect::headers::HostAndPortExtractor;
use raft::net::replica::Replica;

use crate::quorum::factory::client_response::ClientResponse;
use crate::quorum::factory::service_request::ServiceRequestFactory;
use crate::quorum::rpc::grpc::CorrelatingGetValueByKeyRequest;
use crate::quorum::rpc::grpc::GetValueByKeyResponse;
use crate::quorum::rpc::grpc::PutKeyValueResponse;
use crate::quorum::rpc::grpc::VersionedPutKeyValueRequest;
use crate::quorum::store::value::Value;

pub(crate) struct KeyValueStore {
    replica: Arc<Replica>,
    storage: Arc<DashMap<String, Value>>,
}

impl KeyValueStore {
    pub(crate) fn set_initial_state(&self, key_value: (String, Value)) {
        self.storage.clone().insert(key_value.0, key_value.1);
    }

    pub(crate) async fn acknowledge_get(&self, request: Request<CorrelatingGetValueByKeyRequest>) -> Result<Response<()>, Status> {
        let originating_host_port = request.try_referral_host_port().unwrap();
        let request = request.into_inner();
        println!("Received a correlating get request for key {}", request.key.clone());

        let key = request.key;
        let correlation_id = request.correlation_id;
        let storage = self.storage.clone();
        let source_address = self.replica.clone().get_self_address();

        let handler = async move {
            let value: Option<Ref<String, Value>> = storage.get(&key);
            let response = match value {
                None =>
                    ClientResponse::get_value_by_key_response_using_empty_value(key.clone(), correlation_id),
                Some(value_ref) =>
                    ClientResponse::get_value_by_key_response_using_value_ref(
                        key.clone(),
                        value_ref.value(),
                        correlation_id,
                    )
            };
            AsyncNetwork::send_with_source_footprint(
                ServiceRequestFactory::get_value_by_key_response(correlation_id, response),
                source_address,
                originating_host_port,
            ).await.unwrap();
        };
        let _ = &self.replica.add_to_queue(handler);
        return Ok(Response::new(()));
    }

    pub(crate) async fn finish_get(&self, request: Request<GetValueByKeyResponse>) -> Result<Response<()>, Status> {
        let originating_host_port = request.try_referral_host_port().unwrap();
        let response = request.into_inner();
        println!("Received a response for key {}", response.key.clone());

        let _ = &self.replica.register_response(response.correlation_id, originating_host_port, Ok(Box::new(response)));
        return Ok(Response::new(()));
    }

    pub(crate) async fn acknowledge_put(&self, request: Request<VersionedPutKeyValueRequest>) -> Result<Response<()>, Status> {
        let originating_host_port = request.try_referral_host_port().unwrap();
        let request = request.into_inner();
        println!("Received a versioned put request for key {} with timestamp {}", request.key.clone(), request.timestamp);

        let correlation_id = request.correlation_id;
        let storage = self.storage.clone();
        let source_address = self.replica.clone().get_self_address();

        let handler = async move {
            let key = request.key.clone();
            storage.insert(key, Value::new(request.value.clone(), request.timestamp));

            AsyncNetwork::send_with_source_footprint(
                ServiceRequestFactory::put_key_value_response(
                    correlation_id
                ),
                source_address,
                originating_host_port,
            ).await.unwrap();
        };
        let _ = &self.replica.add_to_queue(handler);
        return Ok(Response::new(()));
    }

    pub(crate) async fn finish_put(&self, request: Request<PutKeyValueResponse>) -> Result<Response<()>, Status> {
        let originating_host_port = request.try_referral_host_port().unwrap();
        let response = request.into_inner();
        println!("Received a put response {}", response.was_put);

        let _ = &self.replica.register_response(response.correlation_id, originating_host_port, Ok(Box::new(response)));
        return Ok(Response::new(()));
    }
}

impl KeyValueStore {
    pub(crate) fn new(replica: Arc<Replica>) -> KeyValueStore {
        return KeyValueStore {
            replica,
            storage: Arc::new(DashMap::new()),
        };
    }
}