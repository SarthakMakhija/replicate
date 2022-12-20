use std::sync::Arc;
use tonic::{Response, Status};

use raft::clock::clock::{Clock, SystemClock};
use raft::consensus::quorum::async_quorum_callback::AsyncQuorumCallback;
use raft::net::connect::correlation_id::CorrelationIdGenerator;
use raft::net::connect::random_correlation_id_generator::RandomCorrelationIdGenerator;
use raft::net::connect::service_client::ServiceRequest;
use raft::net::replica::Replica;

use crate::quorum::client_provider::CorrelatingGetValueByKeyRequestClient;
use crate::quorum::client_provider::VersionedPutKeyValueRequestClient;
use crate::quorum::read_repair::ReadRepair;

use crate::quorum::rpc::grpc::CorrelatingGetValueByKeyRequest;
use crate::quorum::rpc::grpc::GetValueByKeyRequest;
use crate::quorum::rpc::grpc::GetValueByKeyResponse;
use crate::quorum::rpc::grpc::PutKeyValueRequest;
use crate::quorum::rpc::grpc::VersionedPutKeyValueRequest;
use crate::quorum::rpc::grpc::PutKeyValueResponse;

pub(crate) struct Client {
    replica: Arc<Replica>,
    clock: Box<dyn Clock>,
}

impl Client {
    pub(crate) async fn get_by(&self, request: GetValueByKeyRequest) -> Result<Response<GetValueByKeyResponse>, Status> {
        println!("received a get request by the client for key {}", request.key.clone());
        let correlation_id_generator = RandomCorrelationIdGenerator::new();
        let service_request_constructor = || {
            let correlation_id = correlation_id_generator.generate();
            ServiceRequest::new(
                CorrelatingGetValueByKeyRequest {
                    key: request.key.clone(),
                    correlation_id,
                },
                Box::new(CorrelatingGetValueByKeyRequestClient {}),
                correlation_id,
            )
        };

        let expected_responses = self.replica.total_peer_count();
        let async_quorum_callback = AsyncQuorumCallback::<GetValueByKeyResponse>::new(expected_responses);
        let _ = &self.replica
            .send_one_way_to_replicas(service_request_constructor, async_quorum_callback.clone())
            .await;

        let completion_response = async_quorum_callback.handle().await;
        let response_by_host = completion_response.success_responses().unwrap();
        println!("response by host {:?}", response_by_host);
        let read_repair = ReadRepair::new(self.replica.clone(), response_by_host);
        let response = read_repair.repair().await;

        return Ok(Response::new(response));
    }

    pub(crate) async fn put(&self, request: PutKeyValueRequest) -> Result<Response<PutKeyValueResponse>, Status> {
        println!("received a put request by the client for key {}", request.key.clone());
        let correlation_id_generator = RandomCorrelationIdGenerator::new();
        let service_request_constructor = || {
            let correlation_id = correlation_id_generator.generate();
            ServiceRequest::new(
                VersionedPutKeyValueRequest {
                    key: request.key.clone(),
                    value: request.value.clone(),
                    timestamp: self.clock.now_seconds(),
                    correlation_id,
                },
                Box::new(VersionedPutKeyValueRequestClient {}),
                correlation_id,
            )
        };

        let expected_responses = self.replica.total_peer_count();
        let async_quorum_callback = AsyncQuorumCallback::<PutKeyValueResponse>::new(expected_responses);
        let _ = &self.replica
            .send_one_way_to_replicas(service_request_constructor, async_quorum_callback.clone())
            .await;

        let completion_response = async_quorum_callback.handle().await;
        let response = completion_response.success_responses().unwrap().values().next().unwrap();
        return Ok(Response::new(
            PutKeyValueResponse {
                was_put: response.was_put,
                correlation_id: response.correlation_id
            })
        );
    }
}

impl Client {
    pub(crate) fn new(replica: Arc<Replica>) -> Client {
        let clock = SystemClock::new();
        return Client {
            replica,
            clock: Box::new(clock),
        };
    }
}