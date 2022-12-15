use std::sync::Arc;

use tonic::{Response, Status};

use raft::consensus::quorum::async_quorum_callback::AsyncQuorumCallback;
use raft::net::connect::correlation_id::CorrelationIdGenerator;
use raft::net::connect::random_correlation_id_generator::RandomCorrelationIdGenerator;
use raft::net::connect::service_client::ServiceRequest;
use raft::net::replica::Replica;

use crate::quorum::client_provider::CorrelatingGetValueByKeyRequestClient;
use crate::quorum::rpc::grpc::{CorrelatingGetValueByKeyRequest, GetValueByKeyRequest, GetValueByKeyResponse};

pub(crate) struct Client {
    replica: Arc<Replica>,
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
        let response = completion_response.success_responses().unwrap().as_slice().get(0).unwrap();
        return Ok(Response::new(
            GetValueByKeyResponse {
                key: response.key.clone(),
                value: response.value.clone(),
                correlation_id: response.correlation_id,
            })
        );
    }
}

impl Client {
    pub(crate) fn new(replica: Arc<Replica>) -> Client {
        return Client {
            replica
        };
    }
}