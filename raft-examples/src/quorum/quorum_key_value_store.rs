use std::sync::Arc;

use tonic::{Request, Response, Status};

use raft::net::replica::Replica;

use crate::quorum::client::client::Client;
use crate::quorum::rpc::grpc::{GetValueByKeyRequest, GetValueByKeyResponse, CorrelatingGetValueByKeyRequest};
use crate::quorum::rpc::grpc::quorum_key_value_server::QuorumKeyValue;
use crate::quorum::server::server::Server;

pub struct QuorumKeyValueStoreService {
    client: Client,
    server: Server,
}

#[tonic::async_trait]
impl QuorumKeyValue for QuorumKeyValueStoreService {
    async fn get_by(&self, request: Request<GetValueByKeyRequest>) -> Result<Response<GetValueByKeyResponse>, Status> {
        return self.client.get_by(request.into_inner()).await;
    }

    async fn acknowledge(&self, request: Request<CorrelatingGetValueByKeyRequest>) -> Result<Response<()>, Status> {
        return self.server.acknowledge(request.into_inner()).await;
    }

    async fn accept(&self, request: Request<GetValueByKeyResponse>) -> Result<Response<()>, Status> {
        return self.server.accept(request.into_inner()).await;
    }
}

impl QuorumKeyValueStoreService {
    pub fn new(replica: Arc<Replica>) -> QuorumKeyValueStoreService {
        return QuorumKeyValueStoreService {
            client: Client::new(replica.clone()),
            server: Server::new(replica.clone()),
        };
    }
}