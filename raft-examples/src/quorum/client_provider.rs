use async_trait::async_trait;
use tonic::{Request, Response};
use raft::net::connect::host_and_port::HostAndPort;
use raft::net::connect::service_client::{ServiceClientProvider, ServiceResponseError};

use crate::quorum::rpc::grpc::{VersionedGetValueByKeyRequest, GetValueByKeyResponse};
use crate::quorum::rpc::grpc::quorum_key_value_client::QuorumKeyValueClient;

pub(crate) struct VersionedGetValueByKeyRequestClient {}
pub(crate) struct GetValueByKeyResponseClient {}


#[async_trait]
impl ServiceClientProvider<VersionedGetValueByKeyRequest, ()> for VersionedGetValueByKeyRequestClient {
    async fn call(&self, request: VersionedGetValueByKeyRequest, address: HostAndPort) -> Result<Response<()>, ServiceResponseError> {
        let mut client = QuorumKeyValueClient::connect(address.as_string_with_http()).await?;
        let request = Request::new(request);
        let response = client.acknowledge(request).await?;
        return Ok(response);
    }
}

#[async_trait]
impl ServiceClientProvider<GetValueByKeyResponse, ()> for GetValueByKeyResponseClient {
    async fn call(&self, request: GetValueByKeyResponse, address: HostAndPort) -> Result<Response<()>, ServiceResponseError> {
        let mut client = QuorumKeyValueClient::connect(address.as_string_with_http()).await?;
        let request = Request::new(request);
        let response = client.accept(request).await?;
        return Ok(response);
    }
}
