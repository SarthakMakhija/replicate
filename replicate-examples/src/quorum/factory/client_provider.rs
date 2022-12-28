use async_trait::async_trait;
use tonic::{Request, Response};

use replicate::net::connect::host_and_port::HostAndPort;
use replicate::net::connect::service_client::ServiceClientProvider;
use replicate::net::connect::error::ServiceResponseError;

use crate::quorum::rpc::grpc::CorrelatingGetValueByKeyRequest;
use crate::quorum::rpc::grpc::GetValueByKeyResponse;
use crate::quorum::rpc::grpc::VersionedPutKeyValueRequest;
use crate::quorum::rpc::grpc::PutKeyValueResponse;

use crate::quorum::rpc::grpc::quorum_key_value_client::QuorumKeyValueClient;

pub(crate) struct CorrelatingGetValueByKeyRequestClient {}

pub(crate) struct GetValueByKeyResponseClient {}

pub(crate) struct VersionedPutKeyValueRequestClient {}

pub(crate) struct PutKeyValueResponseClient {}

#[async_trait]
impl ServiceClientProvider<CorrelatingGetValueByKeyRequest, ()> for CorrelatingGetValueByKeyRequestClient {
    async fn call(&self, request: Request<CorrelatingGetValueByKeyRequest>, address: HostAndPort) -> Result<Response<()>, ServiceResponseError> {
        let mut client = QuorumKeyValueClient::connect(address.as_string_with_http()).await?;
        let response = client.acknowledge_get(request).await?;
        return Ok(response);
    }
}

#[async_trait]
impl ServiceClientProvider<GetValueByKeyResponse, ()> for GetValueByKeyResponseClient {
    async fn call(&self, request: Request<GetValueByKeyResponse>, address: HostAndPort) -> Result<Response<()>, ServiceResponseError> {
        let mut client = QuorumKeyValueClient::connect(address.as_string_with_http()).await?;
        let response = client.finish_get(request).await?;
        return Ok(response);
    }
}

#[async_trait]
impl ServiceClientProvider<VersionedPutKeyValueRequest, ()> for VersionedPutKeyValueRequestClient {
    async fn call(&self, request: Request<VersionedPutKeyValueRequest>, address: HostAndPort) -> Result<Response<()>, ServiceResponseError> {
        let mut client = QuorumKeyValueClient::connect(address.as_string_with_http()).await?;
        let response = client.acknowledge_put(request).await?;
        return Ok(response);
    }
}

#[async_trait]
impl ServiceClientProvider<PutKeyValueResponse, ()> for PutKeyValueResponseClient {
    async fn call(&self, request: Request<PutKeyValueResponse>, address: HostAndPort) -> Result<Response<()>, ServiceResponseError> {
        let mut client = QuorumKeyValueClient::connect(address.as_string_with_http()).await?;
        let response = client.finish_put(request).await?;
        return Ok(response);
    }
}
