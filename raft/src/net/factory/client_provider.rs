use async_trait::async_trait;
use tonic::{Request, Response};

use replicate::net::connect::host_and_port::HostAndPort;
use replicate::net::connect::service_client::{ServiceClientProvider, ServiceResponseError};

use crate::net::rpc::grpc::RequestVote;
use crate::net::rpc::grpc::RequestVoteResponse;
use crate::net::rpc::grpc::AppendEntries;
use crate::net::rpc::grpc::AppendEntriesResponse;
use crate::net::rpc::grpc::raft_client::RaftClient;

pub(crate) struct RequestVoteClient {}

pub(crate) struct RequestVoteResponseClient {}

pub(crate) struct RaftHeartbeatServiceClient {}

#[async_trait]
impl ServiceClientProvider<RequestVote, ()> for RequestVoteClient {
    async fn call(&self, request: Request<RequestVote>, address: HostAndPort) -> Result<Response<()>, ServiceResponseError> {
        let mut client = RaftClient::connect(address.as_string_with_http()).await?;
        let response = client.acknowledge_request_vote(request).await?;
        return Ok(response);
        //TODO: Handle error
    }
}

#[async_trait]
impl ServiceClientProvider<RequestVoteResponse, ()> for RequestVoteResponseClient {
    async fn call(&self, request: Request<RequestVoteResponse>, address: HostAndPort) -> Result<Response<()>, ServiceResponseError> {
        let mut client = RaftClient::connect(address.as_string_with_http()).await?;
        let response = client.finish_request_vote(request).await?;
        return Ok(response);
        //TODO: Handle error
    }
}

#[async_trait]
impl ServiceClientProvider<AppendEntries, AppendEntriesResponse> for RaftHeartbeatServiceClient {
    async fn call(&self, request: Request<AppendEntries>, address: HostAndPort) -> Result<Response<AppendEntriesResponse>, ServiceResponseError> {
        let mut client = RaftClient::connect(address.as_string_with_http()).await?;
        let response = client.acknowledge_heartbeat(request).await?;
        return Ok(response);
        //TODO: Handle error
    }
}