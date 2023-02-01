use async_trait::async_trait;
use tonic::{Request, Response};
use tonic::transport::Channel;

use replicate::net::connect::error::ServiceResponseError;
use replicate::net::connect::host_and_port::HostAndPort;
use replicate::net::connect::service_client::ServiceClientProvider;

use crate::net::rpc::grpc::AppendEntries;
use crate::net::rpc::grpc::AppendEntriesResponse;
use crate::net::rpc::grpc::raft_client::RaftClient;
use crate::net::rpc::grpc::RequestVote;
use crate::net::rpc::grpc::RequestVoteResponse;

pub struct RequestVoteClient {}

pub struct HeartbeatClient {}

pub struct ReplicateLogClient {}

#[async_trait]
impl ServiceClientProvider<RequestVote, RequestVoteResponse> for RequestVoteClient {
    async fn call(&self, request: Request<RequestVote>, address: HostAndPort, _channel: Option<Channel>) -> Result<Response<RequestVoteResponse>, ServiceResponseError> {
        let mut client = RaftClient::connect(address.as_string_with_http()).await?;
        let response = client.acknowledge_request_vote(request).await?;
        return Ok(response);
    }
}

#[async_trait]
impl ServiceClientProvider<AppendEntries, AppendEntriesResponse> for HeartbeatClient {
    async fn call(&self, request: Request<AppendEntries>, address: HostAndPort, _channel: Option<Channel>) -> Result<Response<AppendEntriesResponse>, ServiceResponseError> {
        let mut client = RaftClient::connect(address.as_string_with_http()).await?;
        let response = client.acknowledge_heartbeat(request).await?;
        return Ok(response);
    }
}

#[async_trait]
impl ServiceClientProvider<AppendEntries, AppendEntriesResponse> for ReplicateLogClient {
    async fn call(&self, request: Request<AppendEntries>, address: HostAndPort, _channel: Option<Channel>) -> Result<Response<AppendEntriesResponse>, ServiceResponseError> {
        let mut client = RaftClient::connect(address.as_string_with_http()).await?;
        let response = client.acknowledge_replicate_log(request).await?;
        return Ok(response);
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use tonic::Request;

    use replicate::net::connect::host_and_port::HostAndPort;
    use replicate::net::connect::service_client::ServiceClientProvider;

    use crate::net::builder::heartbeat::HeartbeatRequestBuilder;
    use crate::net::builder::log::ReplicateLogRequestBuilder;
    use crate::net::builder::request_vote::RequestVoteBuilder;
    use crate::net::factory::client_provider::{HeartbeatClient, ReplicateLogClient, RequestVoteClient};

    #[tokio::test]
    async fn request_vote_client_with_connection_error() {
        let client = RequestVoteClient {};
        let request = Request::new(
            RequestVoteBuilder::request_vote(10, 1, 10)
        );
        let address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080);

        let result = client.call(request, address, None).await;
        assert!(result.is_err());

        let result = result.unwrap_err().downcast::<tonic::transport::Error>();
        assert!(result.is_ok());
    }


    #[tokio::test]
    async fn append_entries_client_with_connection_error() {
        let client = HeartbeatClient {};
        let request = Request::new(
            HeartbeatRequestBuilder::heartbeat_request(1, 10, 10)
        );
        let address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080);

        let result = client.call(request, address, None).await;
        assert!(result.is_err());

        let result = result.unwrap_err().downcast::<tonic::transport::Error>();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn replicate_log_client_with_connection_error() {
        let client = ReplicateLogClient {};
        let request = Request::new(
            ReplicateLogRequestBuilder::replicate_log_request_with_no_log_reference(
                1,
                10,
                10,
            )
        );
        let address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080);

        let result = client.call(request, address, None).await;
        assert!(result.is_err());

        let result = result.unwrap_err().downcast::<tonic::transport::Error>();
        assert!(result.is_ok());
    }
}