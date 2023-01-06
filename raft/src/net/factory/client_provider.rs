use async_trait::async_trait;
use tonic::{Request, Response};

use replicate::net::connect::host_and_port::HostAndPort;
use replicate::net::connect::service_client::ServiceClientProvider;
use replicate::net::connect::error::ServiceResponseError;

use crate::net::rpc::grpc::RequestVote;
use crate::net::rpc::grpc::RequestVoteResponse;
use crate::net::rpc::grpc::AppendEntries;
use crate::net::rpc::grpc::AppendEntriesResponse;
use crate::net::rpc::grpc::raft_client::RaftClient;

pub struct RequestVoteClient {}

pub struct RequestVoteResponseClient {}

pub struct HeartbeatServiceClient {}

pub struct ReplicateLogClient {}

pub struct ReplicateLogResponseClient {}

#[async_trait]
impl ServiceClientProvider<RequestVote, ()> for RequestVoteClient {
    async fn call(&self, request: Request<RequestVote>, address: HostAndPort) -> Result<Response<()>, ServiceResponseError> {
        let mut client = RaftClient::connect(address.as_string_with_http()).await?;
        let response = client.acknowledge_request_vote(request).await?;
        return Ok(response);
    }
}

#[async_trait]
impl ServiceClientProvider<RequestVoteResponse, ()> for RequestVoteResponseClient {
    async fn call(&self, request: Request<RequestVoteResponse>, address: HostAndPort) -> Result<Response<()>, ServiceResponseError> {
        let mut client = RaftClient::connect(address.as_string_with_http()).await?;
        let response = client.finish_request_vote(request).await?;
        return Ok(response);
    }
}

#[async_trait]
impl ServiceClientProvider<AppendEntries, AppendEntriesResponse> for HeartbeatServiceClient {
    async fn call(&self, request: Request<AppendEntries>, address: HostAndPort) -> Result<Response<AppendEntriesResponse>, ServiceResponseError> {
        let mut client = RaftClient::connect(address.as_string_with_http()).await?;
        let response = client.acknowledge_heartbeat(request).await?;
        return Ok(response);
    }
}

#[async_trait]
impl ServiceClientProvider<AppendEntries, ()> for ReplicateLogClient {
    async fn call(&self, request: Request<AppendEntries>, address: HostAndPort) -> Result<Response<()>, ServiceResponseError> {
        let mut client = RaftClient::connect(address.as_string_with_http()).await?;
        let response = client.acknowledge_replicate_log(request).await?;
        return Ok(response);
    }
}

#[async_trait]
impl ServiceClientProvider<AppendEntriesResponse, ()> for ReplicateLogResponseClient {
    async fn call(&self, request: Request<AppendEntriesResponse>, address: HostAndPort) -> Result<Response<()>, ServiceResponseError> {
        let mut client = RaftClient::connect(address.as_string_with_http()).await?;
        let response = client.finish_replicate_log(request).await?;
        return Ok(response);
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use tonic::Request;
    use replicate::net::connect::host_and_port::HostAndPort;
    use replicate::net::connect::service_client::ServiceClientProvider;
    use crate::net::factory::client_provider::{HeartbeatServiceClient, ReplicateLogClient, ReplicateLogResponseClient, RequestVoteClient, RequestVoteResponseClient};
    use crate::net::rpc::grpc::RequestVote;
    use crate::net::rpc::grpc::RequestVoteResponse;
    use crate::net::rpc::grpc::AppendEntries;
    use crate::net::rpc::grpc::AppendEntriesResponse;

    #[tokio::test]
    async fn request_vote_client_with_connection_error() {
        let client = RequestVoteClient{};
        let request = Request::new(
            RequestVote {
                term: 1,
                replica_id: 10,
                correlation_id: 10,
            }
        );
        let address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080);

        let result = client.call(request, address).await;
        assert!(result.is_err());

        let result = result.unwrap_err().downcast::<tonic::transport::Error>();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn request_vote_response_client_with_connection_error() {
        let client = RequestVoteResponseClient{};
        let request = Request::new(
            RequestVoteResponse {
                term: 1,
                voted: true,
                correlation_id: 10,
            }
        );
        let address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080);

        let result = client.call(request, address).await;
        assert!(result.is_err());

        let result = result.unwrap_err().downcast::<tonic::transport::Error>();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn append_entries_client_with_connection_error() {
        let client = HeartbeatServiceClient {};
        let request = Request::new(
            AppendEntries {
                term: 1,
                leader_id: 10,
                correlation_id: 10,
                entry: None,
                previous_log_index: None,
                previous_log_term: None
            }
        );
        let address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080);

        let result = client.call(request, address).await;
        assert!(result.is_err());

        let result = result.unwrap_err().downcast::<tonic::transport::Error>();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn replicate_log_client_with_connection_error() {
        let client = ReplicateLogClient {};
        let request = Request::new(
            AppendEntries {
                term: 1,
                leader_id: 30,
                correlation_id: 10,
                previous_log_index: None,
                previous_log_term: None,
                entry: None
            }
        );
        let address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080);

        let result = client.call(request, address).await;
        assert!(result.is_err());

        let result = result.unwrap_err().downcast::<tonic::transport::Error>();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn replicate_log_response_client_with_connection_error() {
        let client = ReplicateLogResponseClient {};
        let request = Request::new(
            AppendEntriesResponse {
                term: 1,
                success: true,
                correlation_id: 10,
            }
        );
        let address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080);

        let result = client.call(request, address).await;
        assert!(result.is_err());

        let result = result.unwrap_err().downcast::<tonic::transport::Error>();
        assert!(result.is_ok());
    }
}