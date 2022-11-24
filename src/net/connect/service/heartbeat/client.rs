use async_trait::async_trait;
use tonic::{Request, Response};

use crate::net::connect::host_and_port::HostAndPort;
use crate::net::connect::service::heartbeat::service::grpc::heartbeat_client::HeartbeatClient;
use crate::net::connect::service::heartbeat::service::grpc::HeartbeatRequest;
use crate::net::connect::service_client::{ServiceClient, ServiceResponseError};

pub(crate) struct HeartbeatServiceClient {}

#[async_trait]
impl ServiceClient<HeartbeatRequest, ()> for HeartbeatServiceClient {
    async fn call(&self, request: HeartbeatRequest, address: &HostAndPort) -> Result<Response<()>, ServiceResponseError> {
        let mut client = HeartbeatClient::connect(address.as_string_with_http()).await?;
        let request = Request::new(request);
        let response = client.acknowledge(request).await?;
        return Ok(response);
    }
}