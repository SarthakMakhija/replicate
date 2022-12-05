use std::error::Error;

use async_trait::async_trait;
use tonic::Response;

use crate::net::connect::host_and_port::HostAndPort;

pub struct ServiceRequest<Payload, Response>
    where Payload: Send, Response: Send {
    pub(crate) payload: Payload,
    pub(crate) service_client: Box<dyn ServiceClientProvider<Payload, Response>>,
}

pub type ServiceResponseError = Box<dyn Error + Send + Sync + 'static>;

#[async_trait]
pub(crate) trait ServiceClientProvider<Payload: Send, R: Send>: Send + Sync {
    async fn call(&self, request: Payload, address: &HostAndPort) -> Result<Response<R>, ServiceResponseError>;
}