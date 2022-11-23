use std::error::Error;

use async_trait::async_trait;
use tonic::Response;

use crate::net::connect::host_and_port::HostAndPort;

pub(crate) struct ServiceServerRequest<Payload, Response>
    where Payload: Send, Response: Send {
    pub(crate) payload: Payload,
    pub(crate) callable: Box<dyn ServiceServerCallable<Payload, Response>>,
}

pub type ServiceServerError = Box<dyn Error>;

#[async_trait]
pub(crate) trait ServiceServerCallable<Payload: Send, R: Send>: Send + Sync {
    async fn call(&self, request: Payload, address: &HostAndPort) -> Result<Response<R>, ServiceServerError>;
}