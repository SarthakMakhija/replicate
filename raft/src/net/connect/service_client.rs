use std::error::Error;
use std::sync::Arc;

use async_trait::async_trait;
use tonic::Response;

use crate::net::connect::host_and_port::HostAndPort;

pub struct ServiceRequest<Payload, Response, CorrelationId>
    where Payload: Send {
    pub(crate) payload: Payload,
    pub(crate) service_client: Box<dyn ServiceClientProvider<Payload, Response>>,
    pub(crate) correlation_id: CorrelationId,
}

impl<Payload: Send, Response, CorrelationId> ServiceRequest<Payload, Response, CorrelationId>
    where Payload: Send {
    pub fn new(
        payload: Payload,
        service_client: Box<dyn ServiceClientProvider<Payload, Response>>,
        correlation_id: CorrelationId,
    ) -> Self <> {
        return ServiceRequest {
            payload,
            service_client,
            correlation_id,
        };
    }
}

pub type ServiceResponseError = Box<dyn Error + Send + Sync + 'static>;

#[async_trait]
pub trait ServiceClientProvider<Payload: Send, R>: Send + Sync {
    async fn call(&self, request: Payload, address: Arc<HostAndPort>) -> Result<Response<R>, ServiceResponseError>;
}