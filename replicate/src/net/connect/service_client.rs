use async_trait::async_trait;
use tonic::{Request, Response};
use tonic::transport::Channel;

use crate::net::connect::correlation_id::CorrelationId;
use crate::net::connect::error::ServiceResponseError;
use crate::net::connect::host_and_port::HostAndPort;

pub struct ServiceRequest<Payload, Response>
    where Payload: Send {
    pub(crate) payload: Payload,
    pub(crate) service_client: Box<dyn ServiceClientProvider<Payload, Response>>,
    pub(crate) correlation_id: CorrelationId,
}

impl<Payload: Send, Response> ServiceRequest<Payload, Response>
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

    pub fn get_payload(&self) -> &Payload {
        return &self.payload;
    }

    pub fn into_payload(self) -> Payload {
        return self.payload;
    }
}

#[async_trait]
pub trait ServiceClientProvider<Payload: Send, R>: Send + Sync {
    async fn call(&self, request: Request<Payload>, address: HostAndPort, channel: Option<Channel>) -> Result<Response<R>, ServiceResponseError>;
}