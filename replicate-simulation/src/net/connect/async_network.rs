use std::error::Error;
use std::fmt::{Display, Formatter};

use tonic::Request;
use tonic::transport::Channel;

use replicate::net::connect::error::ServiceResponseError;
use replicate::net::connect::host_and_port::HostAndPort;
use replicate::net::connect::host_port_extractor::HostAndPortHeaderAdder;
use replicate::net::connect::service_client::ServiceRequest;

use crate::net::connect::induced_failure::InducedFailure;

pub struct AsyncNetwork {
    induced_failure: InducedFailure,
}

impl AsyncNetwork {
    pub fn new() -> Self {
        return AsyncNetwork {
            induced_failure: InducedFailure::new()
        };
    }

    pub async fn send_with_source_footprint<Payload: Send, R>(
        &self,
        service_request: ServiceRequest<Payload, R>,
        source_address: HostAndPort,
        target_address: HostAndPort,
    ) -> Result<R, ServiceResponseError>
        where Payload: Send {
        return self.send(
            service_request,
            Some(source_address),
            target_address,
            None,
        ).await;
    }

    pub async fn send_without_source_footprint<Payload: Send, R>(
        &self,
        service_request: ServiceRequest<Payload, R>,
        target_address: HostAndPort,
    ) -> Result<R, ServiceResponseError>
        where Payload: Send {
        return self.send(
            service_request,
            None,
            target_address,
            None,
        ).await;
    }

    pub async fn send_with_source_footprint_on<Payload: Send, R>(
        &self,
        service_request: ServiceRequest<Payload, R>,
        source_address: HostAndPort,
        target_address: HostAndPort,
        channel: Option<Channel>,
    ) -> Result<R, ServiceResponseError>
        where Payload: Send {
        return self.send(
            service_request,
            Some(source_address),
            target_address,
            channel,
        ).await;
    }

    pub fn drop_requests_to(&self, address: HostAndPort) {
        self.induced_failure.drop_requests_to(address);
    }

    pub fn drop_requests_after(&self, count: u64, address: HostAndPort) {
        self.induced_failure.drop_requests_after(count, address);
    }

    async fn send<Payload: Send, R>(
        &self,
        service_request: ServiceRequest<Payload, R>,
        source_address: Option<HostAndPort>,
        target_address: HostAndPort,
        channel: Option<Channel>,
    ) -> Result<R, ServiceResponseError>
        where Payload: Send {
        if self.induced_failure.should_drop_to(&target_address) {
            return Err(Box::new(RequestDropError {
                host_and_port: target_address
            }));
        }

        self.induced_failure.increase_request_count_for(target_address.clone());
        let client = &service_request.service_client;
        let payload = service_request.payload;
        let mut request = Request::new(payload);
        if let Some(address) = source_address {
            request.add_host_port(address);
        }

        let result = client.call(request, target_address, channel).await;
        return match result {
            Ok(response) => { Ok(response.into_inner()) }
            Err(e) => { Err(e) }
        };
    }
}

#[derive(Debug)]
pub struct RequestDropError {
    pub host_and_port: HostAndPort,
}

impl Display for RequestDropError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "message meant to be dropped to {:?}", self.host_and_port)
    }
}

impl Error for RequestDropError {}