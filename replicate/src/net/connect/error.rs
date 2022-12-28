use std::error::Error;
use std::fmt::{Display, Formatter};
use tonic::Status;
use crate::net::connect::host_and_port::HostAndPort;

pub type ServiceResponseError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Debug)]
pub struct ServiceConnectError {
    address: HostAndPort,
    additional_message: String,
}

impl Display for ServiceConnectError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let message = format!("Failed to connect to the address {:?}, additional context {}", self.address, self.additional_message);
        write!(formatter, "{}", message)
    }
}

impl Error for ServiceConnectError {}

#[derive(Debug)]
pub struct ServiceError {
    address: HostAndPort,
    additional_message: String,
}

impl Display for ServiceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let message = format!("Failed to invoke the service {} on the address {:?}, additional context {}", "NA", self.address, self.additional_message);
        write!(formatter, "{}", message)
    }
}

impl Error for ServiceError {}

pub fn transform_connection_result<T>(connection_result: Result<T, tonic::transport::Error>, address: HostAndPort) -> Result<T, ServiceResponseError> {
    return match connection_result {
        Ok(response) => Ok(response),
        Err(err) => {
            Err(Box::new(ServiceConnectError { address, additional_message: err.to_string() }))
        }
    };
}

pub fn transform_rpc_result<T>(rpc_result: Result<T, Status>, address: HostAndPort) -> Result<T, ServiceResponseError> {
    return match rpc_result {
        Ok(response) => Ok(response),
        Err(status) => Err(Box::new(ServiceError { address, additional_message: status.message().to_string() }))
    };
}