use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use crate::net::connect::correlation_id::CorrelationId;

pub struct RequestTimeoutError {
    pub correlation_id: CorrelationId
}

impl Display for RequestTimeoutError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Request timeout {}", self.correlation_id)
    }
}

impl Debug for RequestTimeoutError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Request timeout {}", self.correlation_id)
    }
}

impl Error for RequestTimeoutError {}