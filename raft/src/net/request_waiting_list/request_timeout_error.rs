use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

pub struct RequestTimeoutError {}

impl Display for RequestTimeoutError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Request timeout")
    }
}

impl Debug for RequestTimeoutError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Request timeout")
    }
}

impl Error for RequestTimeoutError {}