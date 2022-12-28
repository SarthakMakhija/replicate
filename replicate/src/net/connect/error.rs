use std::error::Error;
use tonic::{Code, Status};
use crate::net::connect::host_port_extractor::HostAndPortConstructionError;

pub type ServiceResponseError = Box<dyn Error + Send + Sync + 'static>;

impl From<HostAndPortConstructionError> for Status {
    fn from(err: HostAndPortConstructionError) -> Self {
        return Status::new(
            Code::FailedPrecondition,
            err.to_string()
        )
    }
}

#[cfg(test)]
mod tests {
    use tonic::{Code, Status};
    use crate::net::connect::host_port_extractor::HostAndPortConstructionError;

    #[test]
    fn status_from_host_and_port_construction_error() {
        let missing_host_or_port = HostAndPortConstructionError::MissingHortOrPort;
        let error_message = format!("{}", &missing_host_or_port);

        let status = Status::from(missing_host_or_port);

        assert_eq!(Code::FailedPrecondition ,status.code());
        assert!(status.to_string().contains(&error_message));
    }
}