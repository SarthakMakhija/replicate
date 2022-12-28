use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::net::AddrParseError;
use tonic::{Code, Status};

use crate::net::connect::host_and_port::HostAndPort;

pub const REFERRAL_HOST: &str = "replicate.referral.host";
pub const REFERRAL_PORT: &str = "replicate.referral.port";

#[derive(Debug)]
pub enum HostAndPortConstructionError {
    MissingHortOrPort,
    InvalidHost(AddrParseError, String),
}

impl Display for HostAndPortConstructionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            HostAndPortConstructionError::MissingHortOrPort =>
                write!(f, "can not construct HostAndPort, either the host or the port is missing"),
            HostAndPortConstructionError::InvalidHost(_, host) =>
                write!(f, "can not construct HostAndPort, host {} is invalid", host)
        }
    }
}

impl Error for HostAndPortConstructionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match *self {
            HostAndPortConstructionError::MissingHortOrPort => None,
            HostAndPortConstructionError::InvalidHost(ref err, _) => Some(err),
        }
    }
}

impl From<HostAndPortConstructionError> for Status {
    fn from(err: HostAndPortConstructionError) -> Self {
        return Status::new(
            Code::FailedPrecondition,
            err.to_string()
        )
    }
}

pub trait HostAndPortExtractor {
    fn try_referral_host_port(&self) -> Result<HostAndPort, HostAndPortConstructionError> {
        let optional_host = self.get_referral_host();
        let optional_port = self.get_referral_port();
        if optional_host.is_none() || optional_port.is_none() {
            return Err(HostAndPortConstructionError::MissingHortOrPort);
        }

        let host = optional_host.unwrap();
        return match HostAndPort::try_new(&host, u16::try_from(optional_port.unwrap()).unwrap()) {
            Ok(host_and_port) => Ok(host_and_port),
            Err(err) => Err(HostAndPortConstructionError::InvalidHost(err, host))
        };
    }
    fn get_referral_host(&self) -> Option<String>;
    fn get_referral_port(&self) -> Option<u16>;
}


#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use tonic::Request;

    use crate::net::connect::host_port_extractor::{HostAndPortConstructionError, HostAndPortExtractor, REFERRAL_HOST, REFERRAL_PORT};
    use crate::net::connect::host_and_port::HostAndPort;
    use tonic::{Code, Status};

    #[test]
    fn get_host_and_port() {
        let mut request = Request::new(());
        let headers = request.metadata_mut();
        headers.insert(REFERRAL_HOST, "192.168.0.1".parse().unwrap());
        headers.insert(REFERRAL_PORT, "9888".parse().unwrap());

        let host_and_port = request.try_referral_host_port().unwrap();
        let expected = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)), 9888);

        assert_eq!(expected, host_and_port)
    }

    #[test]
    fn missing_host_in_request() {
        let mut request = Request::new(());
        let headers = request.metadata_mut();
        headers.insert(REFERRAL_PORT, "9888".parse().unwrap());

        let host_and_port = request.try_referral_host_port();

        assert!(host_and_port.is_err());
        assert!(matches!(host_and_port, Err(HostAndPortConstructionError::MissingHortOrPort)));
    }

    #[test]
    fn missing_port_in_request() {
        let mut request = Request::new(());
        let headers = request.metadata_mut();
        headers.insert(REFERRAL_HOST, "192.168.0.1".parse().unwrap());

        let host_and_port = request.try_referral_host_port();

        assert!(host_and_port.is_err());
        assert!(matches!(host_and_port, Err(HostAndPortConstructionError::MissingHortOrPort)));
    }

    #[test]
    fn invalid_host_in_request() {
        let mut request = Request::new(());
        let headers = request.metadata_mut();
        headers.insert(REFERRAL_HOST, "invalid_host".parse().unwrap());
        headers.insert(REFERRAL_PORT, "9888".parse().unwrap());

        let host_and_port = request.try_referral_host_port();

        assert!(host_and_port.is_err());
        assert!(matches!(host_and_port, Err(HostAndPortConstructionError::InvalidHost(_, _))));
    }

    #[test]
    fn status_from_host_and_port_construction_error() {
        let missing_host_or_port = HostAndPortConstructionError::MissingHortOrPort;
        let error_message = format!("{}", &missing_host_or_port);

        let status = Status::from(missing_host_or_port);

        assert_eq!(Code::FailedPrecondition ,status.code());
        assert!(status.to_string().contains(&error_message));
    }
}