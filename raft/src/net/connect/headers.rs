use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::net::AddrParseError;
use std::str::FromStr;

use tonic::Request;

use crate::net::connect::host_and_port::HostAndPort;

pub const REFERRAL_HOST: &str = "raft.referral.host";
pub const REFERRAL_PORT: &str = "raft.referral.port";

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

pub fn try_referral_host_port_from<Payload>(request: &Request<Payload>) -> Result<HostAndPort, HostAndPortConstructionError> {
    let optional_host = get_referral_host_from(&request);
    let optional_port = get_referral_port_from(&request);
    if optional_host.is_none() || optional_port.is_none() {
        return Err(HostAndPortConstructionError::MissingHortOrPort);
    }

    let host = optional_host.unwrap();
    return match HostAndPort::try_new(&host, u16::try_from(optional_port.unwrap()).unwrap()) {
        Ok(host_and_port) => Ok(host_and_port),
        Err(err) => Err(HostAndPortConstructionError::InvalidHost(err, host))
    };
}

pub fn get_referral_host_from<Payload>(request: &Request<Payload>) -> Option<String> {
    let headers = request.metadata();
    let optional_host = headers.get(REFERRAL_HOST);
    if let Some(host) = optional_host {
        return Some(String::from(host.to_str().unwrap()));
    }
    return None;
}

pub fn get_referral_port_from<Payload>(request: &Request<Payload>) -> Option<u16> {
    let headers = request.metadata();
    let optional_port = headers.get(REFERRAL_PORT);
    if let Some(port) = optional_port {
        let result = FromStr::from_str(port.to_str().unwrap());
        return Some(result.unwrap());
    }
    return None;
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use tonic::metadata::MetadataValue;
    use tonic::Request;

    use crate::net::connect::headers::{get_referral_host_from, get_referral_port_from, HostAndPortConstructionError, REFERRAL_HOST, REFERRAL_PORT, try_referral_host_port_from};
    use crate::net::connect::host_and_port::HostAndPort;

    #[test]
    fn get_host_and_port() {
        let mut request = Request::new(());
        let headers = request.metadata_mut();
        headers.insert(REFERRAL_HOST, "192.168.0.1".parse().unwrap());
        headers.insert(REFERRAL_PORT, "9888".parse().unwrap());

        let host_and_port = try_referral_host_port_from(&request).unwrap();
        let expected = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)), 9888);

        assert_eq!(expected, host_and_port)
    }

    #[test]
    fn missing_host_in_request() {
        let mut request = Request::new(());
        let headers = request.metadata_mut();
        headers.insert(REFERRAL_PORT, "9888".parse().unwrap());

        let host_and_port = try_referral_host_port_from(&request);

        assert!(host_and_port.is_err());
        assert!(matches!(host_and_port, Err(HostAndPortConstructionError::MissingHortOrPort)));
    }

    #[test]
    fn missing_port_in_request() {
        let mut request = Request::new(());
        let headers = request.metadata_mut();
        headers.insert(REFERRAL_HOST, "192.168.0.1".parse().unwrap());

        let host_and_port = try_referral_host_port_from(&request);

        assert!(host_and_port.is_err());
        assert!(matches!(host_and_port, Err(HostAndPortConstructionError::MissingHortOrPort)));
    }

    #[test]
    fn invalid_host_in_request() {
        let mut request = Request::new(());
        let headers = request.metadata_mut();
        headers.insert(REFERRAL_HOST, "invalid_host".parse().unwrap());
        headers.insert(REFERRAL_PORT, "9888".parse().unwrap());

        let host_and_port = try_referral_host_port_from(&request);

        assert!(host_and_port.is_err());
        assert!(matches!(host_and_port, Err(HostAndPortConstructionError::InvalidHost(_, _))));
    }

    #[test]
    fn get_host() {
        let mut request = Request::new(());
        let headers = request.metadata_mut();
        headers.insert(REFERRAL_HOST, "192.168.0.1".parse().unwrap());

        let host = get_referral_host_from(&request).unwrap();
        assert_eq!("192.168.0.1".to_string(), host);
    }

    #[test]
    fn get_non_existent_host() {
        let request = Request::new(());

        let host = get_referral_host_from(&request);
        assert_eq!(None, host);
    }

    #[test]
    fn get_port() {
        let mut request = Request::new(());
        let headers = request.metadata_mut();
        headers.insert(REFERRAL_PORT, MetadataValue::from(8912));

        let port = get_referral_port_from(&request).unwrap();
        assert_eq!(8912, port);
    }

    #[test]
    fn get_non_existent_port() {
        let request = Request::new(());

        let port = get_referral_port_from(&request);
        assert_eq!(None, port);
    }
}