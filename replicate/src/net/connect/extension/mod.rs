use std::str::FromStr;
use tonic::metadata::MetadataValue;
use tonic::Request;
use crate::net::connect::host_and_port::HostAndPort;
use crate::net::connect::host_port_extractor::{HostAndPortExtractor, HostAndPortHeaderAdder, REFERRAL_HOST, REFERRAL_PORT};

impl<Payload> HostAndPortExtractor for Request<Payload> {
    fn get_referral_host(&self) -> Option<String> {
        let headers = self.metadata();
        let optional_host = headers.get(REFERRAL_HOST);
        if let Some(host) = optional_host {
            return Some(String::from(host.to_str().unwrap()));
        }
        return None;
    }

    fn get_referral_port(&self) -> Option<u16> {
        let headers = self.metadata();
        let optional_port = headers.get(REFERRAL_PORT);
        if let Some(port) = optional_port {
            let result = FromStr::from_str(port.to_str().unwrap());
            return Some(result.unwrap());
        }
        return None;
    }
}

impl<Payload> HostAndPortHeaderAdder for Request<Payload>  {
    fn add_host_port(&mut self, address: HostAndPort) {
        let headers = self.metadata_mut();
        headers.insert(REFERRAL_HOST, address.host_as_string().parse().unwrap());
        headers.insert(REFERRAL_PORT, MetadataValue::from(address.port()));
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use tonic::metadata::MetadataValue;
    use tonic::Request;
    use crate::net::connect::host_and_port::HostAndPort;
    use crate::net::connect::host_port_extractor::{HostAndPortExtractor, HostAndPortHeaderAdder, REFERRAL_HOST, REFERRAL_PORT};

    #[test]
    fn get_host() {
        let mut request = Request::new(());
        let headers = request.metadata_mut();
        headers.insert(REFERRAL_HOST, "192.168.0.1".parse().unwrap());

        let host = request.get_referral_host().unwrap();
        assert_eq!("192.168.0.1".to_string(), host);
    }

    #[test]
    fn get_non_existent_host() {
        let request = Request::new(());

        let host = request.get_referral_host();
        assert_eq!(None, host);
    }

    #[test]
    fn get_port() {
        let mut request = Request::new(());
        let headers = request.metadata_mut();
        headers.insert(REFERRAL_PORT, MetadataValue::from(8912));

        let port = request.get_referral_port().unwrap();
        assert_eq!(8912, port);
    }

    #[test]
    fn get_non_existent_port() {
        let request = Request::new(());

        let port = request.get_referral_port();
        assert_eq!(None, port);
    }

    #[test]
    fn add_host() {
        let mut request = Request::new(());
        let address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);

        request.add_host_port(address);
        assert_eq!(Some("127.0.0.1".to_string()), request.get_referral_host());
    }

    #[test]
    fn add_port() {
        let mut request = Request::new(());
        let address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);

        request.add_host_port(address);
        assert_eq!(Some(50051), request.get_referral_port());
    }
}