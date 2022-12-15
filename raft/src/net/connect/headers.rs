use std::str::FromStr;

use tonic::Request;

pub const REFERRAL_HOST: &str = "raft.referral.host";
pub const REFERRAL_PORT: &str = "raft.referral.port";

pub fn get_referral_host_from<Payload>(request: &Request<Payload>) -> Option<String> {
    let headers = request.metadata();
    let optional_host = headers.get(REFERRAL_HOST);
    if let Some(host) = optional_host {
        return Some(String::from(host.to_str().unwrap()));
    }
    return None;
}

pub fn get_referral_port_from<Payload>(request: &Request<Payload>) -> Option<u32> {
    let headers = request.metadata();
    let optional_port = headers.get(REFERRAL_PORT);
    if let Some(port) = optional_port {
        return Some(FromStr::from_str(port.to_str().unwrap()).unwrap());
    }
    return None;
}

#[cfg(test)]
mod tests {
    use tonic::metadata::MetadataValue;
    use tonic::Request;

    use crate::net::connect::headers::{get_referral_host_from, get_referral_port_from, REFERRAL_HOST, REFERRAL_PORT};

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