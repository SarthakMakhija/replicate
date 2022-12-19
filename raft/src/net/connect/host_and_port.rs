use std::net::{IpAddr, SocketAddr};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct HostAndPort {
    host: IpAddr,
    port: u16,
}

impl HostAndPort {
    pub fn new(host: IpAddr, port: u16) -> HostAndPort {
        return HostAndPort { host, port };
    }

    pub fn try_new(host: String, port: u16) -> Result<HostAndPort, std::net::AddrParseError> {
        let address = format!("{}:{}", host, port);
        let socket_address: SocketAddr = address.parse()?;
        return Ok(
            HostAndPort::new(socket_address.ip(), port)
        );
    }

    pub fn as_string_with_http(&self) -> String {
        return self.as_string_with_protocol(String::from("http"));
    }

    pub fn as_string_with_protocol(&self, protocol: String) -> String {
        return format!("{}://{}:{}/", protocol, self.host, self.port);
    }

    pub fn as_string(&self) -> String {
        return format!("{}:{}/", self.host, self.port);
    }

    pub fn as_socket_address(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        let address = format!("{}:{}", self.host, self.port);
        return address.parse();
    }

    pub fn host_as_string(&self) -> String {
        return format!("{}", self.host);
    }

    pub fn port(&self) -> u16 {
        return self.port;
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use crate::net::connect::host_and_port::HostAndPort;

    #[test]
    fn as_string_http() {
        let host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9091);
        let as_string = host_and_port.as_string_with_http();

        assert_eq!("http://127.0.0.1:9091/".to_string(), as_string);
    }

    #[test]
    fn as_string() {
        let host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9091);
        let as_string = host_and_port.as_string();

        assert_eq!("127.0.0.1:9091/".to_string(), as_string);
    }

    #[test]
    fn as_socket_address() {
        let host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9091);
        let socket_address = host_and_port.as_socket_address().unwrap();
        let ip_addr = socket_address.ip();

        assert_eq!(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), ip_addr);
    }

    #[test]
    fn host_as_string() {
        let host_and_port = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9091);
        let host_as_string = host_and_port.host_as_string();

        assert_eq!("127.0.0.1".to_string(), host_as_string);
    }

    #[test]
    fn try_new() {
        let host_and_port = HostAndPort::try_new("127.0.0.1".to_string(), 9091);
        let host_and_port = host_and_port.unwrap();

        assert_eq!(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9091), host_and_port);
    }

    #[test]
    fn try_new_with_error() {
        let host_and_port = HostAndPort::try_new("127#0#0#1".to_string(), 9091);
        let is_error = host_and_port.is_err();

        assert!(is_error);
    }
}