use std::net::{IpAddr, SocketAddr};

pub struct HostAndPort {
    host: IpAddr,
    port: u16,
}

impl HostAndPort {
    pub fn new(host: IpAddr, port: u16) -> HostAndPort {
        return HostAndPort { host, port };
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
}