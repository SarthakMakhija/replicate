use std::net::{IpAddr, SocketAddr};

pub(crate) struct HostAndPort {
    host: IpAddr,
    port: u16,
}

impl HostAndPort {
    pub(crate) fn new(host: IpAddr, port: u16) -> HostAndPort {
        return HostAndPort { host, port };
    }

    pub(crate) fn as_string_with_http(&self) -> String {
        return self.as_string(String::from("http"));
    }

    pub(crate) fn as_string(&self, protocol: String) -> String {
        return format!("{}://{}:{}/", protocol, self.host, self.port);
    }

    pub(crate) fn as_socket_address(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        let address = format!("{}:{}", self.host, self.port);
        return address.parse();
    }
}