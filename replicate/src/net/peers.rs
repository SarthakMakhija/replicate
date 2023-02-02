use std::str::FromStr;

use tonic::transport::Endpoint;

use crate::net::connect::host_and_port::HostAndPort;

pub struct Peers {
    peers: Vec<Peer>,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Peer {
    address: HostAndPort,
}

impl Peers {
    pub fn new(addresses: Vec<HostAndPort>) -> Self {
        return Peers {
            peers: addresses.iter().map(|addr| Peer { address: addr.clone() }).collect()
        };
    }

    pub fn from(peers: Vec<Peer>) -> Self {
        return Peers { peers };
    }

    pub fn all_peers(&self) -> Vec<Peer> {
        return self.peers.clone();
    }

    pub(crate) fn all_peers_excluding(&self, peer: Peer) -> Vec<Peer> {
        return self.peers.iter().filter(|a_peer| a_peer.ne(&&peer)).map(|a_peer| a_peer.clone()).collect();
    }

    pub(crate) fn total_peer_count_excluding(&self, peer: Peer) -> usize {
        self.peers.iter().filter(|a_peer| a_peer.ne(&&peer)).count()
    }
}

impl Peer {
    pub fn new(address: HostAndPort) -> Self {
        return Peer {
            address
        };
    }

    pub(crate) fn get_endpoint(&self) -> Endpoint {
        let result = Endpoint::from_str(self.address.as_string_with_http().as_str());
        return match result {
            Ok(endpoint) => endpoint,
            Err(err) => panic!("error creating an endpoint from the address {:?}, {:?}", self.address, err)
        };
    }

    pub(crate) fn get_address(&self) -> &HostAndPort {
        return &self.address;
    }

    pub(crate) fn has_address(&self, address: &HostAndPort) -> bool {
        return self.address.eq(address);
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use crate::net::connect::host_and_port::HostAndPort;
    use crate::net::peers::{Peer, Peers};

    #[test]
    fn all_peers_excluding_self() {
        let peers = Peers::new(
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8989),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9090),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080),
            ],
        );

        let peers = peers.all_peers_excluding(Peer::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080)));
        assert_eq!(vec![
            Peer::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8989)),
            Peer::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9090))], peers);
    }

    #[test]
    fn all_peers() {
        let peers = Peers::new(
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8989),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9090),
            ],
        );

        let peers = peers.all_peers_excluding(Peer::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080)));
        assert_eq!(vec![
            Peer::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8989)),
            Peer::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9090))], peers);
    }

    #[test]
    fn total_peer_count_excluding_self() {
        let peers = Peers::new(
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8989),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9090),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080),
            ],
        );

        let total_peer_count = peers.total_peer_count_excluding(Peer::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7080)));
        assert_eq!(2, total_peer_count);
    }

    #[test]
    fn total_peer_count() {
        let peers = Peers::new(
            vec![
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8989),
                HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9090),
            ],
        );

        let total_peer_count = peers.total_peer_count_excluding(Peer::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9098)));
        assert_eq!(2, total_peer_count);
    }
}