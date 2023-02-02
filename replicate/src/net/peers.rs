use std::str::FromStr;

use tonic::transport::Endpoint;

use crate::net::connect::host_and_port::HostAndPort;

pub struct Peers {
    peers: Vec<Peer>,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub(crate) struct Peer {
    address: HostAndPort,
}

impl Peers {
    pub fn new(addresses: Vec<HostAndPort>) -> Self {
        return Peers {
            peers: addresses.iter().map(|addr| Peer { address: addr.clone() }).collect()
        };
    }

    pub fn get_peer_addresses(&self) -> Vec<HostAndPort> {
        return self.peers.iter().map(|peer| peer.address).collect();
    }

    pub(crate) fn all_peers(&self) -> Vec<Peer> {
        return self.peers.clone();
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