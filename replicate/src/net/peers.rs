use std::str::FromStr;

use tonic::transport::Endpoint;

use crate::net::connect::host_and_port::HostAndPort;

pub struct Peers {
    peers: Vec<Peer>,
}

pub(crate) struct Peer {
    address: HostAndPort,
}

impl Peers {
    pub fn new(addresses: Vec<HostAndPort>) -> Self {
        return Peers {
            peers: addresses.iter().map(|addr| Peer { address: addr.clone() }).collect()
        };
    }

    pub(crate) fn get_peer_addresses(&self) -> Vec<HostAndPort> {
        return self.peers.iter().map(|peer| peer.address).collect();
    }
}

impl Peer {
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
}