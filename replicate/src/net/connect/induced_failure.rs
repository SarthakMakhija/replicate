use dashmap::DashMap;
use crate::net::connect::host_and_port::HostAndPort;

pub(crate) struct InducedFailure {
    drop_requests_to: DashMap<HostAndPort, bool>,
    drop_requests_after: DashMap<HostAndPort, u64>,
    request_count_by_host_and_port: DashMap<HostAndPort, u64>,
}

impl InducedFailure {
    pub(crate) fn new() -> Self {
        return InducedFailure {
            drop_requests_to: DashMap::new(),
            drop_requests_after: DashMap::new(),
            request_count_by_host_and_port: DashMap::new(),
        };
    }

    pub(crate) fn drop_requests_to(&self, host_and_port: HostAndPort) {
        self.drop_requests_to.insert(host_and_port, true);
    }

    pub(crate) fn drop_requests_after(&self, count: u64, host_and_port: HostAndPort) {
        self.drop_requests_to.remove(&host_and_port);
        self.request_count_by_host_and_port.remove(&host_and_port);
        self.drop_requests_after.insert(host_and_port, count);
    }

    pub(crate) fn should_drop_to(&self, target_address: &HostAndPort) -> bool {
        if self.drop_requests_to.contains_key(&target_address) {
            println!("dropping request to {:?}", target_address);
            return true;
        }
        if let Some(entry) = self.drop_requests_after.get(&target_address) {
            let request_count: u64 = match self.request_count_by_host_and_port.get(&target_address) {
                None => 0,
                Some(count_by_host) => *count_by_host.value()
            };
            if request_count >= *(entry.value()) {
                return true;
            }
            return false;
        }
        return false;
    }

    pub(crate) fn increase_request_count_for(&self, target_address: HostAndPort) {
        let mut count = self.request_count_by_host_and_port.entry(target_address).or_insert(0);
        let count = count.pair_mut().1;
        *count = *count + 1;
    }
}

#[cfg(test)]
#[cfg(feature = "test_type_simulation")]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use crate::net::connect::host_and_port::HostAndPort;
    use crate::net::connect::induced_failure::InducedFailure;

    #[test]
    fn should_drop_request() {
        let induced_failure = InducedFailure::new();
        let address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8713);

        induced_failure.drop_requests_to(address);
        assert!(induced_failure.should_drop_to(&address));
    }

    #[test]
    fn should_not_drop_request() {
        let induced_failure = InducedFailure::new();
        let address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8713);

        assert_eq!(false, induced_failure.should_drop_to(&address));
    }

    #[test]
    fn should_drop_request_given_request_count_matches_the_expected_count() {
        let induced_failure = InducedFailure::new();
        let address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8713);

        induced_failure.drop_requests_after(2, address.clone());
        induced_failure.request_count_by_host_and_port.insert(address.clone(), 2);

        assert!(induced_failure.should_drop_to(&address));
    }

    #[test]
    fn should_drop_request_given_request_count_matches_the_expected_count_of_zero() {
        let induced_failure = InducedFailure::new();
        let address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8713);

        induced_failure.drop_requests_after(0, address.clone());

        assert!(induced_failure.should_drop_to(&address));
    }

    #[test]
    fn should_drop_request_given_request_count_exceeds_the_expected_count() {
        let induced_failure = InducedFailure::new();
        let address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8713);

        induced_failure.drop_requests_after(2, address.clone());
        induced_failure.request_count_by_host_and_port.insert(address.clone(), 3);

        assert!(induced_failure.should_drop_to(&address));
    }
}