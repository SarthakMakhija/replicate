use std::any::Any;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::clock::clock::Clock;
use crate::net::connect::correlation_id::CorrelationId;
use crate::net::connect::host_and_port::HostAndPort;
use crate::net::request_waiting_list::request_timeout_error::RequestTimeoutError;

pub type ResponseErrorType = Box<dyn Error + Send + Sync>;

pub type AnyResponse = Box<dyn Any>;

pub(crate) type ResponseCallbackType = Arc<dyn ResponseCallback + 'static>;

pub trait ResponseCallback: Send + Sync {
    fn on_response(&self, from: HostAndPort, response: Result<AnyResponse, ResponseErrorType>);
}

pub(crate) struct TimestampedCallback {
    callback: ResponseCallbackType,
    target_address: HostAndPort,
    creation_time: SystemTime,
}

impl TimestampedCallback {
    pub(crate) fn new(callback: ResponseCallbackType, target_address: HostAndPort, creation_time: SystemTime) -> Self {
        return TimestampedCallback {
            callback,
            target_address,
            creation_time,
        };
    }

    pub(crate) fn on_response(&self, from: HostAndPort, response: Result<AnyResponse, ResponseErrorType>) {
        self.callback.on_response(from, response);
    }

    pub(crate) fn on_timeout_response(&self, correlation_id: &CorrelationId) {
        //eprintln!("Request timed out for correlation_id {}", correlation_id);
        self.callback.on_response(self.target_address, Err(Box::new(RequestTimeoutError {
            correlation_id: *correlation_id
        })));
    }

    pub(crate) fn has_expired(&self, clock: &Box<dyn Clock>, expiry_after: &Duration) -> bool {
        return clock.duration_since(self.creation_time).ge(expiry_after);
    }
}

#[cfg(all(test, feature="test_type_unit"))]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use crate::clock::clock::{Clock, SystemClock};
    use crate::net::connect::host_and_port::HostAndPort;
    use crate::net::request_waiting_list::response_callback::tests::setup::{FutureClock, NothingCallback};
    use crate::net::request_waiting_list::response_callback::TimestampedCallback;

    mod setup {
        use std::ops::Add;
        use std::time::{Duration, SystemTime};

        use crate::clock::clock::Clock;
        use crate::net::connect::host_and_port::HostAndPort;
        use crate::net::request_waiting_list::response_callback::{AnyResponse, ResponseCallback, ResponseErrorType};

        #[derive(Clone)]
        pub struct FutureClock {
            pub duration_to_add: Duration,
        }

        pub struct NothingCallback {}

        impl Clock for FutureClock {
            fn now(&self) -> SystemTime {
                return SystemTime::now().add(self.duration_to_add);
            }
        }

        impl ResponseCallback for NothingCallback {
            fn on_response(&self, _: HostAndPort, _: Result<AnyResponse, ResponseErrorType>) {}
        }
    }

    #[test]
    fn has_expired() {
        let target_address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
        let timestamped_callback = TimestampedCallback::new(Arc::new(NothingCallback {}), target_address, SystemTime::now());
        let clock: Box<dyn Clock> = Box::new(FutureClock { duration_to_add: Duration::from_secs(5) });

        let has_expired = timestamped_callback.has_expired(&clock, &Duration::from_secs(2));
        assert!(has_expired);
    }

    #[test]
    fn has_not_expired() {
        let target_address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
        let timestamped_callback = TimestampedCallback::new(Arc::new(NothingCallback {}), target_address, SystemTime::now());
        let clock: Box<dyn Clock> = Box::new(SystemClock::new());

        let has_expired = timestamped_callback.has_expired(&clock, &Duration::from_secs(100));
        assert_eq!(false, has_expired);
    }
}