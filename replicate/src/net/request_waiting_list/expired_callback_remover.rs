use std::sync::Arc;
use std::thread;
use std::time::Duration;

use dashmap::DashMap;

use crate::clock::clock::Clock;
use crate::net::connect::correlation_id::CorrelationId;
use crate::net::request_waiting_list::request_waiting_list_config::RequestWaitingListConfig;
use crate::net::request_waiting_list::response_callback::TimestampedCallback;

pub(crate) struct ExpiredCallbackRemover {
    pending_requests: Arc<DashMap<CorrelationId, TimestampedCallback>>,
    expiry_after: Duration,
    clock: Arc<dyn Clock>,
}

impl ExpiredCallbackRemover {
    pub(crate) fn start(pending_requests: Arc<DashMap<CorrelationId, TimestampedCallback>>,
                        clock: Arc<dyn Clock>,
                        config: RequestWaitingListConfig) {

        let remover = ExpiredCallbackRemover { pending_requests, expiry_after: config.get_request_expiry_after(), clock };
        let pause_request_expiry_checker = config.get_pause_request_expiry_checker();

        thread::spawn(move || {
            loop {
                remover.remove();
                thread::sleep(pause_request_expiry_checker);
            }
        });
    }

    fn remove(&self) {
        self.pending_requests.retain(|correlation_id, timestamped_callback| {
            let has_expired = timestamped_callback.has_expired(&self.clock, &self.expiry_after);
            if has_expired {
                timestamped_callback.on_timeout_response(correlation_id);
                return false;
            }
            return true;
        });
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, SystemTime};

    use dashmap::DashMap;
    use crate::net::connect::correlation_id::CorrelationId;
    use crate::net::connect::host_and_port::HostAndPort;

    use crate::net::request_waiting_list::expired_callback_remover::ExpiredCallbackRemover;
    use crate::net::request_waiting_list::expired_callback_remover::tests::setup::{FutureClock, RequestTimeoutErrorResponseCallback};
    use crate::net::request_waiting_list::request_waiting_list_config::RequestWaitingListConfig;
    use crate::net::request_waiting_list::response_callback::TimestampedCallback;

    mod setup {
        use std::ops::Add;
        use std::sync::Mutex;
        use std::time::{Duration, SystemTime};

        use crate::clock::clock::Clock;
        use crate::net::connect::correlation_id::CorrelationId;
        use crate::net::connect::host_and_port::HostAndPort;
        use crate::net::request_waiting_list::request_timeout_error::RequestTimeoutError;
        use crate::net::request_waiting_list::response_callback::{AnyResponse, ResponseCallback, ResponseErrorType};

        pub struct FutureClock {
            pub duration_to_add: Duration,
        }

        pub struct RequestTimeoutErrorResponseCallback {
            pub failed_correlation_id: Mutex<CorrelationId>,
        }

        impl Clock for FutureClock {
            fn now(&self) -> SystemTime {
                return SystemTime::now().add(self.duration_to_add);
            }
        }

        impl ResponseCallback for RequestTimeoutErrorResponseCallback {
            fn on_response(&self, _: HostAndPort, response: Result<AnyResponse, ResponseErrorType>) {
                let response_error_type = response.unwrap_err();
                let request_timeout = response_error_type.downcast_ref::<RequestTimeoutError>().unwrap();
                let mut guard = self.failed_correlation_id.lock().unwrap();
                *guard = request_timeout.correlation_id;
            }
        }
    }

    #[test]
    fn error_response_on_expired_key() {
        let correlation_id: CorrelationId = 1;
        let clock = Arc::new(FutureClock { duration_to_add: Duration::from_secs(5) });
        let pending_requests = Arc::new(DashMap::new());

        let error_response_callback = Arc::new(RequestTimeoutErrorResponseCallback {
            failed_correlation_id: Mutex::new(0)
        });
        let cloned_response_callback = error_response_callback.clone();
        let target_address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);

        pending_requests.clone().insert(
            correlation_id,
            TimestampedCallback::new(error_response_callback, target_address,SystemTime::now()),
        );

        ExpiredCallbackRemover::start(
            pending_requests,
            clock,
            RequestWaitingListConfig::new(Duration::from_secs(2), Duration::from_millis(0))
        );
        thread::sleep(Duration::from_millis(5));

        let readable_response = cloned_response_callback.failed_correlation_id.lock().unwrap();
        let failed_correlation_id = *readable_response;
        assert_eq!(correlation_id, failed_correlation_id);
    }
}