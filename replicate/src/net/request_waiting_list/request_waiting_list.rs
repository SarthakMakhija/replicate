use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;

use crate::clock::clock::Clock;
use crate::net::connect::correlation_id::CorrelationId;
use crate::net::connect::host_and_port::HostAndPort;
use crate::net::request_waiting_list::expired_callback_remover::ExpiredCallbackRemover;
use crate::net::request_waiting_list::request_waiting_list_config::RequestWaitingListConfig;
use crate::net::request_waiting_list::response_callback::{AnyResponse, ResponseCallbackType, ResponseErrorType, TimestampedCallback};

pub struct RequestWaitingList {
    pending_requests: Arc<DashMap<CorrelationId, TimestampedCallback>>,
    clock: Arc<dyn Clock>,
}

impl RequestWaitingList {
    pub fn new(clock: Arc<dyn Clock>, config: RequestWaitingListConfig) -> Self <> {
        return Self::new_with_capacity(0, clock, config);
    }

    pub fn new_with_capacity(
        capacity: usize,
        clock: Arc<dyn Clock>,
        config: RequestWaitingListConfig) -> Self <> {
        let pending_requests = Arc::new(DashMap::with_capacity(capacity));
        let request_waiting_list = RequestWaitingList { pending_requests, clock: clock.clone() };

        request_waiting_list.spin_expired_callbacks_remover(config);
        return request_waiting_list;
    }

    pub fn add(&self, correlation_id: CorrelationId, target_address: HostAndPort, callback: ResponseCallbackType) {
        let timestamped_callback = TimestampedCallback::new(callback, target_address, self.clock.now());
        self.pending_requests.insert(correlation_id, timestamped_callback);
    }

    pub fn handle_response(&self, correlation_id: CorrelationId, from: HostAndPort, response: Result<AnyResponse, ResponseErrorType>) {
        let key_value_existence = self.pending_requests.remove(&correlation_id);
        if let Some(callback_by_key) = key_value_existence {
            let timestamped_callback = callback_by_key.1;
            timestamped_callback.on_response(from, response);
        }
    }

    fn spin_expired_callbacks_remover(&self, config: RequestWaitingListConfig) {
        ExpiredCallbackRemover::start(
            self.pending_requests.clone(),
            self.clock.clone(),
            config,
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::{Arc, RwLock};
    use std::thread;

    use crate::clock::clock::SystemClock;
    use crate::net::request_waiting_list::request_waiting_list::tests::setup_callbacks::{ErrorResponseCallback, RequestTimeoutErrorResponseCallback, SuccessResponseCallback};
    use crate::net::request_waiting_list::request_waiting_list::tests::setup_error::TestError;

    use super::*;

    mod setup_error {
        use std::error::Error;
        use std::fmt::{Display, Formatter};

        #[derive(Debug)]
        pub struct TestError {
            pub message: String,
        }

        impl Display for TestError {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "{}", self.message)
            }
        }

        impl Error for TestError {}
    }

    mod setup_callbacks {
        use std::collections::HashMap;
        use std::sync::RwLock;

        use crate::net::connect::host_and_port::HostAndPort;
        use crate::net::request_waiting_list::request_timeout_error::RequestTimeoutError;
        use crate::net::request_waiting_list::request_waiting_list::tests::setup_error::TestError;
        use crate::net::request_waiting_list::response_callback::{AnyResponse, ResponseCallback, ResponseErrorType};

        pub struct SuccessResponseCallback {
            pub response: RwLock<HashMap<String, String>>,
        }

        pub struct ErrorResponseCallback {
            pub error_response: RwLock<HashMap<String, String>>,
        }

        pub struct RequestTimeoutErrorResponseCallback {
            pub error_response: RwLock<HashMap<String, String>>,
        }

        impl ResponseCallback for SuccessResponseCallback {
            fn on_response(&self, _: HostAndPort, response: Result<AnyResponse, ResponseErrorType>) {
                let value = *response.unwrap().downcast().unwrap();
                self.response.write().unwrap().insert(String::from("Response"), value);
            }
        }

        impl ResponseCallback for ErrorResponseCallback {
            fn on_response(&self, _: HostAndPort, response: Result<AnyResponse, ResponseErrorType>) {
                let response_error_type = response.unwrap_err();
                let actual_error = response_error_type.downcast_ref::<TestError>().unwrap();
                self.error_response.write().unwrap().insert(String::from("Response"), actual_error.message.to_string());
            }
        }

        impl ResponseCallback for RequestTimeoutErrorResponseCallback {
            fn on_response(&self, _: HostAndPort, response: Result<AnyResponse, ResponseErrorType>) {
                let response_error_type = response.unwrap_err();
                let _ = response_error_type.downcast_ref::<RequestTimeoutError>().unwrap();
                self.error_response.write().unwrap().insert(String::from("Response"), "timeout".to_string());
            }
        }
    }

    #[test]
    fn success_response() {
        let correlation_id: CorrelationId = 1;
        let clock = Arc::new(SystemClock::new());
        let request_waiting_list = RequestWaitingList::new(
            clock.clone(),
            RequestWaitingListConfig::new(
                Duration::from_secs(100),
                Duration::from_secs(10),
            ),
        );

        let success_response_callback = Arc::new(SuccessResponseCallback { response: RwLock::new(HashMap::new()) });
        let cloned_response_callback = success_response_callback.clone();
        let from = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);

        request_waiting_list.add(correlation_id, from.clone(), success_response_callback);
        request_waiting_list.handle_response(correlation_id, from, Ok(Box::new("success response".to_string())));

        let readable_response = cloned_response_callback.response.read().unwrap();
        assert_eq!("success response", readable_response.get("Response").unwrap());
    }

    #[test]
    fn success_response_with_capacity_of_request_waiting_list() {
        let correlation_id: CorrelationId = 1;
        let clock = Arc::new(SystemClock::new());
        let request_waiting_list = RequestWaitingList::new_with_capacity(
            1,
            clock.clone(),
            RequestWaitingListConfig::new(
                Duration::from_secs(100),
                Duration::from_secs(10),
            ),
        );

        let success_response_callback = Arc::new(SuccessResponseCallback { response: RwLock::new(HashMap::new()) });
        let cloned_response_callback = success_response_callback.clone();
        let from = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);

        request_waiting_list.add(correlation_id, from.clone(), success_response_callback);
        request_waiting_list.handle_response(correlation_id, from, Ok(Box::new("success response".to_string())));

        let readable_response = cloned_response_callback.response.read().unwrap();
        assert_eq!("success response", readable_response.get("Response").unwrap());
    }

    #[test]
    fn error_response() {
        let correlation_id: CorrelationId = 1;
        let clock = Arc::new(SystemClock::new());
        let request_waiting_list = RequestWaitingList::new(
            clock.clone(),
            RequestWaitingListConfig::new(
                Duration::from_secs(100),
                Duration::from_secs(10),
            ),
        );

        let error_response_callback = Arc::new(ErrorResponseCallback { error_response: RwLock::new(HashMap::new()) });
        let cloned_response_callback = error_response_callback.clone();
        let from = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);

        request_waiting_list.add(correlation_id, from.clone(), error_response_callback);
        request_waiting_list.handle_response(correlation_id, from, Err(Box::new(TestError { message: "test error".to_string() })));

        let readable_response = cloned_response_callback.error_response.read().unwrap();
        assert_eq!("test error", readable_response.get("Response").unwrap());
    }

    #[test]
    fn error_response_on_expired_key() {
        let correlation_id: CorrelationId = 1;
        let clock = Arc::new(SystemClock::new());
        let request_waiting_list = RequestWaitingList::new(
            clock.clone(),
            RequestWaitingListConfig::new(
                Duration::from_millis(3),
                Duration::from_millis(2),
            ),
        );

        let error_response_callback = Arc::new(RequestTimeoutErrorResponseCallback { error_response: RwLock::new(HashMap::new()) });
        let cloned_response_callback = error_response_callback.clone();
        let target_address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);

        request_waiting_list.add(correlation_id, target_address, error_response_callback);
        thread::sleep(Duration::from_millis(10));

        let readable_response = cloned_response_callback.error_response.read().unwrap();
        assert_eq!("timeout", readable_response.get("Response").unwrap());
    }
}