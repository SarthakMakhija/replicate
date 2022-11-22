use std::borrow::BorrowMut;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use dashmap::DashMap;

use crate::clock::clock::{Clock, SystemClock};
use crate::net::request_timeout_error::RequestTimeoutError;
use crate::net::response_callback::{ResponseCallbackType, ResponseErrorType, TimestampedCallback};

pub struct RequestWaitingList<Key, Response>
    where Key: Eq + Hash + Send + Sync + Debug + 'static, {
    pending_requests: Arc<DashMap<Key, TimestampedCallback<Response>>>,
    clock: Arc<dyn Clock>,
    expire_requests_after: Duration,
}

impl<Key, Response: 'static> RequestWaitingList<Key, Response>
    where Key: Eq + Hash + Send + Sync + Debug + 'static, {
    pub fn new(clock: Arc<dyn Clock>, expire_requests_after: Duration, pause_expired_callbacks_remover_every: Duration) -> RequestWaitingList<Key, Response> {
        return Self::new_with_capacity(0, clock, expire_requests_after, pause_expired_callbacks_remover_every);
    }

    pub fn new_with_capacity(
        capacity: usize,
        clock: Arc<dyn Clock>,
        expire_requests_after: Duration,
        pause_expired_callbacks_remover_every: Duration) -> RequestWaitingList<Key, Response> {
        let pending_requests = Arc::new(DashMap::with_capacity(capacity));
        let request_waiting_list = RequestWaitingList { pending_requests, clock: clock.clone(), expire_requests_after };

        request_waiting_list.spin_expired_callbacks_remover(pause_expired_callbacks_remover_every);
        return request_waiting_list;
    }

    pub fn add(&mut self, key: Key, callback: ResponseCallbackType<Response>) {
        let timestamped_callback = TimestampedCallback::new(callback, self.clock.now());
        self.pending_requests.borrow_mut().insert(key, timestamped_callback);
    }

    pub fn handle_response(&mut self, key: Key, response: Result<Response, ResponseErrorType>) {
        let key_value_existence = self.pending_requests.remove(&key);
        if let Some(callback_by_key) = key_value_existence {
            let timestamped_callback = callback_by_key.1;
            timestamped_callback.on_response(response);
        }
    }

    fn spin_expired_callbacks_remover(&self, pause_expired_callbacks_remover_every: Duration) {
        let pending_requests = self.pending_requests.clone();
        let expiry_after = self.expire_requests_after;
        let clock = self.clock.clone();

        thread::spawn(move || {
            loop {
                Self::remove(&pending_requests, &expiry_after, &clock);
                thread::sleep(pause_expired_callbacks_remover_every);
            }
        });
    }

    fn remove(pending_requests: &Arc<DashMap<Key, TimestampedCallback<Response>>>, expiry_after: &Duration, clock: &Arc<dyn Clock>) {
        pending_requests.retain(|_, timestamped_callback| {
            let has_expired = timestamped_callback.has_expired(&clock, &expiry_after);
            if has_expired {
                timestamped_callback.on_response(Err(Box::new(RequestTimeoutError {})));
                return false;
            }
            return true;
        });
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    use crate::net::request_waiting_list::tests::setup_callbacks::{ErrorResponseCallback, RequestTimeoutErrorResponseCallback, SuccessResponseCallback};
    use crate::net::request_waiting_list::tests::setup_error::TestError;

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

        use crate::net::request_timeout_error::RequestTimeoutError;
        use crate::net::request_waiting_list::tests::setup_error::TestError;
        use crate::net::response_callback::{ResponseCallback, ResponseErrorType};

        pub struct SuccessResponseCallback {
            pub response: RwLock<HashMap<String, String>>,
        }

        pub struct ErrorResponseCallback {
            pub error_response: RwLock<HashMap<String, String>>,
        }

        pub struct RequestTimeoutErrorResponseCallback {
            pub error_response: RwLock<HashMap<String, String>>,
        }

        impl ResponseCallback<String> for SuccessResponseCallback {
            fn on_response(&self, response: Result<String, ResponseErrorType>) {
                self.response.write().unwrap().insert(String::from("Response"), response.unwrap());
            }
        }

        impl ResponseCallback<String> for ErrorResponseCallback {
            fn on_response(&self, response: Result<String, ResponseErrorType>) {
                let response_error_type = response.unwrap_err();
                let actual_error = response_error_type.downcast_ref::<TestError>().unwrap();
                self.error_response.write().unwrap().insert(String::from("Response"), actual_error.message.to_string());
            }
        }

        impl ResponseCallback<String> for RequestTimeoutErrorResponseCallback {
            fn on_response(&self, response: Result<String, ResponseErrorType>) {
                let response_error_type = response.unwrap_err();
                let _ = response_error_type.downcast_ref::<RequestTimeoutError>().unwrap();
                self.error_response.write().unwrap().insert(String::from("Response"), "timeout".to_string());
            }
        }
    }

    #[test]
    fn success_response() {
        let key: i32 = 1;
        let clock = Arc::new(SystemClock::new());
        let mut request_waiting_list = RequestWaitingList::<i32, String>::new(
            clock.clone(),
            Duration::from_secs(100),
            Duration::from_secs(10),
        );

        let success_response_callback = Arc::new(SuccessResponseCallback { response: RwLock::new(HashMap::new()) });
        let cloned_response_callback = success_response_callback.clone();

        request_waiting_list.add(key, success_response_callback);
        request_waiting_list.handle_response(key, Ok("success response".to_string()));

        let readable_response = cloned_response_callback.response.read().unwrap();
        assert_eq!("success response", readable_response.get("Response").unwrap());
    }

    #[test]
    fn success_response_with_capacity_of_request_waiting_list() {
        let key: i32 = 1;
        let clock = Arc::new(SystemClock::new());
        let mut request_waiting_list = RequestWaitingList::<i32, String>::new_with_capacity(
            1,
            clock.clone(),
            Duration::from_secs(100),
            Duration::from_secs(10),
        );

        let success_response_callback = Arc::new(SuccessResponseCallback { response: RwLock::new(HashMap::new()) });
        let cloned_response_callback = success_response_callback.clone();

        request_waiting_list.add(key, success_response_callback);
        request_waiting_list.handle_response(key, Ok("success response".to_string()));

        let readable_response = cloned_response_callback.response.read().unwrap();
        assert_eq!("success response", readable_response.get("Response").unwrap());
    }

    #[test]
    fn error_response() {
        let key: i32 = 1;
        let clock = Arc::new(SystemClock::new());
        let mut request_waiting_list = RequestWaitingList::<i32, String>::new(
            clock.clone(),
            Duration::from_secs(100),
            Duration::from_secs(10),
        );

        let error_response_callback = Arc::new(ErrorResponseCallback { error_response: RwLock::new(HashMap::new()) });
        let cloned_response_callback = error_response_callback.clone();

        request_waiting_list.add(key, error_response_callback);
        request_waiting_list.handle_response(key, Err(Box::new(TestError { message: "test error".to_string() })));

        let readable_response = cloned_response_callback.error_response.read().unwrap();
        assert_eq!("test error", readable_response.get("Response").unwrap());
    }

    #[test]
    fn error_response_on_expired_key() {
        let key: i32 = 1;
        let clock = Arc::new(SystemClock::new());
        let mut request_waiting_list = RequestWaitingList::<i32, String>::new(
            clock.clone(),
            Duration::from_millis(3),
            Duration::from_millis(2),
        );

        let error_response_callback = Arc::new(RequestTimeoutErrorResponseCallback { error_response: RwLock::new(HashMap::new()) });
        let cloned_response_callback = error_response_callback.clone();

        request_waiting_list.add(key, error_response_callback);
        thread::sleep(Duration::from_millis(10));

        let readable_response = cloned_response_callback.error_response.read().unwrap();
        assert_eq!("timeout", readable_response.get("Response").unwrap());
    }
}