use std::borrow::BorrowMut;
use std::hash::Hash;
use std::time::Instant;

use dashmap::DashMap;

use crate::net::response_callback::{ResponseCallbackType, ResponseErrorType, TimestampedCallback};

pub struct RequestWaitingList<Key, Response>
    where Key: Eq + Hash, {
    pending_requests: DashMap<Key, TimestampedCallback<Response>>,
}

impl<Key, Response> RequestWaitingList<Key, Response>
    where Key: Eq + Hash, {
    pub fn new() -> RequestWaitingList<Key, Response> {
        return Self::new_with_capacity(0);
    }

    pub fn new_with_capacity(capacity: usize) -> RequestWaitingList<Key, Response> {
        return RequestWaitingList { pending_requests: DashMap::with_capacity(capacity) };
    }

    pub fn add(&mut self, key: Key, callback: ResponseCallbackType<Response>) {
        let timestamped_callback = TimestampedCallback::new(callback, Instant::now());
        self.pending_requests.borrow_mut().insert(key, timestamped_callback);
    }

    pub fn handle_response(&mut self, key: Key, response: Result<Response, ResponseErrorType>) {
        let key_value_existence = self.pending_requests.remove(&key);
        if let Some(callback_by_key) = key_value_existence {
            let timestamped_callback = callback_by_key.1;
            timestamped_callback.on_response(response);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    use crate::net::request_waiting_list::tests::setup_callbacks::{ErrorResponseCallback, SuccessResponseCallback};
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

        use crate::net::request_waiting_list::tests::setup_error::TestError;
        use crate::net::response_callback::{ResponseCallback, ResponseErrorType};

        pub struct SuccessResponseCallback {
            pub response: RwLock<HashMap<String, String>>,
        }

        pub struct ErrorResponseCallback {
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
    }

    #[test]
    fn success_response() {
        let key: i32 = 1;
        let mut request_waiting_list = RequestWaitingList::<i32, String>::new();

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
        let mut request_waiting_list = RequestWaitingList::<i32, String>::new_with_capacity(1);

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
        let mut request_waiting_list = RequestWaitingList::<i32, String>::new();

        let error_response_callback = Arc::new(ErrorResponseCallback { error_response: RwLock::new(HashMap::new()) });
        let cloned_response_callback = error_response_callback.clone();

        request_waiting_list.add(key, error_response_callback);
        request_waiting_list.handle_response(key, Err(Box::new(TestError { message: "test error".to_string() })));

        let readable_response = cloned_response_callback.error_response.read().unwrap();
        assert_eq!("test error", readable_response.get("Response").unwrap());
    }
}