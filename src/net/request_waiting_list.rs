use std::borrow::BorrowMut;
use std::hash::Hash;

use dashmap::DashMap;

use crate::net::response_callback::{ResponseCallbackType, ResponseErrorType};

pub struct RequestWaitingList<Key, Response>
    where Key: Eq + Hash, {
    pending_requests: DashMap<Key, ResponseCallbackType<Response>>,
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
        self.pending_requests.borrow_mut().insert(key, callback);
    }

    pub fn handle_response(&mut self, key: Key, response: Result<Response, ResponseErrorType>) {
        let key_value_existence = self.pending_requests.remove(&key);
        if let Some(callback_by_key) = key_value_existence {
            let callback = callback_by_key.1;
            callback.on_response(response);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

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
        use std::cell::RefCell;

        use crate::net::request_waiting_list::tests::setup_error::TestError;
        use crate::net::response_callback::{ResponseCallback, ResponseErrorType};

        pub struct SuccessResponseCallback {
            pub response: RefCell<String>,
        }

        pub struct ErrorResponseCallback {
            pub error_message: RefCell<String>,
        }

        impl ResponseCallback<String> for SuccessResponseCallback {
            fn on_response(&self, response: Result<String, ResponseErrorType>) {
                self.response.borrow_mut().push_str(response.unwrap().as_str());
            }
        }

        impl ResponseCallback<String> for ErrorResponseCallback {
            fn on_response(&self, response: Result<String, ResponseErrorType>) {
                let response_error_type = response.unwrap_err();
                let actual_error = response_error_type.downcast_ref::<TestError>().unwrap();
                self.error_message.borrow_mut().push_str(actual_error.message.as_str());
            }
        }
    }

    #[test]
    fn success_response() {
        let key: i32 = 1;
        let mut request_waiting_list = RequestWaitingList::<i32, String>::new();

        let success_response_callback = Rc::new(SuccessResponseCallback { response: RefCell::new(String::from("")) });
        let cloned_response_callback = success_response_callback.clone();

        request_waiting_list.add(key, success_response_callback);
        request_waiting_list.handle_response(key, Ok("success response".to_string()));

        assert_eq!("success response", cloned_response_callback.response.take());
    }

    #[test]
    fn success_response_with_capacity_of_request_waiting_list() {
        let key: i32 = 1;
        let mut request_waiting_list = RequestWaitingList::<i32, String>::new_with_capacity(1);

        let success_response_callback = Rc::new(SuccessResponseCallback { response: RefCell::new(String::from("")) });
        let cloned_response_callback = success_response_callback.clone();

        request_waiting_list.add(key, success_response_callback);
        request_waiting_list.handle_response(key, Ok("success response".to_string()));

        assert_eq!("success response", cloned_response_callback.response.take());
    }

    #[test]
    fn error_response() {
        let key: i32 = 1;
        let mut request_waiting_list = RequestWaitingList::<i32, String>::new();

        let error_response_callback = Rc::new(ErrorResponseCallback { error_message: RefCell::new("".to_string()) });
        let cloned_response_callback = error_response_callback.clone();

        request_waiting_list.add(key, error_response_callback);
        request_waiting_list.handle_response(key, Err(Box::new(TestError { message: "test error".to_string() })));

        assert_eq!("test error", cloned_response_callback.error_message.take());
    }
}