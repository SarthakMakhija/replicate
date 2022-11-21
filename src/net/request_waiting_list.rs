use std::borrow::BorrowMut;
use std::error::Error;
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
        return RequestWaitingList { pending_requests: DashMap::new() };
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
    use std::fmt::{Display, Formatter};
    use std::rc::Rc;
    use crate::net::response_callback::ResponseCallback;
    use super::*;

    #[test]
    fn test_success_response() {
        struct SuccessResponseCallback {
            response: RefCell<String>,
        }
        impl ResponseCallback<String> for SuccessResponseCallback {
            fn on_response(&self, response: Result<String, ResponseErrorType>) {
                self.response.borrow_mut().push_str(response.unwrap().as_str());
            }
        }

        let key: i32 = 1;
        let mut request_waiting_list = RequestWaitingList::<i32, String>::new();

        let success_response_callback = Rc::new(SuccessResponseCallback { response: RefCell::new(String::from("")) });
        let cloned_response_callback = success_response_callback.clone();

        request_waiting_list.add(key, success_response_callback);
        request_waiting_list.handle_response(key, Ok("success response".to_string()));
        assert_eq!("success response", cloned_response_callback.response.take());
    }

    #[test]
    fn test_error_response() {
        #[derive(Debug)]
        struct TestError {
            details: String,
        }
        impl Display for TestError {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "{}", self.details)
            }
        }
        impl Error for TestError {}

        struct ErrorResponseCallback {
            error_message: RefCell<String>,
        }
        impl ResponseCallback<String> for ErrorResponseCallback {
            fn on_response(&self, response: Result<String, ResponseErrorType>) {
                let response_error_type = response.unwrap_err();
                let actual_error = response_error_type.downcast_ref::<TestError>().unwrap();
                self.error_message.borrow_mut().push_str(actual_error.details.as_str());
            }
        }

        let key: i32 = 1;
        let mut request_waiting_list = RequestWaitingList::<i32, String>::new();

        let error_response_callback = Rc::new(ErrorResponseCallback { error_message: RefCell::new("".to_string()) });
        let cloned_response_callback = error_response_callback.clone();

        request_waiting_list.add(key, error_response_callback);
        request_waiting_list.handle_response(key, Err(Box::new(TestError { details: "test error".to_string() })));

        assert_eq!("test error", cloned_response_callback.error_message.take());
    }
}