use std::any::{Any, TypeId};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::sync::{Arc, Mutex, RwLock};

use crate::consensus::quorum::quorum_completion_handle::{QuorumCompletionHandle, WakerState};
use crate::net::connect::host_and_port::HostAndPort;
use crate::net::request_waiting_list::response_callback::{AnyResponse, ResponseCallback, ResponseErrorType};

pub type SuccessCondition<Response> = Box<dyn Fn(&Response) -> bool + Send + Sync>;

#[derive(Debug)]
pub struct UnexpectedQuorumCallbackResponseError {
    pub response_type_id: TypeId
}

impl Display for UnexpectedQuorumCallbackResponseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "UnexpectedQuorumCallbackResponseError, received a response type id {:?}", self.response_type_id)
    }
}

impl std::error::Error for UnexpectedQuorumCallbackResponseError {}

pub struct AsyncQuorumCallback<Response: Any + Send + Sync + Debug> {
    quorum_completion_handle: QuorumCompletionHandle<Response>,
}

impl<Response: Any + Send + Sync + Debug> ResponseCallback for AsyncQuorumCallback<Response> {
    fn on_response(&self, from: HostAndPort, response: Result<AnyResponse, ResponseErrorType>) {
        self.quorum_completion_handle.on_response(from, response);
    }
}

impl<Response: Any + Send + Sync + Debug> AsyncQuorumCallback<Response> {
    pub fn new<>(expected_responses: usize) -> Arc<AsyncQuorumCallback<Response>> {
        return Self::new_with_success_condition(expected_responses, Box::new(|_: &Response| true));
    }

    pub fn new_with_success_condition<>(expected_total_responses: usize, success_condition: SuccessCondition<Response>) -> Arc<AsyncQuorumCallback<Response>> {
        return Arc::new(AsyncQuorumCallback {
            quorum_completion_handle: QuorumCompletionHandle {
                responses: RwLock::new(HashMap::new()),
                expected_total_responses,
                majority_quorum: expected_total_responses / 2 + 1,
                success_condition,
                waker_state: Arc::new(Mutex::new(WakerState { waker: None })),
            },
        });
    }

    pub fn handle(&self) -> &QuorumCompletionHandle<Response> {
        return self.quorum_completion_handle.borrow();
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::BorrowMut;
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    use crate::consensus::quorum::async_quorum_callback::{AsyncQuorumCallback, UnexpectedQuorumCallbackResponseError};
    use crate::consensus::quorum::async_quorum_callback::tests::setup::{GetValueResponse, PutValueResponse, TestError};
    use crate::net::connect::host_and_port::HostAndPort;
    use crate::net::request_waiting_list::response_callback::ResponseCallback;

    mod setup {
        use std::error::Error;
        use std::fmt::{Display, Formatter};

        #[derive(Eq, PartialEq, Debug)]
        pub struct GetValueResponse {
            pub value: String,
        }

        #[derive(Eq, PartialEq, Debug)]
        pub struct PutValueResponse {
            pub key: String,
            pub value: String,
        }

        #[derive(Debug, Eq, PartialEq)]
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unexpected_response_error() {
        let async_quorum_callback = AsyncQuorumCallback::<GetValueResponse>::new(2);
        let response_from_1 = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
        let response_from_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50052);

        async_quorum_callback.on_response(response_from_1.clone(), Ok(Box::new(GetValueResponse { value: "one".to_string() })));
        async_quorum_callback.on_response(response_from_other.clone(), Ok(Box::new(PutValueResponse { key: "key".to_string(), value: "two".to_string() })));
        let handle = async_quorum_callback.handle();

        let completion_response = handle.await;

        assert!(completion_response.is_error());
        let error_responses = completion_response.error_response().unwrap();
        let result_downcast_error = error_responses.get(&response_from_other).unwrap().downcast_ref::<UnexpectedQuorumCallbackResponseError>();
        assert!(result_downcast_error.is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn successful_responses() {
        let async_quorum_callback = AsyncQuorumCallback::<GetValueResponse>::new(3);
        let response_from_1 = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
        let response_from_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50052);

        async_quorum_callback.on_response(response_from_1.clone(), Ok(Box::new(GetValueResponse { value: "one".to_string() })));
        async_quorum_callback.on_response(response_from_other.clone(), Ok(Box::new(GetValueResponse { value: "two".to_string() })));
        let handle = async_quorum_callback.handle();

        let completion_response = handle.await;

        let mut expected = HashMap::new();
        expected.insert(response_from_1, GetValueResponse { value: "one".to_string() });
        expected.insert(response_from_other, GetValueResponse { value: "two".to_string() });

        assert_eq!(2, completion_response.response_len());
        assert_eq!(&expected, completion_response.success_response().unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn successful_responses_with_success_condition() {
        let success_condition = Box::new(|response: &GetValueResponse| response.value == "ok" );
        let async_quorum_callback = AsyncQuorumCallback::<GetValueResponse>::new_with_success_condition(2, success_condition);
        let response_from_1 = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
        let response_from_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50052);

        async_quorum_callback.on_response(response_from_1.clone(), Ok(Box::new(GetValueResponse { value: "ok".to_string() })));
        async_quorum_callback.on_response(response_from_other.clone(), Ok(Box::new(GetValueResponse { value: "ok".to_string() })));
        let handle = async_quorum_callback.handle();

        let completion_response = handle.await;

        let mut expected = HashMap::new();
        expected.insert(response_from_1, GetValueResponse { value: "ok".to_string() });
        expected.insert(response_from_other, GetValueResponse { value: "ok".to_string() });

        assert_eq!(2, completion_response.response_len());
        assert_eq!(&expected, completion_response.success_response().unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn successful_responses_after_delay() {
        let async_quorum_callback = AsyncQuorumCallback::<GetValueResponse>::new(3);
        let async_quorum_callback_one = Arc::new(RwLock::new(async_quorum_callback));
        let async_quorum_callback_two = async_quorum_callback_one.clone();
        let async_quorum_callback_three = async_quorum_callback_two.clone();

        let response_from_1 = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
        let response_from_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50052);

        async_quorum_callback_one.read().unwrap().on_response(response_from_1.clone(), Ok(Box::new(GetValueResponse { value: "one".to_string() })));

        let second_response_handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            async_quorum_callback_two.read().unwrap().on_response(response_from_other.clone(), Ok(Box::new(GetValueResponse { value: "two".to_string() })));
        });
        second_response_handle.await.unwrap();

        let mut guard = async_quorum_callback_three.write().unwrap();
        let handle = guard.borrow_mut().handle();

        let completion_response = handle.await;

        let mut expected = HashMap::new();
        expected.insert(response_from_1, GetValueResponse { value: "one".to_string() });
        expected.insert(response_from_other, GetValueResponse { value: "two".to_string() });

        assert_eq!(2, completion_response.response_len());
        assert_eq!(&expected, completion_response.success_response().unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_responses() {
        let async_quorum_callback = AsyncQuorumCallback::<GetValueResponse>::new(3);

        let response_from_1 = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
        let response_from_2 = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50052);
        let response_from_3 = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50053);

        async_quorum_callback.on_response(response_from_1.clone(), Err(Box::new(TestError { message: "test error one".to_string() })));
        async_quorum_callback.on_response(response_from_2.clone(), Err(Box::new(TestError { message: "test error two".to_string() })));
        async_quorum_callback.on_response(response_from_3.clone(), Ok(Box::new(GetValueResponse { value: "two".to_string() })));

        let handle = async_quorum_callback.handle();

        let completion_response = handle.await;

        assert_eq!(2, completion_response.response_len());
        let error_responses = completion_response.error_response().unwrap();
        let test_error_one = error_responses.get(&response_from_1).unwrap().downcast_ref::<TestError>().unwrap();
        assert_eq!("test error one", test_error_one.message);

        let test_error_two = error_responses.get(&response_from_2).unwrap().downcast_ref::<TestError>().unwrap();
        assert_eq!("test error two", test_error_two.message);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn response_with_success_condition_not_met() {
        let success_condition = Box::new(|response: &GetValueResponse| response.value == "ok" );
        let async_quorum_callback = AsyncQuorumCallback::<GetValueResponse>::new_with_success_condition(2, success_condition);
        let response_from_1 = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
        let response_from_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50052);

        async_quorum_callback.on_response(response_from_1.clone(), Ok(Box::new(GetValueResponse { value: "ok".to_string() })));
        async_quorum_callback.on_response(response_from_other.clone(), Ok(Box::new(GetValueResponse { value: "not ok".to_string() })));
        let handle = async_quorum_callback.handle();

        let completion_response = handle.await;

        let mut expected = HashMap::new();
        expected.insert(response_from_1, GetValueResponse { value: "ok".to_string() });
        expected.insert(response_from_other, GetValueResponse { value: "ok".to_string() });

        assert!(completion_response.is_success_condition_not_met())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_responses_with_success_condition_not_met() {
        let success_condition = Box::new(|response: &GetValueResponse| response.value == "ok" );
        let async_quorum_callback = AsyncQuorumCallback::<GetValueResponse>::new_with_success_condition(
            3,
            success_condition
        );

        let response_from_1 = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
        let response_from_2 = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50052);
        let response_from_3 = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50053);

        async_quorum_callback.on_response(response_from_1.clone(), Err(Box::new(TestError { message: "test error one".to_string() })));
        async_quorum_callback.on_response(response_from_2.clone(), Err(Box::new(TestError { message: "test error two".to_string() })));
        async_quorum_callback.on_response(response_from_3.clone(), Ok(Box::new(GetValueResponse { value: "not ok".to_string() })));

        let handle = async_quorum_callback.handle();

        let completion_response = handle.await;

        assert_eq!(2, completion_response.response_len());
        let error_responses = completion_response.error_response().unwrap();
        let test_error_one = error_responses.get(&response_from_1).unwrap().downcast_ref::<TestError>().unwrap();
        assert_eq!("test error one", test_error_one.message);

        let test_error_two = error_responses.get(&response_from_2).unwrap().downcast_ref::<TestError>().unwrap();
        assert_eq!("test error two", test_error_two.message);
    }
}