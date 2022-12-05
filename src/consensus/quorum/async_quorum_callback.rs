use std::borrow::BorrowMut;
use std::fmt::Debug;
use std::sync::RwLock;
use crate::consensus::quorum::quorum_completion_handle::QuorumCompletionHandle;

use crate::net::request_waiting_list::response_callback::{ResponseCallback, ResponseErrorType};

pub(crate) type SuccessCondition<Response> = Box<dyn Fn(&Response) -> bool + Send + Sync>;

pub(crate) struct AsyncQuorumCallback<Response: Send + Sync + Unpin + Debug> {
    quorum_completion_handle: QuorumCompletionHandle<Response>,
}

impl<Response: Send + Sync + Unpin + Debug> ResponseCallback<Response> for AsyncQuorumCallback<Response> {
    fn on_response(&self, response: Result<Response, ResponseErrorType>) {
        self.quorum_completion_handle.on_response(response);
    }
}

impl<Response: Send + Sync + Unpin + Debug> AsyncQuorumCallback<Response> {
    pub(crate) fn new<>(expected_responses: usize) -> Self <> {
        return Self::new_with_success_condition(expected_responses, Box::new(|_: &Response| true));
    }

    pub(crate) fn new_with_success_condition<>(expected_total_responses: usize, success_condition: SuccessCondition<Response>) -> Self <> {
        return AsyncQuorumCallback {
            quorum_completion_handle: QuorumCompletionHandle {
                responses: RwLock::new(Vec::with_capacity(expected_total_responses)),
                expected_total_responses,
                majority_quorum: expected_total_responses / 2 + 1,
                success_condition,
                waker: None,
            },
        };
    }

    pub(crate) fn handle(&mut self) -> &mut QuorumCompletionHandle<Response> {
        return self.quorum_completion_handle.borrow_mut();
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::BorrowMut;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    use crate::consensus::quorum::async_quorum_callback::AsyncQuorumCallback;
    use crate::consensus::quorum::async_quorum_callback::tests::setup::{GetValueResponse, TestError};
    use crate::net::request_waiting_list::response_callback::ResponseCallback;

    mod setup {
        use std::error::Error;
        use std::fmt::{Display, Formatter};

        #[derive(Eq, PartialEq, Debug)]
        pub struct GetValueResponse {
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
    async fn successful_responses() {
        let mut async_quorum_callback = AsyncQuorumCallback::new(3);
        async_quorum_callback.on_response(Ok(GetValueResponse { value: "one".to_string() }));
        async_quorum_callback.on_response(Ok(GetValueResponse { value: "two".to_string() }));
        let handle = async_quorum_callback.handle();

        let completion_response = handle.await;

        assert_eq!(2, completion_response.response_len());
        assert_eq!(&vec![GetValueResponse { value: "two".to_string() },
                         GetValueResponse { value: "one".to_string() }],
                   completion_response.success_responses().unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn successful_responses_after_delay() {
        let async_quorum_callback = AsyncQuorumCallback::new(3);
        let async_quorum_callback_one = Arc::new(RwLock::new(async_quorum_callback));
        let async_quorum_callback_two = async_quorum_callback_one.clone();
        let async_quorum_callback_three = async_quorum_callback_two.clone();

        async_quorum_callback_one.read().unwrap().on_response(Ok(GetValueResponse { value: "one".to_string() }));

        let second_response_handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            async_quorum_callback_two.read().unwrap().on_response(Ok(GetValueResponse { value: "two".to_string() }));
        });
        second_response_handle.await.unwrap();

        let mut guard = async_quorum_callback_three.write().unwrap();
        let handle = guard.borrow_mut().handle();

        let completion_response = handle.await;

        assert_eq!(2, completion_response.response_len());
        assert_eq!(&vec![GetValueResponse { value: "two".to_string() },
                         GetValueResponse { value: "one".to_string() }],
                   completion_response.success_responses().unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_responses() {
        let mut async_quorum_callback = AsyncQuorumCallback::new(3);
        async_quorum_callback.on_response(Err(Box::new(TestError { message: "test error one".to_string() })));
        async_quorum_callback.on_response(Err(Box::new(TestError { message: "test error two".to_string() })));
        async_quorum_callback.on_response(Ok(GetValueResponse { value: "two".to_string() }));

        let handle = async_quorum_callback.handle();

        let completion_response = handle.await;

        assert_eq!(2, completion_response.response_len());
        let error_responses = completion_response.error_responses().unwrap();
        let test_error_one = error_responses.as_slice().get(0).unwrap().downcast_ref::<TestError>().unwrap();
        assert_eq!("test error two", test_error_one.message);

        let test_error_two = error_responses.as_slice().get(1).unwrap().downcast_ref::<TestError>().unwrap();
        assert_eq!("test error one", test_error_two.message);
    }
}