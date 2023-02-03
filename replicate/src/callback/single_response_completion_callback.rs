use std::any::Any;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex, RwLock};

use crate::callback::quorum_completion_handle::{QuorumCompletionHandle, WakerState};
use crate::net::connect::host_and_port::HostAndPort;
use crate::net::request_waiting_list::response_callback::{AnyResponse, ResponseCallback, ResponseErrorType};

pub struct SingleResponseCompletionCallback<Response: Any + Send + Sync + Debug> {
    quorum_completion_handle: QuorumCompletionHandle<Response>,
}

impl<Response: Any + Send + Sync + Debug> ResponseCallback for SingleResponseCompletionCallback<Response> {
    fn on_response(&self, from: HostAndPort, response: Result<AnyResponse, ResponseErrorType>) {
        self.quorum_completion_handle.on_response(from, response);
    }
}

impl<Response: Any + Send + Sync + Debug> SingleResponseCompletionCallback<Response> {
    pub fn new<>() -> Arc<SingleResponseCompletionCallback<Response>> {
        return Arc::new(SingleResponseCompletionCallback {
            quorum_completion_handle: QuorumCompletionHandle {
                responses: RwLock::new(HashMap::new()),
                expected_total_responses: 1,
                majority_quorum: 1,
                success_condition: Box::new(|_: &Response| true),
                waker_state: Arc::new(Mutex::new(WakerState { waker: None })),
            },
        });
    }

    pub fn handle(&self) -> &QuorumCompletionHandle<Response> {
        return self.quorum_completion_handle.borrow();
    }
}

#[cfg(all(test, feature="test_type_unit"))]
mod tests {
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr};

    use crate::callback::single_response_completion_callback::SingleResponseCompletionCallback;
    use crate::callback::single_response_completion_callback::tests::setup::{GetValueResponse, TestError};
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
    async fn successful_response() {
        let async_quorum_callback = SingleResponseCompletionCallback::<GetValueResponse>::new();
        let response_from_1 = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);

        async_quorum_callback.on_response(response_from_1.clone(), Ok(Box::new(GetValueResponse { value: "one".to_string() })));
        let handle = async_quorum_callback.handle();

        let completion_response = handle.await;

        let mut expected = HashMap::new();
        expected.insert(response_from_1, GetValueResponse { value: "one".to_string() });

        assert_eq!(1, completion_response.response_len());
        assert_eq!(&expected, completion_response.success_response().unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_response() {
        let async_quorum_callback = SingleResponseCompletionCallback::<GetValueResponse>::new();

        let response_from_1 = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);

        async_quorum_callback.on_response(response_from_1.clone(), Err(Box::new(TestError { message: "test error one".to_string() })));

        let handle = async_quorum_callback.handle();
        let completion_response = handle.await;

        assert_eq!(1, completion_response.response_len());
        let error_response = completion_response.error_response().unwrap();
        let test_error_one = error_response.get(&response_from_1).unwrap().downcast_ref::<TestError>().unwrap();
        assert_eq!("test error one", test_error_one.message);
    }
}