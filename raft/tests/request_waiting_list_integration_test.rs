use std::error::Error;
use std::fmt::{Display, Formatter};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;
use raft::clock::clock::SystemClock;

use raft::consensus::quorum::async_quorum_callback::AsyncQuorumCallback;
use raft::net::connect::host_and_port::HostAndPort;
use raft::net::request_waiting_list::request_waiting_list::RequestWaitingList;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handle_single_response_type() {
    let clock = Arc::new(SystemClock::new());
    let request_waiting_list = RequestWaitingList::new(
        clock.clone(),
        Duration::from_millis(3),
        Duration::from_millis(2),
    );

    let get_async_quorum_callback = AsyncQuorumCallback::<GetValueResponse>::new(2);
    let get_async_quorum_callback_clone1 = get_async_quorum_callback.clone();
    let get_async_quorum_callback_clone2 = get_async_quorum_callback.clone();

    request_waiting_list.add(10, get_async_quorum_callback_clone1);
    request_waiting_list.add(20, get_async_quorum_callback_clone2);

    let get_handle = tokio::spawn( async move {
        return get_async_quorum_callback.handle().await;
    });

    let response_from_1 = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
    let response_from_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50052);

    request_waiting_list.handle_response(10, response_from_1, Ok(Box::new(GetValueResponse { value: "one".to_string() })));
    request_waiting_list.handle_response(20, response_from_other, Ok(Box::new(GetValueResponse { value: "two".to_string() })));

    let get_response = get_handle.await.unwrap();
    let all_gets = get_response.success_responses().unwrap();

    assert_eq!(2, get_response.response_len());
    assert_eq!(&vec![GetValueResponse { value: "two".to_string() },
                     GetValueResponse { value: "one".to_string() }],
               all_gets
    );
}


#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handle_multiple_response_types() {
    let clock = Arc::new(SystemClock::new());
    let request_waiting_list = RequestWaitingList::new(
        clock.clone(),
        Duration::from_millis(3),
        Duration::from_millis(2),
    );

    let get_async_quorum_callback = AsyncQuorumCallback::<GetValueResponse>::new(2);
    let get_async_quorum_callback_clone1 = get_async_quorum_callback.clone();
    let get_async_quorum_callback_clone2 = get_async_quorum_callback.clone();

    request_waiting_list.add(10, get_async_quorum_callback_clone1);
    request_waiting_list.add(20, get_async_quorum_callback_clone2);

    let get_handle = tokio::spawn( async move {
        return get_async_quorum_callback.handle().await;
    });


    let set_async_quorum_callback = AsyncQuorumCallback::<SetValueResponse>::new(2);
    let set_async_quorum_callback_clone1 = set_async_quorum_callback.clone();
    let set_async_quorum_callback_clone2 = set_async_quorum_callback.clone();

    request_waiting_list.add(30, set_async_quorum_callback_clone1);
    request_waiting_list.add(40, set_async_quorum_callback_clone2);

    let set_handle = tokio::spawn( async move {
        return set_async_quorum_callback.handle().await;
    });

    let response_from_1 = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
    let response_from_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50052);

    request_waiting_list.handle_response(10, response_from_1.clone(), Ok(Box::new(GetValueResponse { value: "one".to_string() })));
    request_waiting_list.handle_response(20, response_from_other.clone(), Ok(Box::new(GetValueResponse { value: "two".to_string() })));

    request_waiting_list.handle_response(30, response_from_1, Ok(Box::new(SetValueResponse { key: "key1".to_string(), value: "value1".to_string() })));
    request_waiting_list.handle_response(40, response_from_other, Ok(Box::new(SetValueResponse { key: "key2".to_string(), value: "value2".to_string() })));

    let get_response = get_handle.await.unwrap();
    let all_gets = get_response.success_responses().unwrap();

    assert_eq!(2, get_response.response_len());
    assert_eq!(&vec![GetValueResponse { value: "two".to_string() },
                     GetValueResponse { value: "one".to_string() }],
               all_gets
    );

    let set_response = set_handle.await.unwrap();
    let all_sets = set_response.success_responses().unwrap();

    assert_eq!(2, set_response.response_len());
    assert_eq!(&vec![SetValueResponse { key: "key2".to_string(), value: "value2".to_string() },
                     SetValueResponse { key: "key1".to_string(), value: "value1".to_string() }],
               all_sets
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handle_multiple_response_types_with_error() {
    let clock = Arc::new(SystemClock::new());
    let request_waiting_list = RequestWaitingList::new(
        clock.clone(),
        Duration::from_millis(3),
        Duration::from_millis(2),
    );

    let get_async_quorum_callback = AsyncQuorumCallback::<GetValueResponse>::new(2);
    let get_async_quorum_callback_clone1 = get_async_quorum_callback.clone();
    let get_async_quorum_callback_clone2 = get_async_quorum_callback.clone();

    request_waiting_list.add(10, get_async_quorum_callback_clone1);
    request_waiting_list.add(20, get_async_quorum_callback_clone2);

    let get_handle = tokio::spawn( async move {
        return get_async_quorum_callback.handle().await;
    });


    let set_async_quorum_callback = AsyncQuorumCallback::<SetValueResponse>::new(2);
    let set_async_quorum_callback_clone1 = set_async_quorum_callback.clone();
    let set_async_quorum_callback_clone2 = set_async_quorum_callback.clone();

    request_waiting_list.add(30, set_async_quorum_callback_clone1);
    request_waiting_list.add(40, set_async_quorum_callback_clone2);

    let set_handle = tokio::spawn( async move {
        return set_async_quorum_callback.handle().await;
    });

    let response_from_1 = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
    let response_from_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50052);

    request_waiting_list.handle_response(10, response_from_1.clone(), Ok(Box::new(GetValueResponse { value: "one".to_string() })));
    request_waiting_list.handle_response(20, response_from_other.clone(), Ok(Box::new(GetValueResponse { value: "two".to_string() })));

    request_waiting_list.handle_response(30, response_from_1, Ok(Box::new(SetValueResponse { key: "key1".to_string(), value: "value1".to_string() })));
    request_waiting_list.handle_response(40, response_from_other, Err(Box::new(TestError{message: "Test error".to_string()})));

    let get_response = get_handle.await.unwrap();
    let all_gets = get_response.success_responses().unwrap();

    assert_eq!(2, get_response.response_len());
    assert_eq!(&vec![GetValueResponse { value: "two".to_string() },
                     GetValueResponse { value: "one".to_string() }],
               all_gets
    );

    let set_response = set_handle.await.unwrap();
    assert_eq!(1, set_response.response_len());

    let error_responses = set_response.error_responses().unwrap();
    let test_error = error_responses.as_slice().get(0).unwrap().downcast_ref::<TestError>().unwrap();
    assert_eq!("Test error", test_error.message);
}

#[derive(Eq, PartialEq, Debug)]
struct GetValueResponse {
    pub value: String,
}


#[derive(Eq, PartialEq, Debug)]
struct SetValueResponse {
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