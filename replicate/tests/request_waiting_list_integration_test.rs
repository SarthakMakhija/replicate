use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use replicate::callback::async_quorum_callback::AsyncQuorumCallback;
use replicate::clock::clock::SystemClock;
use replicate::net::connect::host_and_port::HostAndPort;
use replicate::net::request_waiting_list::request_waiting_list::RequestWaitingList;
use replicate::net::request_waiting_list::request_waiting_list_config::RequestWaitingListConfig;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handle_single_response_type() {
    let clock = Arc::new(SystemClock::new());
    let request_waiting_list = RequestWaitingList::new(
        clock.clone(),
        RequestWaitingListConfig::new(
            Duration::from_millis(3),
            Duration::from_millis(2),
        ),
    );

    let get_async_quorum_callback = AsyncQuorumCallback::<GetValueResponse>::new(3, 2);
    let get_async_quorum_callback_clone1 = get_async_quorum_callback.clone();
    let get_async_quorum_callback_clone2 = get_async_quorum_callback.clone();

    let response_from_1 = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
    let response_from_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50052);

    request_waiting_list.add(10, response_from_1.clone(), get_async_quorum_callback_clone1);
    request_waiting_list.add(20, response_from_other.clone(), get_async_quorum_callback_clone2);

    let get_handle = tokio::spawn(async move {
        return get_async_quorum_callback.handle().await;
    });

    request_waiting_list.handle_response(10, response_from_1.clone(), Ok(Box::new(GetValueResponse { value: "one".to_string() })));
    request_waiting_list.handle_response(20, response_from_other.clone(), Ok(Box::new(GetValueResponse { value: "two".to_string() })));

    let get_response = get_handle.await.unwrap();
    let all_gets = get_response.success_response().unwrap();

    let mut expected = HashMap::new();
    expected.insert(response_from_1, GetValueResponse { value: "one".to_string() });
    expected.insert(response_from_other, GetValueResponse { value: "two".to_string() });

    assert_eq!(2, get_response.response_len());
    assert_eq!(&expected, all_gets);
}


#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handle_multiple_response_types() {
    let clock = Arc::new(SystemClock::new());
    let request_waiting_list = RequestWaitingList::new(
        clock.clone(),
        RequestWaitingListConfig::new(
            Duration::from_millis(3),
            Duration::from_millis(2),
        ),
    );

    let get_async_quorum_callback = AsyncQuorumCallback::<GetValueResponse>::new(3, 2);
    let get_async_quorum_callback_clone1 = get_async_quorum_callback.clone();
    let get_async_quorum_callback_clone2 = get_async_quorum_callback.clone();

    let response_from_1 = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
    let response_from_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50052);

    request_waiting_list.add(10, response_from_1.clone(), get_async_quorum_callback_clone1);
    request_waiting_list.add(20, response_from_other.clone(), get_async_quorum_callback_clone2);

    let get_handle = tokio::spawn(async move {
        return get_async_quorum_callback.handle().await;
    });

    let set_async_quorum_callback = AsyncQuorumCallback::<SetValueResponse>::new(3, 2);
    let set_async_quorum_callback_clone1 = set_async_quorum_callback.clone();
    let set_async_quorum_callback_clone2 = set_async_quorum_callback.clone();

    request_waiting_list.add(30, response_from_1.clone(), set_async_quorum_callback_clone1);
    request_waiting_list.add(40, response_from_other.clone(), set_async_quorum_callback_clone2);

    let set_handle = tokio::spawn(async move {
        return set_async_quorum_callback.handle().await;
    });

    request_waiting_list.handle_response(10, response_from_1.clone(), Ok(Box::new(GetValueResponse { value: "one".to_string() })));
    request_waiting_list.handle_response(20, response_from_other.clone(), Ok(Box::new(GetValueResponse { value: "two".to_string() })));

    request_waiting_list.handle_response(30, response_from_1.clone(), Ok(Box::new(SetValueResponse { key: "key1".to_string(), value: "value1".to_string() })));
    request_waiting_list.handle_response(40, response_from_other.clone(), Ok(Box::new(SetValueResponse { key: "key2".to_string(), value: "value2".to_string() })));

    let get_response = get_handle.await.unwrap();
    let all_gets = get_response.success_response().unwrap();

    let mut expected = HashMap::new();
    expected.insert(response_from_1.clone(), GetValueResponse { value: "one".to_string() });
    expected.insert(response_from_other.clone(), GetValueResponse { value: "two".to_string() });

    assert_eq!(2, get_response.response_len());
    assert_eq!(&expected, all_gets);

    let set_response = set_handle.await.unwrap();
    let all_sets = set_response.success_response().unwrap();

    let mut expected = HashMap::new();
    expected.insert(response_from_1, SetValueResponse { key: "key1".to_string(), value: "value1".to_string() });
    expected.insert(response_from_other, SetValueResponse { key: "key2".to_string(), value: "value2".to_string() });

    assert_eq!(2, set_response.response_len());
    assert_eq!(&expected, all_sets);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handle_multiple_response_types_with_error() {
    let clock = Arc::new(SystemClock::new());
    let request_waiting_list = RequestWaitingList::new(
        clock.clone(),
        RequestWaitingListConfig::new(
            Duration::from_millis(3),
            Duration::from_millis(2),
        ),
    );

    let get_async_quorum_callback = AsyncQuorumCallback::<GetValueResponse>::new(3, 2);
    let get_async_quorum_callback_clone1 = get_async_quorum_callback.clone();
    let get_async_quorum_callback_clone2 = get_async_quorum_callback.clone();

    let response_from_1 = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
    let response_from_other = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50052);

    request_waiting_list.add(10, response_from_1.clone(), get_async_quorum_callback_clone1);
    request_waiting_list.add(20, response_from_other.clone(), get_async_quorum_callback_clone2);

    let get_handle = tokio::spawn(async move {
        return get_async_quorum_callback.handle().await;
    });

    let set_async_quorum_callback = AsyncQuorumCallback::<SetValueResponse>::new(3, 2);
    let set_async_quorum_callback_clone1 = set_async_quorum_callback.clone();
    let set_async_quorum_callback_clone2 = set_async_quorum_callback.clone();

    request_waiting_list.add(30, response_from_1.clone(), set_async_quorum_callback_clone1);
    request_waiting_list.add(40, response_from_other.clone(), set_async_quorum_callback_clone2);

    let set_handle = tokio::spawn(async move {
        return set_async_quorum_callback.handle().await;
    });

    request_waiting_list.handle_response(10, response_from_1.clone(), Ok(Box::new(GetValueResponse { value: "one".to_string() })));
    request_waiting_list.handle_response(20, response_from_other.clone(), Ok(Box::new(GetValueResponse { value: "two".to_string() })));

    request_waiting_list.handle_response(30, response_from_1.clone(), Ok(Box::new(SetValueResponse { key: "key1".to_string(), value: "value1".to_string() })));
    request_waiting_list.handle_response(40, response_from_other.clone(), Err(Box::new(TestError { message: "Test error".to_string() })));

    let get_response = get_handle.await.unwrap();
    let all_gets = get_response.success_response().unwrap();

    let mut expected = HashMap::new();
    expected.insert(response_from_1, GetValueResponse { value: "one".to_string() });
    expected.insert(response_from_other, GetValueResponse { value: "two".to_string() });

    assert_eq!(2, get_response.response_len());
    assert_eq!(&expected, all_gets);

    let set_response = set_handle.await.unwrap();
    assert_eq!(1, set_response.response_len());

    let responses = set_response.error_response().unwrap();
    let test_error = responses.get(&response_from_other).unwrap().downcast_ref::<TestError>().unwrap();
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