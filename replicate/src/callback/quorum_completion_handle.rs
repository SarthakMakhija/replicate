use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};
use std::task::{Context, Poll, Waker};

use QuorumCompletionResponse::{Error, Success, SuccessConditionNotMet};

use crate::callback::async_quorum_callback::{SuccessCondition, UnexpectedQuorumCallbackResponseError};
use crate::callback::quorum_completion_response::QuorumCompletionResponse;
use crate::net::connect::host_and_port::HostAndPort;
use crate::net::request_waiting_list::response_callback::{AnyResponse, ResponseErrorType};

pub struct QuorumCompletionHandle<Response: Any + Send + Sync + Debug> {
    pub(crate) responses: RwLock<HashMap<HostAndPort, Result<Response, ResponseErrorType>>>,
    pub(crate) expected_total_responses: usize,
    pub(crate) majority_quorum: usize,
    pub(crate) success_condition: SuccessCondition<Response>,
    pub(crate) waker_state: Arc<Mutex<WakerState>>,
}

pub(crate) struct WakerState {
    pub(crate) waker: Option<Waker>,
}

impl<Response: Any + Send + Sync + Debug> QuorumCompletionHandle<Response> {
    pub(crate) fn on_response(&self, from: HostAndPort, response: Result<AnyResponse, ResponseErrorType>) {
        match response {
            Ok(any_response) => {
                let response_type_id = any_response.type_id();
                let optional_response = any_response.downcast::<Response>().ok();
                match optional_response {
                    None => {
                        let err = UnexpectedQuorumCallbackResponseError { response_type_id };
                        self.responses.write().unwrap().insert(from, Err(Box::new(err)));
                    }
                    Some(response) => {
                        let typed_response = *response;
                        self.responses.write().unwrap().insert(from, Ok(typed_response));
                    }
                }
            }
            Err(e) => {
                self.responses.write().unwrap().insert(from, Err(e));
            }
        }

        if let Some(waker) = &self.waker_state.lock().unwrap().waker {
            waker.wake_by_ref();
        }
    }
}

impl<Response: Any + Send + Sync + Debug> Future for &QuorumCompletionHandle<Response> {
    type Output = QuorumCompletionResponse<Response>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut guard = self.waker_state.lock().unwrap();
        if let Some(waker) = guard.waker.as_ref() {
            if !waker.will_wake(ctx.waker()) {
                (*guard).waker = Some(ctx.waker().clone());
            }
        } else {
            guard.waker = Some(ctx.waker().clone());
        }

        let mut write_guard = self.responses.write().unwrap();

        let total_non_error_responses = self.non_error_response_count(&write_guard);
        let success_response_count = self.success_response_count(&mut write_guard);
        if success_response_count >= self.majority_quorum {
            return Poll::Ready(Success(self.all_success_responses(&mut write_guard)));
        }
        if self.missing_success_condition_response_count(&write_guard) >= self.majority_quorum {
            return Poll::Ready(SuccessConditionNotMet(self.all_missing_success_condition_responses(&mut write_guard)));
        }

        let error_response_count = self.error_response_count(&write_guard);
        if error_response_count >= self.majority_quorum {
            return Poll::Ready(Error(self.all_error_responses(&mut write_guard)));
        }
        if total_non_error_responses + error_response_count >= self.expected_total_responses {
            if error_response_count >= 1 {
                return Poll::Ready(Error(self.all_error_responses(&mut write_guard)));
            }
            return Poll::Ready(SuccessConditionNotMet(self.all_missing_success_condition_responses(&mut write_guard)));
        }
        return Poll::Pending;
    }
}

impl<Response: Any + Send + Sync + Debug> QuorumCompletionHandle<Response> {
    fn non_error_response_count(&self, responses_guard: &RwLockWriteGuard<HashMap<HostAndPort, Result<Response, ResponseErrorType>>>) -> usize {
        return responses_guard.iter().filter(|response| response.1.is_ok()).count();
    }

    fn error_response_count(&self, responses_guard: &RwLockWriteGuard<HashMap<HostAndPort, Result<Response, ResponseErrorType>>>) -> usize {
        return responses_guard.iter().filter(|response| response.1.is_err()).count();
    }

    fn missing_success_condition_response_count(&self, responses_guard: &RwLockWriteGuard<HashMap<HostAndPort, Result<Response, ResponseErrorType>>>) -> usize {
        return responses_guard
            .iter()
            .filter(|response| response.1.is_ok())
            .map(|response| response.1)
            .map(|response| response.as_ref().unwrap())
            .filter(|response| !(self.success_condition)(response))
            .count();
    }

    fn success_response_count(&self, responses_guard: &mut RwLockWriteGuard<HashMap<HostAndPort, Result<Response, ResponseErrorType>>>) -> usize {
        return responses_guard
            .iter()
            .filter(|response| response.1.is_ok())
            .map(|response| response.1)
            .map(|response| response.as_ref().unwrap())
            .filter(|response| (self.success_condition)(response))
            .count();
    }

    fn all_success_responses(&self, responses_guard: &mut RwLockWriteGuard<HashMap<HostAndPort, Result<Response, ResponseErrorType>>>) -> HashMap<HostAndPort, Response> {
        return responses_guard
            .drain()
            .filter(|response| response.1.is_ok())
            .map(|response| (response.0, response.1.unwrap()))
            .collect();
    }

    fn all_missing_success_condition_responses(&self, responses_guard: &mut RwLockWriteGuard<HashMap<HostAndPort, Result<Response, ResponseErrorType>>>) -> HashMap<HostAndPort, Response> {
        return responses_guard
            .drain()
            .filter(|response| response.1.is_ok())
            .filter(|response| {
                let response = response.1.as_ref().unwrap();
                !(self.success_condition)(response)
            })
            .map(|response| (response.0, response.1.unwrap()))
            .collect();
    }

    fn all_error_responses(&self, responses_guard: &mut RwLockWriteGuard<HashMap<HostAndPort, Result<Response, ResponseErrorType>>>) -> HashMap<HostAndPort, ResponseErrorType> {
        return responses_guard
            .drain()
            .filter(|response| response.1.is_err())
            .map(|response| (response.0, response.1.unwrap_err()))
            .collect();
    }
}
