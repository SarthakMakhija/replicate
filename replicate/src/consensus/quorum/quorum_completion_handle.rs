use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};
use std::task::{Context, Poll, Waker};

use crate::consensus::quorum::async_quorum_callback::SuccessCondition;
use crate::consensus::quorum::quorum_completion_response::QuorumCompletionResponse;
use crate::net::connect::host_and_port::HostAndPort;
use crate::net::request_waiting_list::response_callback::{AnyResponse, ResponseErrorType};

pub struct QuorumCompletionHandle<Response: Any + Send + Sync + Debug> {
    pub(crate) success_responses: RwLock<HashMap<HostAndPort, Response>>,
    pub(crate) error_responses: RwLock<HashMap<HostAndPort, ResponseErrorType>>,
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
                let optional_response: Option<Box<Response>> = any_response.downcast::<Response>().ok();
                let unwrapped = optional_response.unwrap();
                let typed_response = *unwrapped;
                self.success_responses.write().unwrap().insert(from, typed_response);
            }
            Err(e) => {
                self.error_responses.write().unwrap().insert(from, e);
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

        let mut write_guard = self.success_responses.write().unwrap();

        let total_non_error_responses = write_guard.len();
        let success_response_count = self.success_response_count(&mut write_guard);
        if success_response_count >= self.majority_quorum {
            let all_responses = self.all_success_responses(&mut write_guard);
            return Poll::Ready(QuorumCompletionResponse::Success(all_responses));
        }

        let mut write_guard = self.error_responses.write().unwrap();

        let error_response_count = write_guard.len();
        if total_non_error_responses + error_response_count == self.expected_total_responses {
            let all_responses = self.all_error_responses(&mut write_guard);
            if !all_responses.is_empty() {
                return Poll::Ready(QuorumCompletionResponse::Error(all_responses));
            }
            return Poll::Ready(QuorumCompletionResponse::SuccessConditionNotMet);
        }
        return Poll::Pending;
    }
}

impl<Response: Any + Send + Sync + Debug> QuorumCompletionHandle<Response> {
    fn success_response_count(&self, responses_guard: &mut RwLockWriteGuard<HashMap<HostAndPort, Response>>) -> usize {
        return responses_guard.iter().filter(|response|(self.success_condition)(response.1)).count();
    }

    fn all_success_responses(&self, responses_guard: &mut RwLockWriteGuard<HashMap<HostAndPort, Response>>) -> HashMap<HostAndPort, Response> {
        return responses_guard.drain().collect();
    }

    fn all_error_responses(&self, responses_guard: &mut RwLockWriteGuard<HashMap<HostAndPort, ResponseErrorType>>) -> HashMap<HostAndPort, ResponseErrorType> {
        return responses_guard.drain().collect();
    }
}
