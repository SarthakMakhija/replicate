use std::any::Any;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};
use std::task::{Context, Poll, Waker};
use crate::consensus::quorum::async_quorum_callback::SuccessCondition;
use crate::consensus::quorum::quorum_completion_response::QuorumCompletionResponse;
use crate::net::connect::host_and_port::HostAndPort;
use crate::net::request_waiting_list::response_callback::{AnyResponse, ResponseErrorType};

pub struct  QuorumCompletionHandle<Response: Any + Send + Sync + Debug> {
    pub(crate) responses: RwLock<Vec<Result<Response, ResponseErrorType>>>,
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
                self.responses.write().unwrap().push(Ok(typed_response));
            }
            Err(e) => {
                self.responses.write().unwrap().push(Err(e));
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

        let mut responses_guard = self.responses.write().unwrap();

        let success_response_count = &self.success_response_count(&mut responses_guard);
        if *success_response_count >= self.majority_quorum {
            let all_responses = self.all_success_responses(&mut responses_guard);
            return Poll::Ready(QuorumCompletionResponse::Success(all_responses));
        }

        let error_response_count = self.error_response_count(&mut responses_guard);
        if success_response_count + error_response_count == self.expected_total_responses {
            let all_responses = self.all_error_responses(responses_guard);
            return Poll::Ready(QuorumCompletionResponse::Error(all_responses));
        }
        return Poll::Pending;
    }
}

impl<Response: Any + Send + Sync + Debug> QuorumCompletionHandle<Response> {
    fn all_success_responses(&self, responses_guard: &mut RwLockWriteGuard<Vec<Result<Response, ResponseErrorType>>>) -> Vec<Response> {
        let mut all_responses = Vec::with_capacity(responses_guard.len());
        while let Some(response) = responses_guard.pop() {
            if response.is_ok() {
                all_responses.push(response.unwrap());
            }
        }
        all_responses
    }

    fn all_error_responses(&self, mut responses_guard: RwLockWriteGuard<Vec<Result<Response, ResponseErrorType>>>) -> Vec<ResponseErrorType> {
        let mut all_responses = Vec::with_capacity(responses_guard.len());
        while let Some(response) = responses_guard.pop() {
            if response.is_err() {
                all_responses.push(response.unwrap_err());
            }
        }
        all_responses
    }

    fn success_response_count(&self, responses_guard: &mut RwLockWriteGuard<Vec<Result<Response, ResponseErrorType>>>) -> usize {
        responses_guard
            .iter()
            .filter(|response| response.is_ok())
            .filter(|response| (self.success_condition)(response.as_ref().unwrap()))
            .count()
    }

    fn error_response_count(&self, responses_guard: &mut RwLockWriteGuard<Vec<Result<Response, ResponseErrorType>>>) -> usize {
        responses_guard
            .iter()
            .filter(|response| response.is_err())
            .count()
    }
}
