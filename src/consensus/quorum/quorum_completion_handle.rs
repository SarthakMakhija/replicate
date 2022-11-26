use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};
use std::task::{Context, Poll, Waker};
use crate::consensus::quorum::async_quorum_callback::SuccessCondition;
use crate::consensus::quorum::quorum_completion_response::QuorumCompletionResponse;
use crate::net::request_waiting_list::response_callback::ResponseErrorType;

pub(crate) struct  QuorumCompletionHandle<Response: Send + Sync + Unpin + Debug> {
    pub(crate) responses: RwLock<Vec<Result<Response, ResponseErrorType>>>,
    pub(crate) expected_total_responses: usize,
    pub(crate) majority_quorum: usize,
    pub(crate) success_condition: SuccessCondition<Response>,
    pub(crate) waker: Option<Arc<Mutex<Waker>>>,
}

impl<Response: Send + Sync + Unpin + Debug> QuorumCompletionHandle<Response> {
    pub(crate) fn on_response(&self, response: Result<Response, ResponseErrorType>) {
        self.responses.write().unwrap().push(response);

        if let Some(waker) = &self.waker {
            let waker = waker.lock().unwrap();
            waker.wake_by_ref();
        }
    }
}

impl<Response: Send + Sync + Unpin + Debug> Future for QuorumCompletionHandle<Response> {
    type Output = QuorumCompletionResponse<Response>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(waker) = &self.waker {
            let mut waker = waker.lock().unwrap();
            if !waker.will_wake(ctx.waker()) {
                *waker = ctx.waker().clone();
            }
        } else {
            let waker = Arc::new(Mutex::new(ctx.waker().clone()));
            self.waker = Some(waker.clone());
        }

        let mut responses_guard = self.responses.write().unwrap();

        let success_response_count = &self.success_response_count(&mut responses_guard);
        if *success_response_count >= self.majority_quorum {
            let all_responses = Self::all_success_responses(&mut responses_guard);
            return Poll::Ready(QuorumCompletionResponse::Success(all_responses));
        }

        let error_response_count = Self::error_response_count(&mut responses_guard);
        if success_response_count + error_response_count == self.expected_total_responses {
            let all_responses = Self::all_error_responses(responses_guard);
            return Poll::Ready(QuorumCompletionResponse::Error(all_responses));
        }
        return Poll::Pending;
    }
}

impl<Response: Send + Sync + Unpin + Debug> QuorumCompletionHandle<Response> {
    fn all_success_responses(responses_guard: &mut RwLockWriteGuard<Vec<Result<Response, ResponseErrorType>>>) -> Vec<Response> {
        let mut all_responses = Vec::with_capacity(responses_guard.len());
        while let Some(response) = responses_guard.pop() {
            if response.is_ok() {
                all_responses.push(response.unwrap());
            }
        }
        all_responses
    }

    fn all_error_responses(mut responses_guard: RwLockWriteGuard<Vec<Result<Response, ResponseErrorType>>>) -> Vec<ResponseErrorType> {
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

    fn error_response_count(responses_guard: &mut RwLockWriteGuard<Vec<Result<Response, ResponseErrorType>>>) -> usize {
        responses_guard
            .iter()
            .filter(|response| response.is_err())
            .count()
    }
}
