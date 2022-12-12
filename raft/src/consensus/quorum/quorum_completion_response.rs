use std::any::Any;
use crate::net::request_waiting_list::response_callback::ResponseErrorType;

pub enum QuorumCompletionResponse<Response: Any> {
    Success(Vec<Response>),
    Error(Vec<ResponseErrorType>),
}

impl<Response: Any> QuorumCompletionResponse<Response> {
    pub fn response_len(&self) -> usize {
        return match self {
            QuorumCompletionResponse::Success(r) => {
                r.len()
            }
            QuorumCompletionResponse::Error(e) => {
                e.len()
            }
        };
    }

    pub fn success_responses(&self) -> Option<&Vec<Response>> {
        return match self {
            QuorumCompletionResponse::Success(r) => {
                Some(r)
            }
            QuorumCompletionResponse::Error(_) => {
                None
            }
        };
    }

    pub fn error_responses(&self) -> Option<&Vec<ResponseErrorType>> {
        return match self {
            QuorumCompletionResponse::Success(_) => {
                None
            }
            QuorumCompletionResponse::Error(e) => {
                Some(e)
            }
        };
    }
}