use std::any::Any;
use std::collections::HashMap;

use crate::net::connect::host_and_port::HostAndPort;
use crate::net::request_waiting_list::response_callback::ResponseErrorType;

pub enum QuorumCompletionResponse<Response: Any> {
    Success(HashMap<HostAndPort, Response>),
    Error(HashMap<HostAndPort, ResponseErrorType>),
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

    pub fn success_responses(&self) -> Option<&HashMap<HostAndPort, Response>> {
        return match self {
            QuorumCompletionResponse::Success(r) => {
                Some(r)
            }
            QuorumCompletionResponse::Error(_) => {
                None
            }
        };
    }

    pub fn error_responses(&self) -> Option<&HashMap<HostAndPort, ResponseErrorType>> {
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