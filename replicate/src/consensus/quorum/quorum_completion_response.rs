use std::any::Any;
use std::collections::HashMap;

use crate::net::connect::host_and_port::HostAndPort;
use crate::net::request_waiting_list::response_callback::ResponseErrorType;

pub enum QuorumCompletionResponse<Response: Any> {
    Success(HashMap<HostAndPort, Response>),
    Error(HashMap<HostAndPort, ResponseErrorType>),
    SuccessConditionNotMet,
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
            QuorumCompletionResponse::SuccessConditionNotMet => {
                0
            }
        };
    }

    pub fn success_response(&self) -> Option<&HashMap<HostAndPort, Response>> {
        return match self {
            QuorumCompletionResponse::Success(r) => {
                Some(r)
            }
            QuorumCompletionResponse::Error(_) => {
                None
            }
            QuorumCompletionResponse::SuccessConditionNotMet => {
                None
            }
        };
    }

    pub fn error_response(&self) -> Option<&HashMap<HostAndPort, ResponseErrorType>> {
        return match self {
            QuorumCompletionResponse::Success(_) => {
                None
            }
            QuorumCompletionResponse::Error(e) => {
                Some(e)
            }
            QuorumCompletionResponse::SuccessConditionNotMet => {
                None
            }
        };
    }

    pub fn is_success(&self) -> bool {
        if let QuorumCompletionResponse::Success(_) = &self {
            return true;
        }
        return false;
    }

    pub fn is_error(&self) -> bool {
        if let QuorumCompletionResponse::Error(_) = &self {
            return true;
        }
        return false;
    }

    pub fn is_success_condition_not_met(&self) -> bool {
        if let QuorumCompletionResponse::SuccessConditionNotMet = &self {
            return true;
        }
        return false;
    }
}