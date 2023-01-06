use std::any::Any;
use std::collections::HashMap;

use crate::net::connect::host_and_port::HostAndPort;
use crate::net::request_waiting_list::response_callback::ResponseErrorType;

pub enum QuorumCompletionResponse<Response: Any> {
    Success(HashMap<HostAndPort, Response>),
    Error(HashMap<HostAndPort, ResponseErrorType>),
    SuccessConditionNotMet(HashMap<HostAndPort, Response>),
    Split(HashMap<HostAndPort, Result<Response, ResponseErrorType>>)
}

impl<Response: Any> QuorumCompletionResponse<Response> {
    pub fn response_len(&self) -> usize {
        return match self {
            QuorumCompletionResponse::Success(r) => r.len(),
            QuorumCompletionResponse::Error(e) => e.len(),
            QuorumCompletionResponse::SuccessConditionNotMet(r) => r.len(),
            QuorumCompletionResponse::Split(r) => r.len(),
        };
    }

    pub fn success_response(&self) -> Option<&HashMap<HostAndPort, Response>> {
        return match self {
            QuorumCompletionResponse::Success(r) => Some(r),
            _ => None
        };
    }

    pub fn error_response(&self) -> Option<&HashMap<HostAndPort, ResponseErrorType>> {
        return match self {
            QuorumCompletionResponse::Error(e) => Some(e),
            _ => None
        };
    }

    pub fn success_condition_not_met_response(&self) -> Option<&HashMap<HostAndPort, Response>> {
        return match self {
            QuorumCompletionResponse::SuccessConditionNotMet(r) => Some(r),
            _ => None
        };
    }

    pub fn split_response(&self) -> Option<&HashMap<HostAndPort, Result<Response, ResponseErrorType>>> {
        return match self {
            QuorumCompletionResponse::Split(r) => Some(r),
            _ => None
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
        if let QuorumCompletionResponse::SuccessConditionNotMet(_) = &self {
            return true;
        }
        return false;
    }

    pub fn is_split(&self) -> bool {
        if let QuorumCompletionResponse::Split(_) = &self {
            return true;
        }
        return false;
    }
}