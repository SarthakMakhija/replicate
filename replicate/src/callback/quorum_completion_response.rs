use std::any::Any;
use std::collections::HashMap;

use crate::net::connect::host_and_port::HostAndPort;
use crate::net::request_waiting_list::response_callback::ResponseErrorType;

pub enum QuorumCompletionResponse<Response: Any> {
    Success(HashMap<HostAndPort, Response>),
    Error(HashMap<HostAndPort, ResponseErrorType>),
    SuccessConditionNotMet(HashMap<HostAndPort, Response>),
}

impl<Response: Any> QuorumCompletionResponse<Response> {
    pub fn response_len(&self) -> usize {
        return match self {
            QuorumCompletionResponse::Success(r) => r.len(),
            QuorumCompletionResponse::Error(e) => e.len(),
            QuorumCompletionResponse::SuccessConditionNotMet(r) => r.len(),
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

    pub fn all_success_condition_not_met_hosts(&self, matching_condition: Box<dyn Fn(&Response) -> bool>) -> Vec<HostAndPort> {
        return match self {
            QuorumCompletionResponse::SuccessConditionNotMet(responses) =>
                responses
                    .iter()
                    .filter(|entry| (matching_condition)(entry.1))
                    .map(|host_and_port| host_and_port.0.clone())
                    .collect(),
            _ =>
                Vec::new()
        };
    }

    fn all_hosts_from(&self,
                      response_by_host: &HashMap<HostAndPort, Response>,
                      matching_condition: Box<dyn Fn(&Response) -> bool>,
    ) -> Vec<HostAndPort> {
        return response_by_host
            .iter()
            .filter(|entry| (matching_condition)(entry.1))
            .map(|host_and_port| host_and_port.0.clone())
            .collect();
    }
}