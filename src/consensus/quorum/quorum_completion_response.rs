use crate::net::request_waiting_list::response_callback::ResponseErrorType;


pub(crate) enum QuorumCompletionResponse<Response: Send + Sync + Unpin> {
    Success(Vec<Response>),
    Error(Vec<ResponseErrorType>),
}

impl<Response: Send + Sync + Unpin> QuorumCompletionResponse<Response> {
    pub(crate) fn response_len(&self) -> usize {
        return match self {
            QuorumCompletionResponse::Success(r) => {
                r.len()
            }
            QuorumCompletionResponse::Error(e) => {
                e.len()
            }
        };
    }

    pub(crate) fn success_responses(&self) -> Option<&Vec<Response>> {
        return match self {
            QuorumCompletionResponse::Success(r) => {
                Some(r)
            }
            QuorumCompletionResponse::Error(_) => {
                None
            }
        };
    }

    pub(crate) fn error_responses(&self) -> Option<&Vec<ResponseErrorType>> {
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