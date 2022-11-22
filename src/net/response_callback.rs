use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

pub(crate) type ResponseErrorType = Box<dyn Error + 'static>;

pub(crate) type ResponseCallbackType<Response> = Arc<dyn ResponseCallback<Response> + 'static>;

pub trait ResponseCallback<Response> {
    fn on_response(&self, response: Result<Response, ResponseErrorType>);
}

pub struct TimestampedCallback<Response> {
    callback: ResponseCallbackType<Response>,
    creation_time: Instant,
}

impl<Response> TimestampedCallback<Response> {
    pub(crate) fn new(callback: ResponseCallbackType<Response>, creation_time: Instant) -> TimestampedCallback<Response> {
        return TimestampedCallback {
            callback,
            creation_time,
        };
    }

    pub(crate) fn on_response(&self, response: Result<Response, ResponseErrorType>) {
        self.callback.on_response(response);
    }
}

