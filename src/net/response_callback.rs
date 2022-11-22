use std::error::Error;
use std::sync::Arc;

pub(crate) type ResponseErrorType = Box<dyn Error + 'static>;

pub trait ResponseCallback<Response> {
    fn on_response(&self, response: Result<Response, ResponseErrorType>);
}

pub(crate) type ResponseCallbackType<Response> = Arc<dyn ResponseCallback<Response> + 'static>;
