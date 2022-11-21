use std::error::Error;
use std::rc::Rc;

pub(crate) type ResponseCallbackType<Response> = Rc<dyn ResponseCallback<Response> + 'static>;

pub(crate) type ResponseErrorType = Box<dyn Error>;

pub trait ResponseCallback<Response> {
    fn on_response(&self, response: Result<Response, ResponseErrorType>);
}
