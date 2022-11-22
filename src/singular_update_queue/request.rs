use std::sync::mpsc::Sender;

pub(crate) struct ChanneledRequest<Request, Response>
    where Request: Send, Response: Send {
    pub(crate) request: Request,
    pub(crate) respond_back: Sender<Response>,
}

pub trait RequestHandler<Request, Response>: Send + Sync {
    fn handle(&self, request: Request) -> Response;
}

impl<Request, Response> ChanneledRequest<Request, Response>
    where Request: Send, Response: Send {
    pub(crate) fn new(request: Request, sender: Sender<Response>) -> ChanneledRequest<Request, Response> {
        return ChanneledRequest { request, respond_back: sender.clone() };
    }
}

