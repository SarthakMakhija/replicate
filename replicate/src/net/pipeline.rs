use std::any::Any;
use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::transport::Channel;

use crate::net::connect::async_network::AsyncNetwork;
use crate::net::connect::error::ServiceResponseError;
use crate::net::connect::host_and_port::HostAndPort;
use crate::net::connect::service_client::ServiceRequest;
use crate::net::peers::Peer;
use crate::singular_update_queue::singular_update_queue::{AsyncBlock, SingularUpdateQueue};

pub type PipelinedRequest = Box<dyn Any + Send>;
pub type PipelinedResponse = Box<dyn Any + Send>;

pub type ResponseHandlerGenerator = Box<dyn Fn(HostAndPort, Result<PipelinedResponse, ServiceResponseError>) -> Option<AsyncBlock> + Send + Sync + 'static>;

pub trait ToPipelinedRequest {
    fn pipeline_request(self) -> PipelinedRequest;
}

pub trait ToPipelinedResponse {
    fn pipeline_response(self) -> PipelinedResponse;
}

impl<T: Any + Send> ToPipelinedRequest for T {
    fn pipeline_request(self) -> PipelinedRequest {
        return Box::new(self);
    }
}

impl<T: Any + Send> ToPipelinedResponse for T {
    fn pipeline_response(self) -> PipelinedResponse {
        return Box::new(self);
    }
}

struct ResponseHandlerByRequest {
    service_request: ServiceRequest<PipelinedRequest, PipelinedResponse>,
    response_handler_generator: Arc<ResponseHandlerGenerator>,
}

pub(crate) struct Pipeline {
    peer: Peer,
    self_address: HostAndPort,
    sender: Sender<ResponseHandlerByRequest>,
    runtime: Runtime,
}

impl Pipeline {
    pub(crate) fn new(peer: Peer, self_address: HostAndPort, singular_update_queue: Arc<SingularUpdateQueue>) -> Self {
        let (sender, receiver) = mpsc::channel(100);
        let pipeline = Pipeline {
            peer,
            self_address,
            sender,
            runtime: Self::single_threaded_runtime(),
        };

        pipeline.start(receiver, pipeline.peer, pipeline.self_address, singular_update_queue, BuiltInChannelBuilder::new());
        return pipeline;
    }

    pub(crate) async fn submit(&self,
                               service_request: ServiceRequest<PipelinedRequest, PipelinedResponse>,
                               response_handler_generator: Arc<ResponseHandlerGenerator>) -> Result<(), Box<dyn Error>> {
        match self.sender.clone().send(ResponseHandlerByRequest { service_request, response_handler_generator }).await {
            Ok(_) =>
                Ok(()),
            Err(send_error) =>
                Err(Box::new(PipelineSubmissionError(send_error.0.service_request.payload))),
        }
    }

    fn start(&self,
             mut receiver: Receiver<ResponseHandlerByRequest>,
             peer: Peer,
             source_address: HostAndPort,
             singular_update_queue: Arc<SingularUpdateQueue>,
             channel_builder: impl ChannelBuilder) {
        let peer_address = peer.get_address().clone();
        self.runtime.spawn(async move {
            let mut channel: Option<Channel> = None;
            while let Some(response_handler_by_request) = receiver.recv().await {
                channel = channel_builder.build(&peer).await;
                let response = AsyncNetwork::send_with_source_footprint_on(
                    response_handler_by_request.service_request,
                    source_address,
                    peer_address,
                    channel,
                ).await;

                if let Some(handler) = (response_handler_by_request.response_handler_generator)(peer_address, response) {
                    let _ = singular_update_queue.submit(handler).await;
                }
            }
        });
    }

    fn single_threaded_runtime() -> Runtime {
        return Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();
    }
}

#[async_trait]
pub(crate) trait ChannelBuilder: Send + Sync + 'static {
    async fn build(&self, peer: &Peer) -> Option<Channel>;
}

struct BuiltInChannelBuilder {}

impl BuiltInChannelBuilder {
    fn new() -> Self {
        return BuiltInChannelBuilder {};
    }
}

#[async_trait]
impl ChannelBuilder for BuiltInChannelBuilder {
    async fn build(&self, peer: &Peer) -> Option<Channel> {
        return match peer.get_endpoint().connect().await {
            Ok(channel) => Some(channel),
            Err(_) => None
        };
    }
}

#[derive(Debug)]
pub struct PipelineSubmissionError<T>(pub T);

impl<T> fmt::Display for PipelineSubmissionError<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "could not submit the request to the pipeline, channel closed")
    }
}

impl<T: Debug> Error for PipelineSubmissionError<T> {}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;

    use async_trait::async_trait;
    use tokio::runtime::Builder;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;
    use tonic::{Request, Response};
    use tonic::transport::Channel;

    use crate::net::connect::error::ServiceResponseError;
    use crate::net::connect::host_and_port::HostAndPort;
    use crate::net::connect::service_client::{ServiceClientProvider, ServiceRequest};
    use crate::net::peers::Peer;
    use crate::net::pipeline::{Pipeline, PipelinedRequest, PipelinedResponse, ResponseHandlerGenerator, ToPipelinedRequest, ToPipelinedResponse};
    use crate::singular_update_queue::singular_update_queue::{SingularUpdateQueue, ToAsyncBlock};

    pub(crate) struct TestRequest {
        pub(crate) id: u8,
    }

    pub(crate) struct GetValueRequest {
        pub(crate) key: u8,
    }

    #[derive(Debug)]
    pub(crate) struct TestResponse {
        pub(crate) id: u8,
    }

    #[derive(Debug)]
    pub(crate) struct GetValueResponse {
        pub(crate) value: u8,
    }

    pub(crate) struct SuccessTestClient {}

    pub(crate) struct SuccessGetValueClient {}

    #[async_trait]
    impl ServiceClientProvider<PipelinedRequest, PipelinedResponse> for SuccessTestClient {
        async fn call(&self, request: Request<PipelinedRequest>, _address: HostAndPort, _channel: Option<Channel>) -> Result<Response<PipelinedResponse>, ServiceResponseError> {
            let request = request.into_inner();
            let test_request = request.downcast::<TestRequest>().unwrap();

            return Ok(Response::new(TestResponse { id: test_request.id }.pipeline_response()));
        }
    }

    #[async_trait]
    impl ServiceClientProvider<PipelinedRequest, PipelinedResponse> for SuccessGetValueClient {
        async fn call(&self, request: Request<PipelinedRequest>, _address: HostAndPort, _channel: Option<Channel>) -> Result<Response<PipelinedResponse>, ServiceResponseError> {
            let request = request.into_inner();
            let get_value = request.downcast::<GetValueRequest>().unwrap();

            return Ok(Response::new(GetValueResponse { value: get_value.key }.pipeline_response()));
        }
    }

    #[test]
    fn send_a_request_to_be_pipelined() {
        let self_address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8918);
        let peer = Peer::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7118));

        let pipeline = Builder::new_multi_thread().enable_all().build().unwrap().block_on(async {
            Pipeline::new(peer, self_address, Arc::new(SingularUpdateQueue::new()))
        });

        Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let mut receiver = submit_test_request(&pipeline).await;
            let response = receiver.recv().await.unwrap();
            assert_eq!(10, response.id);
        });
    }

    #[test]
    fn send_multiple_requests_to_be_pipelined() {
        let self_address = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8918);
        let peer = Peer::new(HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7118));

        let pipeline = Builder::new_multi_thread().enable_all().build().unwrap().block_on(async {
            Pipeline::new(peer, self_address, Arc::new(SingularUpdateQueue::new()))
        });

        Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let mut receiver = submit_test_request(&pipeline).await;
            let response = receiver.recv().await.unwrap();
            assert_eq!(10, response.id);

            let mut receiver = submit_get_value_request(&pipeline).await;
            let response = receiver.recv().await.unwrap();
            assert_eq!(80, response.value);
        });
    }

    async fn submit_test_request(pipeline: &Pipeline) -> Receiver<Box<TestResponse>> {
        let service_request = ServiceRequest::new(
            TestRequest { id: 10 }.pipeline_request(),
            Box::new(SuccessTestClient {}),
            100,
        );

        let (sender, receiver) = mpsc::channel(1);
        let response_handler_generator: ResponseHandlerGenerator = Box::new(move |_host, response: Result<PipelinedResponse, ServiceResponseError>| {
            let inner_sender = sender.clone();
            return Some(
                async move {
                    let test_response = response.unwrap().downcast::<TestResponse>().unwrap();
                    let _ = inner_sender.send(test_response).await;
                }.async_block()
            );
        });

        let response_handler_generator = Arc::new(response_handler_generator);
        let _ = pipeline.submit(service_request, response_handler_generator).await;

        return receiver;
    }

    async fn submit_get_value_request(pipeline: &Pipeline) -> Receiver<Box<GetValueResponse>> {
        let service_request = ServiceRequest::new(
            GetValueRequest { key: 80 }.pipeline_request(),
            Box::new(SuccessGetValueClient {}),
            14,
        );

        let (sender, receiver) = mpsc::channel(1);
        let response_handler_generator: ResponseHandlerGenerator = Box::new(move |_host, response: Result<PipelinedResponse, ServiceResponseError>| {
            let inner_sender = sender.clone();
            return Some(
                async move {
                    let get_value_response = response.unwrap().downcast::<GetValueResponse>().unwrap();
                    let _ = inner_sender.send(get_value_response).await;
                }.async_block()
            );
        });

        let response_handler_generator = Arc::new(response_handler_generator);
        let _ = pipeline.submit(service_request, response_handler_generator).await;

        return receiver;
    }
}