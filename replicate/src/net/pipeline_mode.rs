use std::collections::HashMap;
use std::sync::Arc;

use crate::net::connect::host_and_port::HostAndPort;
use crate::net::connect::service_client::ServiceRequest;
use crate::net::peers::{Peer, Peers};
use crate::net::pipeline::{Pipeline, PipelinedRequest, PipelinedResponse, ResponseHandlerGenerator};
use crate::net::request_waiting_list::request_waiting_list::RequestWaitingList;
use crate::net::request_waiting_list::response_callback::ResponseCallbackType;

pub struct PipelineMode<'a> {
    pub(crate) self_address: HostAndPort,
    pub(crate) request_waiting_list: &'a RequestWaitingList,
    pub(crate) pipeline_by_peer: &'a HashMap<Peer, Arc<Pipeline>>,
    pub(crate) peers: &'a Peers,
}

impl<'a> PipelineMode<'a> {
    pub(crate) fn new(self_address: HostAndPort,
                      request_waiting_list: &'a RequestWaitingList,
                      pipeline_by_peer: &'a HashMap<Peer, Arc<Pipeline>>,
                      peers: &'a Peers) -> Self {

        return PipelineMode {
            self_address,
            request_waiting_list,
            pipeline_by_peer,
            peers,
        };
    }

    pub fn send_to_replicas_with_handler_hook<S, U>(&self,
                                                    service_request_constructor: S,
                                                    response_handler_generator: Arc<ResponseHandlerGenerator>,
                                                    response_callback_generator: U)
        where S: Fn() -> ServiceRequest<PipelinedRequest, PipelinedResponse>,
              U: Fn() -> Option<ResponseCallbackType> {
        self.send_to_with_handler_hook(
            &self.peers,
            service_request_constructor,
            response_handler_generator,
            response_callback_generator,
        );
    }

    pub fn send_to_with_handler_hook<S, U>(&self,
                                           peers: &Peers,
                                           service_request_constructor: S,
                                           response_handler_generator: Arc<ResponseHandlerGenerator>,
                                           response_callback_generator: U)
        where S: Fn() -> ServiceRequest<PipelinedRequest, PipelinedResponse>,
              U: Fn() -> Option<ResponseCallbackType> {
        for peer in peers.all_peers_excluding(Peer::new(self.self_address)) {
            let peer_address = peer.get_address().clone();
            let service_request = service_request_constructor();
            let peer_handler_generator = response_handler_generator.clone();

            if let Some(response_callback) = response_callback_generator() {
                let correlation_id = service_request.correlation_id;
                self.request_waiting_list.add(correlation_id, peer_address.clone(), response_callback);
            }

            let pipeline = self.pipeline_by_peer.get(&peer).unwrap().clone();
            tokio::spawn(async move {
                let _ = pipeline.submit(service_request, peer_handler_generator).await;
            });
        }
    }
}

