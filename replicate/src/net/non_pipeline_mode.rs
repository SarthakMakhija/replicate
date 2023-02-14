use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;

use tokio::task::JoinHandle;

use crate::net::connect::async_network::AsyncNetwork;
use crate::net::connect::correlation_id::CorrelationId;
use crate::net::connect::error::ServiceResponseError;
use crate::net::connect::host_and_port::HostAndPort;
use crate::net::connect::service_client::ServiceRequest;
use crate::net::peers::{Peer, Peers};
use crate::net::replica::TotalFailedSends;
use crate::net::request_waiting_list::request_waiting_list::RequestWaitingList;
use crate::net::request_waiting_list::response_callback::ResponseCallbackType;
use crate::singular_update_queue::singular_update_queue::SingularUpdateQueue;

pub struct NonPipelineMode<'a> {
    self_address: HostAndPort,
    singular_update_queue: Arc<SingularUpdateQueue>,
    network: Arc<AsyncNetwork>,
    request_waiting_list: &'a RequestWaitingList,
    peers: &'a Peers,
}

impl<'a> NonPipelineMode<'a> {
    pub(crate) fn new(self_address: HostAndPort,
                      singular_update_queue: Arc<SingularUpdateQueue>,
                      network: Arc<AsyncNetwork>,
                      request_waiting_list: &'a RequestWaitingList,
                      peers: &'a Peers) -> Self {
        return NonPipelineMode {
            self_address,
            singular_update_queue,
            network,
            request_waiting_list,
            peers,
        };
    }

    pub async fn send_to_replicas<Payload, S, Response>(&self,
                                                        service_request_constructor: S,
                                                        response_callback: ResponseCallbackType) -> TotalFailedSends
        where Payload: Send + 'static,
              Response: Send + Debug + 'static,
              S: Fn() -> ServiceRequest<Payload, Response> {
        return self.send_to(&self.peers, service_request_constructor, response_callback).await;
    }

    pub async fn send_to<Payload, S, Response>(&self,
                                               peers: &Peers,
                                               service_request_constructor: S,
                                               response_callback: ResponseCallbackType) -> TotalFailedSends
        where Payload: Send + 'static,
              Response: Send + Debug + 'static,
              S: Fn() -> ServiceRequest<Payload, Response> {
        let mut send_task_handles = Vec::new();

        for peer in peers.all_peers_excluding(Peer::new(self.self_address)) {
            let service_request = service_request_constructor();
            send_task_handles.push(self.send(
                &self.request_waiting_list,
                service_request,
                peer.get_address().clone(),
                response_callback.clone(),
            ));
        }

        let mut total_failed_sends: TotalFailedSends = 0;
        for task_handle in send_task_handles {
            let (result, correlation_id, target_address) = task_handle.await.unwrap();
            if result.is_err() {
                let _ = &self.request_waiting_list.handle_response(correlation_id, target_address, Err(result.unwrap_err()));
                total_failed_sends = total_failed_sends + 1;
            }
        }
        return total_failed_sends;
    }

    pub fn send_to_replicas_with_handler_hook<Payload, S, Response, F, T, U>(&self,
                                                                             service_request_constructor: S,
                                                                             response_handler_generator: Arc<F>,
                                                                             response_callback_generator: U)
        where Payload: Send + 'static,
              Response: Send + Debug + 'static,
              S: Fn() -> ServiceRequest<Payload, Response>,
              F: Fn(HostAndPort, Result<Response, ServiceResponseError>) -> Option<T> + Send + Sync + 'static,
              T: Future<Output=()> + Send + 'static,
              U: Fn() -> Option<ResponseCallbackType> {

        for peer in self.peers.all_peers_excluding(Peer::new(self.self_address)) {
            let peer_address = peer.get_address().clone();
            let source_address = self.self_address.clone();
            let singular_update_queue = self.singular_update_queue.clone();
            let service_request = service_request_constructor();
            let peer_handler_generator = response_handler_generator.clone();

            if let Some(response_callback) = response_callback_generator() {
                let correlation_id = service_request.correlation_id;
                self.request_waiting_list.add(correlation_id, peer_address.clone(), response_callback);
            }

            let network = self.network.clone();
            tokio::spawn(async move {
                let response = network.send_with_source_footprint(
                    service_request,
                    source_address,
                    peer_address,
                ).await;

                if let Some(handler) = peer_handler_generator(peer_address, response) {
                    let _ = singular_update_queue.add(handler).await;
                }
            });
        }
    }

    fn send<Payload, Response>(&self,
                               request_waiting_list: &RequestWaitingList,
                               service_request: ServiceRequest<Payload, Response>,
                               target_address: HostAndPort,
                               response_callback: ResponseCallbackType) -> JoinHandle<(Result<Response, ServiceResponseError>, CorrelationId, HostAndPort)>
        where Payload: Send + 'static,
              Response: Send + Debug + 'static {
        let correlation_id = service_request.correlation_id;
        request_waiting_list.add(correlation_id, target_address.clone(), response_callback);

        let source_address = self.self_address.clone();
        let network = self.network.clone();
        return tokio::spawn(async move {
            let result = network.send_with_source_footprint(
                service_request,
                source_address,
                target_address.clone()
            ).await;
            return (result, correlation_id, target_address);
        });
    }
}
