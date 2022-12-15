use std::sync::Arc;

use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use tonic::{Request, Response, Status};

use raft::net::connect::async_network::AsyncNetwork;
use raft::net::connect::headers::{get_referral_host_from, get_referral_port_from};
use raft::net::connect::host_and_port::HostAndPort;
use raft::net::connect::service_client::ServiceRequest;
use raft::net::replica::Replica;

use crate::quorum::client_provider::GetValueByKeyResponseClient;
use crate::quorum::rpc::grpc::{GetValueByKeyResponse, CorrelatingGetValueByKeyRequest};

pub(crate) struct Server {
    replica: Arc<Replica>,
    storage: Arc<DashMap<String, String>>,
}

impl Server {
    pub(crate) async fn acknowledge_get(&self, request: Request<CorrelatingGetValueByKeyRequest>) -> Result<Response<()>, Status> {
        let optional_host = get_referral_host_from(&request);
        let optional_port = get_referral_port_from(&request);
        if optional_host.is_none() || optional_port.is_none() {
            return Err(Status::failed_precondition(format!("Missing originating host/port in acknowledge")));
        }

        let request = request.into_inner();
        println!("Received a correlating get request for key {}", request.key.clone());

        let key = request.key;
        let correlation_id = request.correlation_id;

        let originating_host_port = HostAndPort::try_new(
            optional_host.unwrap(),
            u16::try_from(optional_port.unwrap()).unwrap()
        );

        let storage = self.storage.clone();
        let handler = async move {
            let value: Option<Ref<String, String>> = storage.get(&key);
            let response = match value {
                None =>
                    GetValueByKeyResponse { key, value: "".to_string(), correlation_id },
                Some(value_ref) =>
                    GetValueByKeyResponse { key, value: String::from(value_ref.value()), correlation_id },
            };
            let service_request = ServiceRequest::new(
                response,
                Box::new(GetValueByKeyResponseClient {}),
                correlation_id,
            );
            AsyncNetwork::send_without_source_footprint(
                service_request,
                originating_host_port.unwrap()
            ).await.unwrap();
        };
        let _ = &self.replica.add_to_queue(handler);
        return Ok(Response::new(()));
    }

    pub(crate) async fn finish_get(&self, request: Request<GetValueByKeyResponse>) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        println!("Received a response for key {}", request.key.clone());

        let _ = &self.replica.register_response(request.correlation_id, Ok(Box::new(request)));
        return Ok(Response::new(()));
    }
}

impl Server {
    pub(crate) fn new(replica: Arc<Replica>) -> Server {
        return Server {
            replica,
            storage: Arc::new(DashMap::new()),
        };
    }
}