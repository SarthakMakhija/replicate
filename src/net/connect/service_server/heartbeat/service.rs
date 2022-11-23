use tonic::{Request, Response, Status};

use crate::net::connect::service_server::heartbeat::service::grpc::heartbeat_server::Heartbeat;
use crate::net::connect::service_server::heartbeat::service::grpc::HeartbeatRequest;

pub mod grpc {
    tonic::include_proto!("replication.net.connect");
}

#[derive(Default)]
pub(crate) struct HeartbeatServiceServer {}

#[tonic::async_trait]
impl Heartbeat for HeartbeatServiceServer {
    async fn acknowledge(&self, _: Request<HeartbeatRequest>) -> Result<Response<()>, Status> {
        return Ok(Response::new(()));
    }
}
