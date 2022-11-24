use tonic::{Request, Response, Status};

use crate::net::connect::service::heartbeat::service::grpc::heartbeat_server::Heartbeat;
use crate::net::connect::service::heartbeat::service::grpc::HeartbeatRequest;

pub mod grpc {
    tonic::include_proto!("replication.net.connect");
}

#[derive(Default)]
pub(crate) struct HeartbeatService {}

#[tonic::async_trait]
impl Heartbeat for HeartbeatService {
    async fn acknowledge(&self, _: Request<HeartbeatRequest>) -> Result<Response<()>, Status> {
        return Ok(Response::new(()));
    }
}
