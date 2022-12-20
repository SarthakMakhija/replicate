use std::collections::HashMap;
use std::sync::Arc;

use raft::consensus::quorum::async_quorum_callback::AsyncQuorumCallback;
use raft::net::connect::correlation_id::CorrelationIdGenerator;
use raft::net::connect::host_and_port::HostAndPort;
use raft::net::connect::random_correlation_id_generator::RandomCorrelationIdGenerator;
use raft::net::connect::service_client::ServiceRequest;
use raft::net::replica::Replica;

use crate::quorum::client_provider::VersionedPutKeyValueRequestClient;
use crate::quorum::rpc::grpc::GetValueByKeyResponse;
use crate::quorum::rpc::grpc::PutKeyValueResponse;
use crate::quorum::rpc::grpc::VersionedPutKeyValueRequest;

pub(crate) struct ReadRepair<'a> {
    replica: Arc<Replica>,
    response_by_host: &'a HashMap<HostAndPort, GetValueByKeyResponse>,
}

impl<'a> ReadRepair<'a> {
    pub(crate) fn new(replica: Arc<Replica>, response_by_host: &HashMap<HostAndPort, GetValueByKeyResponse>) -> ReadRepair {
        return ReadRepair {
            replica,
            response_by_host,
        };
    }

    pub(crate) async fn repair(&self) -> GetValueByKeyResponse {
        let latest_value: Option<&GetValueByKeyResponse> = self.get_latest_value();
        return match latest_value {
            None => {
                GetValueByKeyResponse {
                    key: "".to_string(),
                    value: "".to_string(),
                    correlation_id: 0,
                    timestamp: 0,
                }
            }
            Some(get_value_by_key_response) => {
                self.perform_read_repair(get_value_by_key_response).await
            }
        }
    }

    async fn perform_read_repair(&self, latest_value: &GetValueByKeyResponse) -> GetValueByKeyResponse {
        let hosts_with_stale_values = self.get_hosts_with_stale_values(latest_value.timestamp);
        if hosts_with_stale_values.is_empty() {
            return GetValueByKeyResponse {
                key: latest_value.key.clone(),
                value: latest_value.value.clone(),
                correlation_id: latest_value.correlation_id,
                timestamp: latest_value.timestamp,
            };
        }

        println!("hosts_with_stale_values those needing read_repair {:?}", hosts_with_stale_values);
        let correlation_id_generator = RandomCorrelationIdGenerator::new();
        let service_request_constructor = || {
            let correlation_id = correlation_id_generator.generate();
            ServiceRequest::new(
                VersionedPutKeyValueRequest {
                    key: latest_value.key.clone(),
                    value: latest_value.value.clone(),
                    timestamp: latest_value.timestamp,
                    correlation_id,
                },
                Box::new(VersionedPutKeyValueRequestClient {}),
                correlation_id,
            )
        };
        let expected_responses = hosts_with_stale_values.len();
        let async_quorum_callback = AsyncQuorumCallback::<PutKeyValueResponse>::new(expected_responses);

        let _ = &self.replica
            .send_one_way_to(&hosts_with_stale_values, service_request_constructor, async_quorum_callback.clone())
            .await;

        let _ = async_quorum_callback.handle().await;
        return GetValueByKeyResponse {
            key: latest_value.key.clone(),
            value: latest_value.value.clone(),
            correlation_id: latest_value.correlation_id,
            timestamp: latest_value.timestamp,
        };
    }

    fn get_latest_value(&self) -> Option<&GetValueByKeyResponse> {
        return self.response_by_host.values().max_by(|this, other| this.timestamp.cmp(&other.timestamp));
    }

    fn get_hosts_with_stale_values(&self, latest_timestamp: u64) -> Vec<HostAndPort> {
        return self
            .response_by_host
            .keys()
            .map(|host| host.clone())
            .filter(|host| {
                let timestamp = self.response_by_host.get(host).unwrap().timestamp;
                return latest_timestamp > timestamp;
            })
            .collect();
    }
}
