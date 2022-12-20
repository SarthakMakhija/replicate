use std::collections::HashMap;
use std::sync::Arc;

use raft::consensus::quorum::async_quorum_callback::AsyncQuorumCallback;
use raft::net::connect::host_and_port::HostAndPort;
use raft::net::replica::Replica;

use crate::quorum::factory::service_request::ServiceRequestFactory;
use crate::quorum::rpc::grpc::GetValueByKeyResponse;
use crate::quorum::rpc::grpc::PutKeyValueResponse;

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
        let service_request_constructor = || {
            ServiceRequestFactory::versioned_put_key_value_request(
                latest_value.timestamp,
                latest_value.key.clone(),
                latest_value.value.clone(),
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
