use raft_macro::add_correlation_id;

#[test]
fn add_correlation_id_fields (){
    let response = GetValueResponse {
        request_id: 10,
        correlation_id: 10
    };
    assert_eq!(10, response.request_id);
    assert_eq!(10, response.correlation_id);
}

#[add_correlation_id]
#[derive(::prost::Message)]
struct GetValueResponse {}
