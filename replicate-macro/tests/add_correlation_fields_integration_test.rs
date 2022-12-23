use replicate_macro::add_correlation_id;

#[test]
fn add_correlation_id_field() {
    let response = GetValueResponse {
        correlation_id: 10,
    };
    assert_eq!(10, response.correlation_id);
}

#[add_correlation_id]
#[derive(::prost::Message)]
struct GetValueResponse {}
