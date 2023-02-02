use tonic::Request;
use crate::net::pipeline::PipelinedRequest;

pub trait RequestTransformer {
    fn transform<T: 'static>(self) -> Request<T>;
}

impl RequestTransformer for Request<PipelinedRequest> {
    fn transform<T: 'static>(self) -> Request<T> {
        let (metadata, extensions, payload) = self.into_parts();
        let payload = payload.downcast::<T>().unwrap();
        return Request::from_parts(
            metadata,
            extensions,
            *payload
        );
    }
}

#[cfg(test)]
mod tests {
    use tonic::Request;
    use crate::net::connect::host_port_extractor::{HostAndPortExtractor, REFERRAL_HOST};
    use crate::net::connect::request_transformer::RequestTransformer;
    use crate::net::pipeline::ToPipelinedRequest;

    #[test]
    fn transform_request() {
        let mut request = Request::new(().pipeline_request());
        let headers = request.metadata_mut();
        headers.insert(REFERRAL_HOST, "192.168.0.1".parse().unwrap());

        let transformed_request = request.transform::<()>();

        assert_eq!(Some("192.168.0.1".to_string()), transformed_request.get_referral_host());
        assert_eq!((), transformed_request.into_inner());
    }
}