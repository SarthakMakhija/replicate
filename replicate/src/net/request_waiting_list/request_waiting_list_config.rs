use std::time::Duration;

pub struct RequestWaitingListConfig {
    request_expiry_after: Duration,
    pause_request_expiry_checker: Duration,
}

impl RequestWaitingListConfig {
    pub fn new(request_expiry_after: Duration,
               pause_request_expiry_checker: Duration) -> Self {

        return RequestWaitingListConfig {
            request_expiry_after,
            pause_request_expiry_checker,
        };
    }

    pub fn default() -> Self {
        return Self::new(
            Duration::from_secs(3),
            Duration::from_secs(2)
        );
    }

    pub fn get_request_expiry_after(&self) -> Duration {
        return self.request_expiry_after;
    }

    pub fn get_pause_request_expiry_checker(&self) -> Duration {
        return self.pause_request_expiry_checker;
    }
}