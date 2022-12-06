use std::any::Any;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::clock::clock::Clock;
use crate::net::request_waiting_list::request_timeout_error::RequestTimeoutError;

pub(crate) type ResponseErrorType = Box<dyn Error + Send + Sync>;

pub(crate) type ResponseCallbackType = Arc<dyn ResponseCallback + 'static>;

pub(crate) type AnyResponse = Box<dyn Any>;

pub trait ResponseCallback: Send + Sync {
    fn on_response(&self, response: Result<AnyResponse, ResponseErrorType>);
}

pub(crate) struct TimestampedCallback {
    callback: ResponseCallbackType,
    creation_time: Instant,
}

impl TimestampedCallback {
    pub(crate) fn new(callback: ResponseCallbackType, creation_time: Instant) -> Self {
        return TimestampedCallback {
            callback,
            creation_time,
        };
    }

    pub(crate) fn on_response(&self, response: Result<AnyResponse, ResponseErrorType>) {
        self.callback.on_response(response);
    }

    pub(crate) fn on_timeout_response(&self) {
        self.callback.on_response(Err(Box::new(RequestTimeoutError {})));
    }

    pub(crate) fn has_expired(&self, clock: &Arc<dyn Clock>, expiry_after: &Duration) -> bool {
        return clock.now().duration_since(self.creation_time).ge(expiry_after);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::clock::clock::Clock;
    use crate::net::request_waiting_list::response_callback::tests::setup::{FutureClock, NothingCallback, PastClock};
    use crate::net::request_waiting_list::response_callback::TimestampedCallback;

    mod setup {
        use std::ops::{Add, Sub};
        use std::time::{Duration, Instant};

        use crate::clock::clock::Clock;
        use crate::net::request_waiting_list::response_callback::{AnyResponse, ResponseCallback, ResponseErrorType};

        pub struct FutureClock {
            pub duration_to_add: Duration,
        }

        pub struct PastClock {
            pub duration_to_subtract: Duration,
        }

        pub struct NothingCallback {}

        impl Clock for FutureClock {
            fn now(&self) -> Instant {
                return Instant::now().add(self.duration_to_add);
            }
        }

        impl Clock for PastClock {
            fn now(&self) -> Instant {
                return Instant::now().sub(self.duration_to_subtract);
            }
        }

        impl ResponseCallback for NothingCallback {
            fn on_response(&self, _: Result<AnyResponse, ResponseErrorType>) {}
        }
    }

    #[test]
    fn has_expired() {
        let timestamped_callback = TimestampedCallback::new(Arc::new(NothingCallback {}), Instant::now());
        let clock: Arc<dyn Clock> = Arc::new(FutureClock { duration_to_add: Duration::from_secs(5) });

        let has_expired = timestamped_callback.has_expired(&clock, &Duration::from_secs(2));
        assert!(has_expired);
    }

    #[test]
    fn has_not_expired() {
        let timestamped_callback = TimestampedCallback::new(Arc::new(NothingCallback {}), Instant::now());
        let clock: Arc<dyn Clock> = Arc::new(PastClock { duration_to_subtract: Duration::from_secs(5) });

        let has_expired = timestamped_callback.has_expired(&clock, &Duration::from_secs(2));
        assert_eq!(false, has_expired);
    }
}