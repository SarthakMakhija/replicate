use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::clock::clock::Clock;

pub(crate) type ResponseErrorType = Box<dyn Error + 'static>;

pub(crate) type ResponseCallbackType<Response> = Arc<dyn ResponseCallback<Response> + 'static>;

pub trait ResponseCallback<Response> {
    fn on_response(&self, response: Result<Response, ResponseErrorType>);
}

pub struct TimestampedCallback<Response> {
    callback: ResponseCallbackType<Response>,
    creation_time: Instant,
}

impl<Response> TimestampedCallback<Response> {
    pub(crate) fn new(callback: ResponseCallbackType<Response>, creation_time: Instant) -> TimestampedCallback<Response> {
        return TimestampedCallback {
            callback,
            creation_time,
        };
    }

    pub(crate) fn on_response(&self, response: Result<Response, ResponseErrorType>) {
        self.callback.on_response(response);
    }

    pub(crate) fn has_expired(&self, clock: Box<&dyn Clock>, expiry_after: &Duration) -> bool {
        return clock.now().duration_since(self.creation_time).ge(expiry_after);
    }
}

#[cfg(test)]
mod tests {

    mod setup {
        use std::ops::{Add, Sub};
        use std::time::{Duration, Instant};

        use crate::clock::clock::Clock;
        use crate::net::response_callback::{ResponseCallback, ResponseErrorType};

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

        impl ResponseCallback<String> for NothingCallback {
            fn on_response(&self, _: Result<String, ResponseErrorType>) {}
        }
    }

    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::net::response_callback::tests::setup::{FutureClock, NothingCallback, PastClock};
    use crate::net::response_callback::TimestampedCallback;

    #[test]
    fn has_expired() {
        let timestamped_callback = TimestampedCallback::new(Arc::new(NothingCallback {}), Instant::now());
        let clock = FutureClock { duration_to_add: Duration::from_secs(5) };

        let has_expired = timestamped_callback.has_expired(Box::new(&clock), &Duration::from_secs(2));
        assert!(has_expired);
    }

    #[test]
    fn has_not_expired() {
        let timestamped_callback = TimestampedCallback::new(Arc::new(NothingCallback {}), Instant::now());
        let clock = PastClock { duration_to_subtract: Duration::from_secs(5) };

        let has_expired = timestamped_callback.has_expired(Box::new(&clock), &Duration::from_secs(2));
        assert_eq!(false, has_expired);
    }
}