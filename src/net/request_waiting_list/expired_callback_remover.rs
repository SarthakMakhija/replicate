use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use dashmap::DashMap;

use crate::clock::clock::Clock;
use crate::net::request_waiting_list::request_timeout_error::RequestTimeoutError;
use crate::net::request_waiting_list::response_callback::TimestampedCallback;

pub(crate) struct ExpiredCallbackRemover<Key, Response>
    where Key: Eq + Hash + Send + Sync + Debug + 'static, {
    pending_requests: Arc<DashMap<Key, TimestampedCallback<Response>>>,
    expiry_after: Duration,
    clock: Arc<dyn Clock>,
}

impl<Key, Response: 'static> ExpiredCallbackRemover<Key, Response>
    where Key: Eq + Hash + Send + Sync + Debug + 'static, {
    pub(crate) fn start(pending_requests: Arc<DashMap<Key, TimestampedCallback<Response>>>,
                        clock: Arc<dyn Clock>,
                        expiry_after: Duration,
                        pause_expired_callbacks_remover_every: Duration) {

        let remover = ExpiredCallbackRemover { pending_requests, expiry_after, clock };
        thread::spawn(move || {
            loop {
                remover.remove();
                thread::sleep(pause_expired_callbacks_remover_every);
            }
        });
    }

    fn remove(&self) {
        self.pending_requests.retain(|_, timestamped_callback| {
            let has_expired = timestamped_callback.has_expired(&self.clock, &self.expiry_after);
            if has_expired {
                timestamped_callback.on_response(Err(Box::new(RequestTimeoutError {})));
                return false;
            }
            return true;
        });
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use std::thread;
    use std::time::{Duration, Instant};

    use dashmap::DashMap;

    use crate::net::request_waiting_list::expired_callback_remover::ExpiredCallbackRemover;
    use crate::net::request_waiting_list::expired_callback_remover::tests::setup::{FutureClock, RequestTimeoutErrorResponseCallback};
    use crate::net::request_waiting_list::response_callback::TimestampedCallback;

    mod setup {
        use std::collections::HashMap;
        use std::ops::Add;
        use std::sync::RwLock;
        use std::time::{Duration, Instant};

        use crate::clock::clock::Clock;
        use crate::net::request_waiting_list::request_timeout_error::RequestTimeoutError;
        use crate::net::request_waiting_list::response_callback::{ResponseCallback, ResponseErrorType};

        pub struct FutureClock {
            pub duration_to_add: Duration,
        }

        pub struct RequestTimeoutErrorResponseCallback {
            pub error_response: RwLock<HashMap<String, String>>,
        }

        impl Clock for FutureClock {
            fn now(&self) -> Instant {
                return Instant::now().add(self.duration_to_add);
            }
        }

        impl ResponseCallback<String> for RequestTimeoutErrorResponseCallback {
            fn on_response(&self, response: Result<String, ResponseErrorType>) {
                let response_error_type = response.unwrap_err();
                let _ = response_error_type.downcast_ref::<RequestTimeoutError>().unwrap();
                self.error_response.write().unwrap().insert(String::from("Response"), "timeout".to_string());
            }
        }
    }

    #[test]
    fn error_response_on_expired_key() {
        let key: i32 = 1;
        let clock = Arc::new(FutureClock { duration_to_add: Duration::from_secs(5) });
        let pending_requests = Arc::new(DashMap::new());

        let error_response_callback = Arc::new(RequestTimeoutErrorResponseCallback { error_response: RwLock::new(HashMap::new()) });
        let cloned_response_callback = error_response_callback.clone();
        pending_requests.clone().insert(
            key,
            TimestampedCallback::new(error_response_callback, Instant::now()),
        );

        ExpiredCallbackRemover::start(
            pending_requests,
            clock,
            Duration::from_secs(2),
            Duration::from_millis(0)
        );
        thread::sleep(Duration::from_millis(1));

        let readable_response = cloned_response_callback.error_response.read().unwrap();
        assert_eq!("timeout", readable_response.get("Response").unwrap());
    }
}