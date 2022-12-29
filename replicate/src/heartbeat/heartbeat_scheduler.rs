use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::time;

pub struct HeartbeatScheduler {
    interval: Duration,
    keep_running: Arc<AtomicBool>,
}

impl HeartbeatScheduler {
    pub fn new(heartbeat_interval: Duration) -> Self {
        return HeartbeatScheduler {
            interval: heartbeat_interval,
            keep_running: Arc::new(AtomicBool::new(true)),
        };
    }

    pub fn start_with<F, T>(&self, future_generator: F)
        where
            F: Fn() -> T + Send + 'static,
            T: Future + Send + 'static,
            T: Future<Output=()> + Send + 'static {

        let keep_running = self.keep_running.clone();
        let mut interval = time::interval(self.interval);

        tokio::spawn(async move {
            loop {
                if !keep_running.load(Ordering::SeqCst) {
                    return;
                }
                let future = future_generator();
                let _ = future.await;

                interval.tick().await;
            }
        });
    }

    pub fn stop(&mut self) {
        self.keep_running.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU16, Ordering};
    use std::thread;
    use std::time::Duration;

    use crate::heartbeat::heartbeat_scheduler::HeartbeatScheduler;
    use crate::heartbeat::heartbeat_scheduler::tests::setup::HeartbeatCounter;
    use crate::heartbeat::heartbeat_sender::HeartbeatSender;

    mod setup {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU16, Ordering};

        use async_trait::async_trait;

        use crate::heartbeat::heartbeat_sender::HeartbeatSender;
        use crate::net::connect::error::ServiceResponseError;

        pub struct HeartbeatCounter {
            pub counter: Arc<AtomicU16>,
        }

        #[async_trait]
        impl HeartbeatSender for HeartbeatCounter {
            async fn send_heartbeat(&self) -> Result<(), ServiceResponseError> {
                self.counter.clone().fetch_add(1, Ordering::SeqCst);
                return Ok(());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn start() {
        let heartbeat_counter = HeartbeatCounter { counter: Arc::new(AtomicU16::new(0)) };
        let heartbeat_sender = Arc::new(heartbeat_counter);
        let readonly_counter = heartbeat_sender.clone();

        let mut heartbeat_scheduler = HeartbeatScheduler::new(Duration::from_millis(2));
        heartbeat_scheduler.start_with(move || get_future( heartbeat_sender.clone()));

        thread::sleep(Duration::from_millis(5));
        heartbeat_scheduler.stop();

        assert!(readonly_counter.counter.load(Ordering::SeqCst) >= 2);
    }

    fn get_future(heartbeat_counter: Arc<HeartbeatCounter>) -> impl Future<Output=()> {
        return async move {
            let _ = heartbeat_counter.send_heartbeat().await;
        };
    }
}