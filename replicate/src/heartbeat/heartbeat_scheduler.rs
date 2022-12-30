use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::runtime::{Builder, Runtime};
use tokio::time;

use crate::net::connect::error::AnyError;

pub struct SingleThreadedHeartbeatScheduler {
    interval: Duration,
    keep_running: Arc<AtomicBool>,
    thread_pool: Runtime,
}

impl SingleThreadedHeartbeatScheduler {
    pub fn new(interval: Duration) -> Self {
        let thread_pool = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        return SingleThreadedHeartbeatScheduler {
            interval,
            keep_running: Arc::new(AtomicBool::new(true)),
            thread_pool,
        };
    }

    pub fn start_with<F, T>(&self, future_generator: F)
        where
            F: Fn() -> T + Send + 'static,
            T: Future + Send + 'static,
            T: Future<Output=Result<(), AnyError>> + Send + 'static {
        let keep_running = self.keep_running.clone();
        let mut interval = time::interval(self.interval);

        self.thread_pool.spawn(async move {
                loop {
                    if !keep_running.load(Ordering::SeqCst) {
                        return;
                    }
                    let future = future_generator();
                    let _ = future.await;

                    interval.tick().await;
                }
            }
        );
    }

    pub fn stop(&self) {
        self.keep_running.store(false, Ordering::SeqCst);
    }

    pub fn shutdown(self) {
        self.thread_pool.shutdown_background();
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU16, Ordering};
    use std::thread;
    use std::time::Duration;

    use crate::heartbeat::heartbeat_scheduler::SingleThreadedHeartbeatScheduler;
    use crate::heartbeat::heartbeat_scheduler::tests::setup::HeartbeatCounter;
    use crate::net::connect::error::AnyError;

    mod setup {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU16, Ordering};

        use crate::net::connect::error::AnyError;

        pub struct HeartbeatCounter {
            pub counter: Arc<AtomicU16>,
        }

        impl HeartbeatCounter {
            pub async fn send_heartbeat(&self) -> Result<(), AnyError> {
                self.counter.clone().fetch_add(1, Ordering::SeqCst);
                return Ok(());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn start() {
        let heartbeat_counter = HeartbeatCounter { counter: Arc::new(AtomicU16::new(0)) };
        let heartbeat_counter = Arc::new(heartbeat_counter);
        let readonly_counter = heartbeat_counter.clone();

        let heartbeat_scheduler = SingleThreadedHeartbeatScheduler::new(Duration::from_millis(2));
        heartbeat_scheduler.start_with(move || get_future(heartbeat_counter.clone()));

        thread::sleep(Duration::from_millis(5));
        heartbeat_scheduler.stop();

        assert!(readonly_counter.counter.load(Ordering::SeqCst) >= 2);
        heartbeat_scheduler.shutdown();
    }

    fn get_future(heartbeat_counter: Arc<HeartbeatCounter>) -> impl Future<Output=Result<(), AnyError>> {
        return async move {
            return heartbeat_counter.send_heartbeat().await;
        };
    }
}